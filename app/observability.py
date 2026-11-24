from __future__ import annotations

import contextvars
import json
import logging
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar
from uuid import uuid4

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from prometheus_client import Counter, Histogram, start_http_server

REQUEST_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)
T = TypeVar("T")

HTTP_REQUESTS = Counter(
    "webhook_http_requests_total",
    "HTTP requests handled",
    ("method", "route", "status"),
)
HTTP_DURATION = Histogram(
    "webhook_http_request_duration_seconds", "HTTP request latency", ("method", "route")
)
DELIVERY_ATTEMPTS = Counter(
    "webhook_delivery_attempts_total",
    "Webhook delivery attempts",
    ("outcome", "status_class"),
)
DELIVERY_DURATION = Histogram(
    "webhook_delivery_attempt_duration_seconds", "Webhook HTTP delivery latency"
)
OUTBOX_PUBLISHED = Counter("webhook_outbox_published_total", "Outbox records published")
OUTBOX_FAILURES = Counter(
    "webhook_outbox_publish_failures_total", "Outbox publish failures"
)
OUTBOX_RECOVERED = Counter(
    "webhook_outbox_recovered_total", "Expired outbox leases recovered"
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        span = trace.get_current_span()
        span_context = span.get_span_context()
        document: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": REQUEST_ID.get(),
        }
        if span_context.is_valid:
            document["trace_id"] = f"{span_context.trace_id:032x}"
            document["span_id"] = f"{span_context.span_id:016x}"
        for key in ("delivery_id", "event_id", "endpoint_id", "outcome", "status_code"):
            value = getattr(record, key, None)
            if value is not None:
                document[key] = value
        if record.exc_info:
            document["exception"] = self.formatException(record.exc_info)
        return json.dumps(document, separators=(",", ":"), default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, handlers=[handler], force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = [handler]
        logger.setLevel(level)
        logger.propagate = False


def configure_tracing(service_name: str) -> None:
    """Enable OTLP traces only when a collector endpoint is configured."""
    if getattr(configure_tracing, "configured", False):
        return
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        SimpleSpanProcessor,
    )

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    elif os.getenv("OTEL_CONSOLE_TRACES", "false").lower() == "true":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    configure_tracing.configured = True


def traced_job(
    name: str,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    def decorate(function: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(function)
        async def wrapped(*args: Any, **kwargs: Any) -> T:
            tracer = trace.get_tracer("webhook-delivery-engine")
            with tracer.start_as_current_span(name) as span:
                span.set_attribute("messaging.operation.type", "process")
                if len(args) > 1:
                    span.set_attribute("webhook.delivery.id", str(args[1]))
                try:
                    return await function(*args, **kwargs)
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR))
                    raise

        return wrapped

    return decorate


def start_metrics_server(port: int, host: str = "127.0.0.1") -> None:
    start_http_server(port, addr=host)


def start_worker_metrics() -> None:
    start_metrics_server(
        int(os.getenv("WORKER_METRICS_PORT", "9101")),
        os.getenv("WORKER_METRICS_HOST", "127.0.0.1"),
    )


def new_request_id(value: str | None) -> str:
    if value and len(value) <= 128 and all(32 <= ord(char) < 127 for char in value):
        return value
    return str(uuid4())


def observe_http_request(method: str, route: str, status: int, elapsed: float) -> None:
    HTTP_REQUESTS.labels(method=method, route=route, status=str(status)).inc()
    HTTP_DURATION.labels(method=method, route=route).observe(elapsed)
