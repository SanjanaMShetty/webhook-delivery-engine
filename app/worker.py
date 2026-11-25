from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import ClassVar
from uuid import UUID

import httpx
from cryptography.fernet import InvalidToken
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from sqlalchemy import select

from app.database import SessionLocal
from app.models import Delivery, DeliveryAttempt, Event, OutboxMessage, WebhookEndpoint
from app.observability import (
    DELIVERY_ATTEMPTS,
    DELIVERY_DURATION,
    configure_logging,
    configure_tracing,
    start_worker_metrics,
    traced_job,
)
from app.queue_config import (
    HTTP_TIMEOUT_SECONDS,
    MAX_DELIVERY_ATTEMPTS,
    REDIS_SETTINGS,
    RETRY_BASE_SECONDS,
    RETRY_MAX_SECONDS,
)
from app.realtime import publish_delivery_update
from app.security import decrypt_secret


async def startup(ctx):
    configure_logging()
    configure_tracing("webhook-worker")

    HTTPXClientInstrumentor().instrument()
    start_worker_metrics()
    ctx["http"] = httpx.AsyncClient(
        timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=False
    )


async def shutdown(ctx):
    await ctx["http"].aclose()
    trace.get_tracer_provider().force_flush()


def claim_delivery(delivery_id: UUID):
    now = datetime.now(UTC)
    with SessionLocal.begin() as db:
        d = db.scalar(
            select(Delivery).where(Delivery.id == delivery_id).with_for_update()
        )
        if not d or d.status in {
            "processing",
            "delivered",
            "dead",
            "permanent_failure",
            "paused",
        }:
            return None
        if d.next_attempt_at > now:
            return None
        ep = db.get(WebhookEndpoint, d.endpoint_id)
        ev = db.get(Event, d.event_id)
        if not ep or not ev:
            d.status = "permanent_failure"
            d.last_error = "Endpoint or event is missing"
            return None
        if ep.status != "active":
            d.status = "paused"
            d.last_error = "Endpoint is not active"
            return None
        if not ep.signing_secret_ciphertext:
            d.status = "permanent_failure"
            d.last_error = "Webhook signing secret is not configured"
            return None
        try:
            signing_secret = decrypt_secret(ep.signing_secret_ciphertext)
        except (InvalidToken, ValueError):
            d.status = "permanent_failure"
            d.last_error = "Webhook signing secret could not be decrypted"
            return None
        d.status = "processing"
        d.attempt_count += 1
        # attempt_count resets on manual replay; use historical max for globally unique attempt numbers.
        previous = (
            db.scalar(
                select(DeliveryAttempt.attempt_number)
                .where(DeliveryAttempt.delivery_id == d.id)
                .order_by(DeliveryAttempt.attempt_number.desc())
                .limit(1)
            )
            or 0
        )
        number = previous + 1
        attempt = DeliveryAttempt(
            delivery_id=d.id, attempt_number=number, outcome="in_progress"
        )
        db.add(attempt)
        db.flush()
        return {
            "delivery_id": d.id,
            "attempt_id": attempt.id,
            "attempt_number": number,
            "url": ep.url,
            "endpoint_id": ep.id,
            "tenant_id": ep.tenant_id,
            "event_id": ev.id,
            "producer_event_id": ev.producer_event_id,
            "event_type": ev.event_type,
            "body": ev.body_bytes,
            "signing_secret": signing_secret,
        }


def _delivery_update(data, delivery):
    return {
        "tenant_id": str(data["tenant_id"]),
        "type": "delivery.updated",
        "delivery_id": str(delivery.id),
        "event_id": str(delivery.event_id),
        "endpoint_id": str(delivery.endpoint_id),
        "status": delivery.status,
        "attempt_count": delivery.attempt_count,
        "last_status_code": delivery.last_status_code,
        "updated_at": delivery.updated_at.isoformat() if delivery.updated_at else None,
    }
def finish(data, status_code, response_text, error, latency_ms):
    now = datetime.now(UTC)
    retryable = (
        error is not None
        or status_code in (408, 425, 429)
        or (status_code is not None and status_code >= 500)
    )
    with SessionLocal.begin() as db:
        d = db.scalar(
            select(Delivery).where(Delivery.id == data["delivery_id"]).with_for_update()
        )
        a = db.get(DeliveryAttempt, data["attempt_id"])
        ep = db.get(WebhookEndpoint, data["endpoint_id"])
        if not d or not a:
            return
        a.http_status_code = status_code
        a.response_excerpt = (response_text or "")[:1000] or None
        a.error = (error or "")[:1000] or None
        a.latency_ms = latency_ms
        a.completed_at = now
        d.last_status_code = status_code
        d.last_error = (
            error
            or (f"HTTP {status_code}" if status_code and status_code >= 300 else "")
        )[:1000] or None
        if status_code is not None and 200 <= status_code < 300 and error is None:
            a.outcome = "succeeded"
            d.status = "delivered"
            d.delivered_at = now
            if ep:
                ep.consecutive_failures = 0
            return _delivery_update(data, d)
        if ep:
            ep.consecutive_failures += 1
        if retryable and d.attempt_count < MAX_DELIVERY_ATTEMPTS:
            a.outcome = "retryable_failure"
            d.status = "retrying"
            delay = min(
                RETRY_BASE_SECONDS * (2 ** max(0, d.attempt_count - 1)),
                RETRY_MAX_SECONDS,
            )
            due = now + timedelta(seconds=delay)
            d.next_attempt_at = due
            outbox = db.scalar(
                select(OutboxMessage)
                .where(OutboxMessage.delivery_id == d.id)
                .with_for_update()
            )
            if outbox:
                outbox.status = "pending"
                outbox.available_at = due
                outbox.published_at = None
                outbox.lease_owner = outbox.lease_token = outbox.lease_expires_at = None
        elif retryable:
            a.outcome = "retryable_failure"
            d.status = "dead"
        else:
            a.outcome = "permanent_failure"
            d.status = "permanent_failure"

    succeeded = status_code is not None and 200 <= status_code < 300 and error is None
    terminal_retry = retryable and data["attempt_number"] >= MAX_DELIVERY_ATTEMPTS
    outcome = (
        "succeeded"
        if succeeded
        else (
            "dead"
            if terminal_retry
            else "retryable_failure"
            if retryable
            else "permanent_failure"
        )
    )
    status_class = (
        f"{status_code // 100}xx" if status_code is not None else "network_error"
    )
    DELIVERY_ATTEMPTS.labels(outcome=outcome, status_class=status_class).inc()
    DELIVERY_DURATION.observe(max(latency_ms, 0) / 1000)
    logging.getLogger("app.worker").info(
        "webhook attempt completed",
        extra={
            "delivery_id": str(data["delivery_id"]),
            "event_id": str(data["event_id"]),
            "endpoint_id": str(data["endpoint_id"]),
            "outcome": outcome,
            "status_code": status_code,
        },
    )
    return _delivery_update(data, d)


@traced_job("webhook.delivery")
async def deliver_webhook(ctx, delivery_id: str, trace_context: dict[str, str] | None = None):
    data = await asyncio.to_thread(claim_delivery, UUID(delivery_id))
    if not data:
        return
    timestamp = str(int(time.time()))
    signed_body = timestamp.encode("ascii") + b"." + data["body"]
    digest = hmac.new(
        data["signing_secret"].encode("utf-8"), signed_body, hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "WebhookDeliveryEngine/0.1",
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature": f"sha256={digest}",
        "X-Webhook-Delivery-Id": str(data["delivery_id"]),
        "X-Webhook-Event-Id": data["producer_event_id"],
        "X-Webhook-Event-Type": data["event_type"],
        "X-Webhook-Attempt": str(data["attempt_number"]),
    }
    started = time.monotonic()
    try:
        response = await ctx["http"].post(
            data["url"], content=data["body"], headers=headers
        )
        update = await asyncio.to_thread(
            finish,
            data,
            response.status_code,
            response.text,
            None,
            int((time.monotonic() - started) * 1000),
        )
        if update is not None:
            await publish_delivery_update(update["tenant_id"], update)
    except httpx.HTTPError as exc:
        update = await asyncio.to_thread(
            finish,
            data,
            None,
            None,
            f"{type(exc).__name__}: {exc}",
            int((time.monotonic() - started) * 1000),
        )
        if update is not None:
            await publish_delivery_update(update["tenant_id"], update)


class WorkerSettings:
    functions: ClassVar[list] = [deliver_webhook]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS
    max_jobs = 20
    job_timeout = HTTP_TIMEOUT_SECONDS + 10
