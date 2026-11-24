import logging
import time

from fastapi import FastAPI, Request
from fastapi.responses import Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.routes import router as api_router
from app.observability import (
    REQUEST_ID,
    configure_logging,
    configure_tracing,
    new_request_id,
    observe_http_request,
)

configure_logging()
configure_tracing("webhook-api")
logger = logging.getLogger(__name__)

app = FastAPI(title="Webhook Delivery Engine", version="0.2.0")
app.include_router(api_router)
FastAPIInstrumentor.instrument_app(app)


@app.middleware("http")
async def request_observability(request: Request, call_next):
    request_id = new_request_id(request.headers.get("X-Request-ID"))
    token = REQUEST_ID.set(request_id)
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        route = getattr(request.scope.get("route"), "path", "unmatched")
        observe_http_request(
            request.method, route, status_code, time.perf_counter() - started
        )
        logger.info(
            "http request completed",
            extra={
                "outcome": "success" if status_code < 500 else "error",
                "status_code": status_code,
            },
        )
        REQUEST_ID.reset(token)


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    return {"status": "ok"}
