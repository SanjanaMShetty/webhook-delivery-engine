import asyncio
import hashlib
import json
import logging
import os
import time

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select, text

from app.api.routes import router as api_router
from app.database import SessionLocal
from app.models import ApiKey, Tenant
from app.observability import (
    REQUEST_ID,
    configure_logging,
    configure_tracing,
    new_request_id,
    observe_http_request,
)
from app.realtime import delivery_channel

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


@app.get("/health/live", include_in_schema=False)
async def health_live():
    """Report that the API process is alive."""
    return {"status": "alive"}


@app.get("/health/ready", include_in_schema=False)
async def health_ready():
    """Report readiness only when PostgreSQL can serve requests."""
    try:
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ready"}


@app.websocket("/v1/ws/deliveries")
async def delivery_updates(websocket: WebSocket):
    """Stream this tenant's delivery updates after first-message authentication."""
    await websocket.accept()
    redis_client = None
    pubsub = None
    tasks = []
    try:
        try:
            auth_message = json.loads(
                await asyncio.wait_for(websocket.receive_text(), timeout=10)
            )
        except (TimeoutError, json.JSONDecodeError):
            await websocket.close(code=4400, reason="Send an authentication message first")
            return
        if not isinstance(auth_message, dict) or auth_message.get("type") != "authenticate":
            await websocket.close(code=4400, reason="Invalid authentication message")
            return
        api_key = auth_message.get("api_key")
        if not isinstance(api_key, str) or not api_key:
            await websocket.close(code=4400, reason="Invalid authentication message")
            return

        key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
        with SessionLocal() as session:
            tenant = session.scalar(
                select(Tenant)
                .join(ApiKey, ApiKey.tenant_id == Tenant.id)
                .where(
                    ApiKey.key_hash == key_hash,
                    ApiKey.revoked_at.is_(None),
                    Tenant.status == "active",
                )
            )
            tenant_id = str(tenant.id) if tenant is not None else None
        if tenant_id is None:
            await websocket.close(code=4401, reason="Invalid or revoked API key")
            return

        redis_client = Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=2,
        )
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(delivery_channel(tenant_id))
        await websocket.send_json({"type": "authenticated", "tenant_id": tenant_id})

        async def forward_updates():
            async for message in pubsub.listen():
                if message.get("type") == "message":
                    await websocket.send_text(message["data"])

        async def receive_control_messages():
            while True:
                message = await websocket.receive_text()
                if message == "ping":
                    await websocket.send_json({"type": "pong"})

        tasks = [
            asyncio.create_task(forward_updates()),
            asyncio.create_task(receive_control_messages()),
        ]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    except WebSocketDisconnect:
        pass
    except (RedisError, OSError):
        logger.exception("WebSocket Redis connection failed")
        try:
            await websocket.close(code=1013, reason="Live updates temporarily unavailable")
        except RuntimeError:
            pass
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if pubsub is not None:
            await pubsub.aclose()
        if redis_client is not None:
            await redis_client.aclose()
