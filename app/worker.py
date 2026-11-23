from __future__ import annotations
import asyncio
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID
import httpx
from sqlalchemy import select
from app.database import SessionLocal
from app.models import Delivery, DeliveryAttempt, Event, OutboxMessage, WebhookEndpoint
from app.queue_config import (HTTP_TIMEOUT_SECONDS, MAX_DELIVERY_ATTEMPTS,
                              REDIS_SETTINGS, RETRY_BASE_SECONDS, RETRY_MAX_SECONDS)

async def startup(ctx):
    ctx["http"] = httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=False)

async def shutdown(ctx):
    await ctx["http"].aclose()

def claim_delivery(delivery_id: UUID):
    now = datetime.now(timezone.utc)
    with SessionLocal.begin() as db:
        d = db.scalar(select(Delivery).where(Delivery.id == delivery_id).with_for_update())
        if not d or d.status in {"processing", "delivered", "dead", "permanent_failure", "paused"}:
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
        d.status = "processing"
        d.attempt_count += 1
        # attempt_count resets on manual replay; use historical max for globally unique attempt numbers.
        previous = db.scalar(select(DeliveryAttempt.attempt_number).where(
            DeliveryAttempt.delivery_id == d.id
        ).order_by(DeliveryAttempt.attempt_number.desc()).limit(1)) or 0
        number = previous + 1
        attempt = DeliveryAttempt(delivery_id=d.id, attempt_number=number, outcome="in_progress")
        db.add(attempt)
        db.flush()
        return {"delivery_id": d.id, "attempt_id": attempt.id, "attempt_number": number,
                "url": ep.url, "endpoint_id": ep.id, "event_id": ev.id,
                "producer_event_id": ev.producer_event_id, "event_type": ev.event_type,
                "body": ev.body_bytes}

def finish(data, status_code, response_text, error, latency_ms):
    now = datetime.now(timezone.utc)
    retryable = error is not None or status_code in (408, 425, 429) or (status_code is not None and status_code >= 500)
    with SessionLocal.begin() as db:
        d = db.scalar(select(Delivery).where(Delivery.id == data["delivery_id"]).with_for_update())
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
        d.last_error = (error or (f"HTTP {status_code}" if status_code and status_code >= 300 else ""))[:1000] or None
        if status_code is not None and 200 <= status_code < 300 and error is None:
            a.outcome = "succeeded"
            d.status = "delivered"
            d.delivered_at = now
            if ep:
                ep.consecutive_failures = 0
            return
        if ep:
            ep.consecutive_failures += 1
        if retryable and d.attempt_count < MAX_DELIVERY_ATTEMPTS:
            a.outcome = "retryable_failure"
            d.status = "retrying"
            delay = min(RETRY_BASE_SECONDS * (2 ** max(0, d.attempt_count - 1)), RETRY_MAX_SECONDS)
            due = now + timedelta(seconds=delay)
            d.next_attempt_at = due
            outbox = db.scalar(select(OutboxMessage).where(OutboxMessage.delivery_id == d.id).with_for_update())
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

async def deliver_webhook(ctx, delivery_id: str):
    data = await asyncio.to_thread(claim_delivery, UUID(delivery_id))
    if not data:
        return
    headers = {"Content-Type": "application/json", "User-Agent": "WebhookDeliveryEngine/0.1",
               "X-Webhook-Delivery-Id": str(data["delivery_id"]),
               "X-Webhook-Event-Id": data["producer_event_id"],
               "X-Webhook-Event-Type": data["event_type"],
               "X-Webhook-Attempt": str(data["attempt_number"])}
    started = time.monotonic()
    try:
        response = await ctx["http"].post(data["url"], content=data["body"], headers=headers)
        await asyncio.to_thread(finish, data, response.status_code, response.text, None,
                                int((time.monotonic() - started) * 1000))
    except httpx.HTTPError as exc:
        await asyncio.to_thread(finish, data, None, None, f"{type(exc).__name__}: {exc}",
                                int((time.monotonic() - started) * 1000))

class WorkerSettings:
    functions = [deliver_webhook]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS
    max_jobs = 20
    job_timeout = HTTP_TIMEOUT_SECONDS + 10
