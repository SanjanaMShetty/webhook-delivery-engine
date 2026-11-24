from __future__ import annotations

import asyncio
import os
import socket
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from arq import create_pool
from sqlalchemy import or_, select

from app.database import SessionLocal
from app.models import Delivery, DeliveryAttempt, OutboxMessage
from app.observability import (
    OUTBOX_FAILURES,
    OUTBOX_PUBLISHED,
    OUTBOX_RECOVERED,
    configure_logging,
    start_metrics_server,
)
from app.queue_config import REDIS_SETTINGS

OWNER = f"{socket.gethostname()}:{os.getpid()}"


def claim_batch(limit: int = 50):
    now = datetime.now(UTC)
    claimed = []
    with SessionLocal.begin() as db:
        rows = db.scalars(
            select(OutboxMessage)
            .where(
                or_(
                    (OutboxMessage.status == "pending")
                    & (OutboxMessage.available_at <= now),
                    (OutboxMessage.status == "publishing")
                    & (OutboxMessage.lease_expires_at < now),
                )
            )
            .order_by(OutboxMessage.available_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ).all()
        for row in rows:
            token = uuid4()
            row.status = "publishing"
            row.lease_owner = OWNER
            row.lease_token = token
            row.lease_expires_at = now + timedelta(seconds=60)
            row.publish_attempts += 1
            claimed.append(
                (str(row.id), str(row.delivery_id), str(token), row.publish_attempts)
            )
    return claimed


def recover_stale():
    now = datetime.now(UTC)
    with SessionLocal.begin() as db:
        stale = db.scalars(
            select(Delivery)
            .where(
                Delivery.status == "processing",
                Delivery.updated_at < now - timedelta(seconds=90),
            )
            .with_for_update(skip_locked=True)
            .limit(100)
        ).all()
        for delivery in stale:
            attempt = db.scalar(
                select(DeliveryAttempt)
                .where(
                    DeliveryAttempt.delivery_id == delivery.id,
                    DeliveryAttempt.outcome == "in_progress",
                )
                .order_by(DeliveryAttempt.attempt_number.desc())
                .limit(1)
                .with_for_update()
            )
            if attempt:
                attempt.outcome = "retryable_failure"
                attempt.error = "Worker lease expired; outcome may be unknown"
                attempt.completed_at = now
            delivery.last_error = "Worker lease expired; outcome may be unknown"
            if delivery.attempt_count >= int(os.getenv("MAX_DELIVERY_ATTEMPTS", "5")):
                delivery.status = "dead"
            else:
                delivery.status = "retrying"
                delivery.next_attempt_at = now
                outbox = db.scalar(
                    select(OutboxMessage)
                    .where(OutboxMessage.delivery_id == delivery.id)
                    .with_for_update()
                )
                if outbox:
                    outbox.status = "pending"
                    outbox.available_at = now
                    outbox.published_at = None
                    outbox.lease_owner = outbox.lease_token = (
                        outbox.lease_expires_at
                    ) = None

    OUTBOX_RECOVERED.inc(len(stale))


async def publish_once(redis):
    recover_stale()
    for outbox_id, delivery_id, token, publish_attempt in claim_batch():
        try:
            # Deterministic job IDs make redelivery after a publisher crash safe.
            await redis.enqueue_job(
                "deliver_webhook",
                delivery_id,
                _job_id=f"delivery:{delivery_id}:publish:{publish_attempt}",
            )
            with SessionLocal.begin() as db:
                row = db.get(OutboxMessage, outbox_id)
                if row and str(row.lease_token) == token:
                    row.status = "published"
                    row.published_at = datetime.now(UTC)
                    OUTBOX_PUBLISHED.inc()
                    row.lease_owner = row.lease_token = row.lease_expires_at = None
        except Exception as exc:  # noqa: BLE001
            OUTBOX_FAILURES.inc()
            with SessionLocal.begin() as db:
                row = db.get(OutboxMessage, outbox_id)
                if row and str(row.lease_token) == token:
                    row.status = "pending"
                    row.available_at = datetime.now(UTC) + timedelta(
                        seconds=min(2**row.publish_attempts, 60)
                    )
                    row.last_error = str(exc)[:1000]
                    row.lease_owner = row.lease_token = row.lease_expires_at = None


async def main():
    configure_logging()
    start_metrics_server(
        int(os.getenv("OUTBOX_METRICS_PORT", "9102")),
        os.getenv("OUTBOX_METRICS_HOST", "127.0.0.1"),
    )
    redis = await create_pool(REDIS_SETTINGS)
    try:
        while True:
            await publish_once(redis)
            await asyncio.sleep(1)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
