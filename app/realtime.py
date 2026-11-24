"""Tenant-scoped Redis Pub/Sub helpers for live delivery updates."""

import json
import logging
import os
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


def delivery_channel(tenant_id: str) -> str:
    return f"webhook:tenant:{tenant_id}:delivery-updates"


async def publish_delivery_update(tenant_id: str, update: dict[str, Any]) -> None:
    """Publish best-effort live status after the durable DB update commits."""
    client = Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
        socket_connect_timeout=2,
    )
    try:
        subscriber_count = await client.publish(
            delivery_channel(tenant_id), json.dumps(update)
        )
        logger.info(
            "Published live delivery update",
            extra={
                "tenant_id": tenant_id,
                "delivery_id": update.get("delivery_id"),
                "subscriber_count": subscriber_count,
            },
        )
    except RedisError:
        logger.exception(
            "Could not publish live delivery update",
            extra={"tenant_id": tenant_id, "delivery_id": update.get("delivery_id")},
        )
    finally:
        await client.aclose()
