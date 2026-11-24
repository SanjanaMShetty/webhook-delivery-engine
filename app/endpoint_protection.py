"""Destination validation and Redis-backed per-endpoint delivery controls."""

from __future__ import annotations

import ipaddress
import math
import os
import socket
import time
from urllib.parse import urlsplit


class UnsafeDestination(ValueError):
    """Raised when a webhook URL targets a disallowed destination."""


def validate_destination_url(url: str) -> None:
    parsed = urlsplit(str(url))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise UnsafeDestination("Webhook URL must use http or https and include a host")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeDestination("Webhook URL must not contain user credentials")
    if os.getenv("ALLOW_PRIVATE_WEBHOOK_TARGETS", "false").lower() in {
        "1",
        "true",
        "yes",
    }:
        return
    host = parsed.hostname.rstrip(".").lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".localhost"):
        raise UnsafeDestination("Localhost webhook destinations are blocked")
    try:
        addresses = {ipaddress.ip_address(host)}
    except ValueError:
        resolved = socket.getaddrinfo(
            host,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
        addresses = {ipaddress.ip_address(item[4][0]) for item in resolved}
    if not addresses or any(not address.is_global for address in addresses):
        raise UnsafeDestination(
            "Webhook destination resolves to a non-public IP address"
        )


_LIMIT_SCRIPT = """
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local concurrency = tonumber(ARGV[3])
local lease_id = ARGV[4]
local lease_seconds = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local active = redis.call('ZCARD', KEYS[2])
if active >= concurrency then
  local first = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  local delay = 1
  if first[2] then delay = math.max(1, math.ceil(tonumber(first[2]) - now)) end
  return {0, 'concurrency', delay}
end
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], 2) end
if count > rate then return {0, 'rate', 1} end
redis.call('ZADD', KEYS[2], now + lease_seconds, lease_id)
redis.call('EXPIRE', KEYS[2], lease_seconds + 2)
return {1, 'ok', 0}
"""

_BREAKER_ACQUIRE_SCRIPT = """
local state = redis.call('GET', KEYS[1])
if not state then return {1, 0} end
if state == 'open' then
  local until_time = tonumber(redis.call('GET', KEYS[2]) or '0')
  if tonumber(ARGV[1]) < until_time then
    return {0, math.max(1, math.ceil(until_time - tonumber(ARGV[1])))}
  end
  redis.call('SET', KEYS[1], 'half_open')
end
local probe = redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[3], 'NX')
if probe then return {1, 0} end
return {0, math.max(1, redis.call('TTL', KEYS[3]))}
"""

_BREAKER_RECORD_SCRIPT = """
local state = redis.call('GET', KEYS[1])
if ARGV[1] == 'success' then
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
  return 0
end
local failures = redis.call('INCR', KEYS[4])
if failures == 1 then redis.call('EXPIRE', KEYS[4], tonumber(ARGV[5]) * 10) end
if state == 'half_open' or failures >= tonumber(ARGV[2]) then
  redis.call('SET', KEYS[1], 'open')
  redis.call('SET', KEYS[2], tonumber(ARGV[3]) + tonumber(ARGV[4]))
  redis.call('DEL', KEYS[3], KEYS[4])
  return 1
end
return failures
"""


async def acquire_endpoint_capacity(
    redis,
    endpoint_id: str,
    attempt_id: str,
    rate: int,
    concurrency: int,
    lease_seconds: int,
) -> tuple[bool, str, int]:
    now = time.time()
    rate_key = f"webhook:limit:{endpoint_id}:{math.floor(now)}"
    concurrency_key = f"webhook:concurrency:{endpoint_id}"
    result = await redis.eval(
        _LIMIT_SCRIPT,
        2,
        rate_key,
        concurrency_key,
        now,
        rate,
        concurrency,
        attempt_id,
        lease_seconds,
    )
    return (
        bool(int(result[0])),
        result[1].decode() if isinstance(result[1], bytes) else str(result[1]),
        int(result[2]),
    )


async def release_endpoint_capacity(redis, endpoint_id: str, attempt_id: str) -> None:
    await redis.zrem(f"webhook:concurrency:{endpoint_id}", attempt_id)


async def acquire_circuit_probe(
    redis, endpoint_id: str, probe_id: str, probe_lease: int
) -> tuple[bool, int]:
    prefix = f"webhook:circuit:{endpoint_id}"
    result = await redis.eval(
        _BREAKER_ACQUIRE_SCRIPT,
        3,
        f"{prefix}:state",
        f"{prefix}:until",
        f"{prefix}:probe",
        time.time(),
        probe_id,
        probe_lease,
    )
    return bool(int(result[0])), int(result[1])


async def record_circuit_result(
    redis, endpoint_id: str, *, succeeded: bool, threshold: int, cooldown: int
) -> None:
    prefix = f"webhook:circuit:{endpoint_id}"
    await redis.eval(
        _BREAKER_RECORD_SCRIPT,
        4,
        f"{prefix}:state",
        f"{prefix}:until",
        f"{prefix}:probe",
        f"{prefix}:failures",
        "success" if succeeded else "failure",
        threshold,
        time.time(),
        cooldown,
        cooldown,
    )
