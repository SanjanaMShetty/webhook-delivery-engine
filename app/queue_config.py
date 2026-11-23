import os
from arq.connections import RedisSettings

REDIS_SETTINGS = RedisSettings.from_dsn(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
MAX_DELIVERY_ATTEMPTS = int(os.getenv("MAX_DELIVERY_ATTEMPTS", "5"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
RETRY_BASE_SECONDS = int(os.getenv("RETRY_BASE_SECONDS", "5"))
RETRY_MAX_SECONDS = int(os.getenv("RETRY_MAX_SECONDS", "3600"))
