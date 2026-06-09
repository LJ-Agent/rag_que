"""Redis cache client - optional distributed cache for DAG results."""
import json
from typing import Any
from common.config_loader import get_config
from loguru import logger

_client = None

def _get_cfg() -> dict[str, Any]:
    return get_config().get("redis", {})

def get_client():
    global _client
    if _client is not None:
        return _client
    cfg = _get_cfg()
    if not cfg.get("enabled", False):
        logger.info("Redis disabled, using in-memory cache fallback")
        return None
    try:
        import redis
        _client = redis.Redis(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 6379)),
            db=int(cfg.get("db", 0)),
            password=cfg.get("password") or None,
            socket_connect_timeout=3,
            socket_timeout=3,
            decode_responses=True,
        )
        _client.ping()
        logger.info(f"Redis connected: {cfg['host']}:{cfg['port']}")
        return _client
    except Exception as e:
        logger.warning(f"Redis unavailable ({e}), using in-memory fallback")
        return None

def cache_get(key: str) -> dict | None:
    client = get_client()
    if client is None:
        return None
    try:
        raw = client.get(f"que:cache:{key}")
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.debug(f"Redis get error: {e}")
        return None

def cache_set(key: str, value: dict, ttl: int = 300) -> None:
    client = get_client()
    if client is None:
        return
    try:
        client.setex(f"que:cache:{key}", ttl, json.dumps(value, default=str))
    except Exception as e:
        logger.debug(f"Redis set error: {e}")
