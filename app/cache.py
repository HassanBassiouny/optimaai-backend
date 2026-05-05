"""
app/cache.py
Redis caching layer — stores KPI results so dashboards
respond instantly without re-running ML inference every time
"""
import os
import json
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ── Connect ────────────────────────────────────────────────────────────────
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    print("  [cache] Redis connected.")
except Exception as e:
    r = None
    print(f"  [cache] Redis not available ({e}) — caching disabled.")


# ── Cache TTL settings (seconds) ──────────────────────────────────────────
TTL_KPI          = 300    # 5 minutes  — KPI snapshots
TTL_FORECAST     = 600    # 10 minutes — forecast results
TTL_RECOMMEND    = 3600   # 1 hour     — LLM recommendations
TTL_BMC          = 86400  # 24 hours   — Business Model Canvas


def cache_set(key: str, value: dict, ttl: int = TTL_KPI) -> bool:
    """Store a dict in Redis as JSON with a TTL."""
    if not r:
        return False
    try:
        r.setex(key, ttl, json.dumps(value, default=str))
        return True
    except Exception as e:
        print(f"  [cache] set error: {e}")
        return False


def cache_get(key: str) -> dict | None:
    """Retrieve a cached dict from Redis. Returns None if not found."""
    if not r:
        return None
    try:
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        print(f"  [cache] get error: {e}")
        return None


def cache_delete(key: str) -> bool:
    """Delete a specific cache key."""
    if not r:
        return False
    try:
        r.delete(key)
        return True
    except Exception:
        return False


def cache_delete_pattern(pattern: str) -> None:
    """Delete all Redis keys matching a pattern."""
    if not redis_client:
        return
    for key in redis_client.scan_iter(f"*{pattern}*"):
        redis_client.delete(key)


# ── Standard cache keys ────────────────────────────────────────────────────
def kpi_key()         -> str: return "optimaai:latest_kpi"
def forecast_key()    -> str: return "optimaai:latest_forecast"
def recommend_key(role: str) -> str: return f"optimaai:recommend:{role}"
def bmc_key()         -> str: return "optimaai:latest_bmc"
