"""Tenant Manager — multi-tenant isolation for QUE Engine.

Features:
- Per-tenant quota: QPS, max_sub_queries, timeout
- Dedicated DAGExecutor per tenant (resource isolation)
- Backend allowlisting per tenant
- Custom pipeline per tenant
- Cache key prefix per tenant
"""
from dataclasses import dataclass, field
from typing import Any
import threading
import time

from loguru import logger


@dataclass
class TenantConfig:
    """Configuration for a single tenant."""
    tenant_id: str
    # Quotas
    quota_max_qps: int = 100
    quota_max_sub_queries: int = 8
    quota_timeout_ms: int = 30000
    # Backend control
    allowed_backends: list[str] | None = None   # None = all backends allowed
    denied_backends: list[str] = field(default_factory=list)
    # Pipeline
    pipeline_name: str = "icar"                  # preset name or "custom"
    custom_pipeline_stages: list[str] | None = None
    # Cache
    cache_ttl: int = 300
    cache_enabled: bool = True
    # Labels
    labels: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "quota_max_qps": self.quota_max_qps,
            "quota_max_sub_queries": self.quota_max_sub_queries,
            "quota_timeout_ms": self.quota_timeout_ms,
            "allowed_backends": self.allowed_backends,
            "pipeline_name": self.pipeline_name,
        }


class TokenBucket:
    """Simple token bucket rate limiter."""

    def __init__(self, rate: float, burst: int = 10):
        self.rate = rate           # tokens per second
        self.burst = burst         # max tokens
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    @property
    def available(self) -> float:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            return min(self.burst, self._tokens + elapsed * self.rate)


class TenantManager:
    """Multi-tenant isolation manager.

    Usage:
        mgr = TenantManager()
        mgr.register(TenantConfig(tenant_id="project-a", quota_max_qps=50))
        mgr.register(TenantConfig(tenant_id="project-b", quota_max_qps=200))

        # In request handler:
        tenant = mgr.get("project-a")
        if not mgr.check_rate_limit("project-a"):
            raise RateLimitError()
        executor = mgr.get_executor("project-a")
        results = executor.execute(plan, context, trace, tenant.quota_timeout_ms)
    """

    def __init__(self):
        self._tenants: dict[str, TenantConfig] = {}
        self._rate_limiters: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    # ---- Registration ----

    def register(self, config: TenantConfig) -> None:
        with self._lock:
            self._tenants[config.tenant_id] = config
            # Create rate limiter
            self._rate_limiters[config.tenant_id] = TokenBucket(
                rate=config.quota_max_qps,
                burst=config.quota_max_qps,
            )
            logger.info(
                f"Tenant registered: {config.tenant_id} "
                f"(qps={config.quota_max_qps}, max_sub={config.quota_max_sub_queries}, "
                f"pipeline={config.pipeline_name})"
            )

    def unregister(self, tenant_id: str) -> None:
        with self._lock:
            self._tenants.pop(tenant_id, None)
            self._rate_limiters.pop(tenant_id, None)
            logger.info(f"Tenant unregistered: {tenant_id}")

    def get(self, tenant_id: str) -> TenantConfig | None:
        return self._tenants.get(tenant_id)

    def list_all(self) -> list[TenantConfig]:
        return list(self._tenants.values())

    # ---- Rate Limiting ----

    def check_rate_limit(self, tenant_id: str) -> bool:
        """Check if tenant has available QPS quota."""
        limiter = self._rate_limiters.get(tenant_id)
        if limiter is None:
            return True  # No limiter = no limit
        return limiter.consume(1)

    def get_available_qps(self, tenant_id: str) -> float:
        limiter = self._rate_limiters.get(tenant_id)
        if limiter is None:
            return float("inf")
        return limiter.available

    # ---- Backend Filtering ----

    def filter_backends(self, tenant_id: str, backend_names: list[str]) -> list[str]:
        """Filter backend list to only those allowed for this tenant."""
        tenant = self._tenants.get(tenant_id)
        if tenant is None:
            return backend_names  # No tenant = allow all

        allowed = tenant.allowed_backends
        denied = set(tenant.denied_backends)

        if allowed is not None:
            return [n for n in backend_names if n in allowed and n not in denied]
        return [n for n in backend_names if n not in denied]

    # ---- Tenant-specific pipeline ----

    def get_pipeline_name(self, tenant_id: str) -> str:
        tenant = self._tenants.get(tenant_id)
        if tenant is None:
            return "icar"
        return tenant.pipeline_name

    # ---- Cache key prefix ----

    def cache_key(self, tenant_id: str, key: str) -> str:
        """Generate tenant-scoped cache key."""
        return f"tenant:{tenant_id}:{key}"

    # ---- Config loading ----

    def load_from_config(self, tenants_cfg: list[dict]) -> int:
        """Load tenants from YAML config entries."""
        loaded = 0
        for cfg in tenants_cfg:
            tid = cfg.get("tenant_id", "")
            if not tid:
                continue
            self.register(TenantConfig(
                tenant_id=tid,
                quota_max_qps=int(cfg.get("max_qps", 100)),
                quota_max_sub_queries=int(cfg.get("max_sub_queries", 8)),
                quota_timeout_ms=int(cfg.get("timeout_ms", 30000)),
                allowed_backends=cfg.get("allowed_backends"),
                pipeline_name=cfg.get("pipeline", "icar"),
                labels=cfg.get("labels", {}),
            ))
            loaded += 1
        logger.info(f"Loaded {loaded} tenants from config")
        return loaded


# Global singleton
_tenant_manager: TenantManager | None = None


def get_tenant_manager() -> TenantManager:
    global _tenant_manager
    if _tenant_manager is None:
        _tenant_manager = TenantManager()
        # Register default tenant
        _tenant_manager.register(TenantConfig(
            tenant_id="default",
            quota_max_qps=100,
            quota_max_sub_queries=8,
            quota_timeout_ms=30000,
            pipeline_name="icar",
        ))
    return _tenant_manager
