"""Backend Registry — pluggable execution backends for QUE Engine (v2 enhanced).

New in v2:
- resolve_with_scores(): multi-factor scoring for backend selection
- Config-driven backend registration from YAML
- Health check caching with TTL
- Backend weight-based load balancing hints
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
import re
import time as _time

from loguru import logger


class BackendCapability(str, Enum):
    SEARCH = "search"
    MEMORY = "memory"
    REASONING = "reasoning"
    API_CALL = "api_call"
    DATABASE = "database"


@dataclass
class SearchResult:
    """Generic search result — replaces DocumentChunk."""
    id: str = ""
    content: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    score: float = 0.0
    source_backend: str = ""


@dataclass
class BackendScore:
    """Scored backend candidate for routing decisions."""
    backend_name: str
    capability_match: float = 0.0     # 0-1
    pattern_match: float = 0.0        # 0-1
    hint_weight: float = 0.0          # 0-1 from request hints
    health_bonus: float = 0.0          # 0 or 0.1
    total_score: float = 0.0

    def __post_init__(self):
        self.total_score = (
            self.capability_match * 0.30 +
            self.pattern_match * 0.25 +
            self.hint_weight * 0.35 +
            self.health_bonus * 0.10
        )


class SearchBackend(ABC):
    """Pluggable backend interface.

    Any third-party service implements this interface and registers
    via BackendRegistry to participate in QUE's execution routing.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique backend identifier."""
        ...

    @property
    @abstractmethod
    def capabilities(self) -> list[BackendCapability]:
        """What this backend can do."""
        ...

    @property
    def route_patterns(self) -> list[str]:
        """Regex patterns that trigger this backend. Default: match all."""
        return [r".*"]

    @property
    def weight(self) -> float:
        """Default weight for load-balancing hints (0.0-1.0)."""
        return 0.5

    @abstractmethod
    def search(
        self,
        query: str,
        context: dict[str, str],
        top_k: int = 10,
        timeout: float = 30.0,
    ) -> list[SearchResult]:
        """Execute a search against this backend."""
        ...

    def health_check(self) -> bool:
        """Check backend health. Override for real health checks."""
        return True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "capabilities": [c.value for c in self.capabilities],
            "weight": self.weight,
        }


class BackendRegistry:
    """Global registry for search backends with multi-factor routing.

    Usage:
        registry = BackendRegistry()
        registry.register(MyCustomBackend())
        candidates = registry.resolve_with_scores("what is X?", intent, hints)
    """

    def __init__(self):
        self._backends: dict[str, SearchBackend] = {}
        self._health_cache: dict[str, tuple[bool, float]] = {}  # name -> (healthy, timestamp)
        self._health_ttl: float = 15.0  # cache health checks for 15s

    # ---- Registration ----

    def register(self, backend: SearchBackend) -> None:
        name = backend.name
        if name in self._backends:
            logger.warning(f"Backend '{name}' is being replaced")
        self._backends[name] = backend
        logger.info(f"Backend registered: {name} caps={[c.value for c in backend.capabilities]}")

    def unregister(self, name: str) -> None:
        self._backends.pop(name, None)
        self._health_cache.pop(name, None)
        logger.info(f"Backend unregistered: {name}")

    def get(self, name: str) -> SearchBackend | None:
        return self._backends.get(name)

    def list_all(self) -> list[SearchBackend]:
        return list(self._backends.values())

    # ---- Config-driven loading ----

    def load_from_config(self, backends_cfg: list[dict]) -> int:
        """Load backends from YAML config entries.

        Each entry: {name, type, host, port, weight, capabilities, route_patterns}
        Returns count of loaded backends.
        """
        loaded = 0
        for cfg in backends_cfg:
            btype = cfg.get("type", "")
            name = cfg.get("name", "")
            if not name or name in self._backends:
                continue

            if btype == "rag_retrieval":
                from engine.builtin_backends import RAGRetrievalBackend
                backend = RAGRetrievalBackend()
            elif btype == "llm":
                from engine.builtin_backends import LLMBackend
                backend = LLMBackend()
            elif btype == "memory":
                from engine.builtin_backends import MemoryBackend
                backend = MemoryBackend()
            else:
                logger.warning(f"Unknown backend type: {btype}")
                continue

            self.register(backend)
            loaded += 1

        logger.info(f"Loaded {loaded} backends from config")
        return loaded

    # ---- Health ----

    def is_healthy(self, name: str) -> bool:
        """Cached health check for a backend."""
        now = _time.time()
        if name in self._health_cache:
            status, ts = self._health_cache[name]
            if now - ts < self._health_ttl:
                return status

        backend = self._backends.get(name)
        if backend is None:
            return False

        try:
            healthy = backend.health_check()
        except Exception:
            healthy = False

        self._health_cache[name] = (healthy, now)
        return healthy

    def health_summary(self) -> list[dict]:
        result = []
        for name in self._backends:
            healthy = self.is_healthy(name)
            result.append({
                "backend_name": name,
                "status": "healthy" if healthy else "unhealthy",
            })
        return result

    def healthy_backends(self) -> list[SearchBackend]:
        return [b for b in self._backends.values() if self.is_healthy(b.name)]

    # ---- Resolve (simple) ----

    def resolve(
        self,
        query: str,
        preferred_route: str = "",
        hints: list[dict] | None = None,
    ) -> str:
        """Simple single-backend resolution. See resolve_with_scores for multi-factor."""
        candidates = self.resolve_with_scores(query, preferred_route, hints)
        if candidates:
            return candidates[0].backend_name
        return ""

    # ---- Resolve with scores (multi-factor) ----

    def resolve_with_scores(
        self,
        query: str,
        preferred_route: str = "",
        hints: list[dict] | None = None,
        required_capability: BackendCapability | None = None,
    ) -> list[BackendScore]:
        """Multi-factor backend scoring. Returns sorted list (best first).

        Scoring factors:
        1. Capability match (30%): does backend have required_capability?
        2. Pattern match (25%): does query match route_patterns?
        3. Hint weight (35%): was this backend requested with a weight?
        4. Health bonus (10%): is the backend currently healthy?
        """
        scores: list[BackendScore] = []
        hints_map: dict[str, float] = {}
        if hints:
            for h in hints:
                hints_map[h.get("backend_name", "")] = h.get("weight", 0.0)

        for name, backend in self._backends.items():
            score = BackendScore(backend_name=name)

            # 1. Capability match
            if required_capability:
                score.capability_match = 1.0 if required_capability in backend.capabilities else 0.0
            else:
                score.capability_match = 1.0  # no requirement = any backend qualifies

            # 2. Pattern match
            for pattern in backend.route_patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    score.pattern_match = 1.0
                    break
            # Bonus for more specific patterns
            if score.pattern_match and backend.route_patterns != [r".*"]:
                score.pattern_match = min(1.0, score.pattern_match + 0.1)

            # 3. Hint weight
            score.hint_weight = hints_map.get(name, 0.0)

            # 4. Health bonus
            if self.is_healthy(name):
                score.health_bonus = 0.1

            scores.append(score)

        # Sort by total_score descending
        scores.sort(key=lambda s: s.total_score, reverse=True)

        # If preferred_route is specified and available, boost it to top
        if preferred_route and preferred_route in self._backends:
            for s in scores:
                if s.backend_name == preferred_route:
                    s.total_score += 1.0  # strong boost
                    break
            scores.sort(key=lambda s: s.total_score, reverse=True)

        return scores

    def resolve_best(
        self,
        query: str,
        intent: Any = None,
        hints: list[dict] | None = None,
    ) -> SearchBackend | None:
        """Resolve and return the single best backend instance."""
        required = BackendCapability.SEARCH
        if intent:
            from common.enums import IntentCategory
            if intent.primary_intent == IntentCategory.OPEN_DISCUSSION:
                required = BackendCapability.REASONING

        candidates = self.resolve_with_scores(
            query, hints=hints, required_capability=required
        )
        if candidates:
            return self._backends.get(candidates[0].backend_name)
        return None


# Global singleton
_registry: BackendRegistry | None = None


def get_registry() -> BackendRegistry:
    global _registry
    if _registry is None:
        _registry = BackendRegistry()
    return _registry
