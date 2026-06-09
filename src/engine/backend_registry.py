"""Backend Registry — pluggable execution backends for QUE Engine."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
import re

from loguru import logger


class BackendCapability(str, Enum):
    SEARCH = "search"
    MEMORY = "memory"
    REASONING = "reasoning"
    API_CALL = "api_call"


@dataclass
class SearchResult:
    """Generic search result — replaces DocumentChunk."""
    id: str = ""
    content: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    score: float = 0.0
    source_backend: str = ""


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


class BackendRegistry:
    """Global registry for search backends.

    Usage:
        registry = BackendRegistry()
        registry.register(MyCustomBackend())
        backend = registry.resolve("what is X?", intent)
    """

    def __init__(self):
        self._backends: dict[str, SearchBackend] = {}
        self._route_cache: dict[str, str] = {}  # query_hash -> backend_name

    def register(self, backend: SearchBackend) -> None:
        name = backend.name
        if name in self._backends:
            logger.warning(f"Backend '{name}' is being replaced")
        self._backends[name] = backend
        logger.info(f"Backend registered: {name} caps={[c.value for c in backend.capabilities]}")

    def unregister(self, name: str) -> None:
        self._backends.pop(name, None)
        logger.info(f"Backend unregistered: {name}")

    def get(self, name: str) -> SearchBackend | None:
        return self._backends.get(name)

    def list_all(self) -> list[SearchBackend]:
        return list(self._backends.values())

    def resolve(
        self,
        query: str,
        preferred_route: str = "",
        hints: list[dict] | None = None,
    ) -> str:
        """Resolve which backend to use for a given query.

        Priority:
        1. Explicit preferred_route from planner (if it matches a registered backend)
        2. BackendHint from the request
        3. Regex route_patterns matching
        4. Default: first registered backend with SEARCH capability
        """
        # Honour explicit route if backend exists
        if preferred_route and preferred_route in self._backends:
            return preferred_route

        # Hints from request
        if hints:
            for hint in sorted(hints, key=lambda h: h.get("weight", 0), reverse=True):
                name = hint.get("backend_name", "")
                if name in self._backends:
                    return name

        # Regex matching
        for backend in self._backends.values():
            for pattern in backend.route_patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    return backend.name

        # Default: first SEARCH-capable backend
        for backend in self._backends.values():
            if BackendCapability.SEARCH in backend.capabilities:
                return backend.name

        # Last resort
        if self._backends:
            return next(iter(self._backends.keys()))
        return ""

    def health_summary(self) -> list[dict]:
        """Return health status of all registered backends."""
        result = []
        for name, backend in self._backends.items():
            try:
                healthy = backend.health_check()
                result.append({
                    "backend_name": name,
                    "status": "healthy" if healthy else "unhealthy",
                })
            except Exception as e:
                result.append({"backend_name": name, "status": f"error: {e}"})
        return result


# Global singleton
_registry: BackendRegistry | None = None


def get_registry() -> BackendRegistry:
    global _registry
    if _registry is None:
        _registry = BackendRegistry()
    return _registry
