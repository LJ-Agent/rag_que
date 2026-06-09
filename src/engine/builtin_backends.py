"""Built-in backends for QUE Engine."""
import time
from typing import Any

from engine.backend_registry import (
    SearchBackend, SearchResult, BackendCapability, get_registry,
)
from common.config_loader import get_config
from loguru import logger


class RAGRetrievalBackend(SearchBackend):
    """Backend that calls RAG-PYTHON retrieval service via gRPC."""

    @property
    def name(self) -> str:
        return "rag_retrieval"

    @property
    def capabilities(self) -> list[BackendCapability]:
        return [BackendCapability.SEARCH]

    @property
    def route_patterns(self) -> list[str]:
        return [r".*"]  # default catch-all

    def search(self, query: str, context: dict[str, str], top_k: int = 10,
               timeout: float = 30.0) -> list[SearchResult]:
        kb_ids = _parse_kb_ids(context)
        from infrastructure.rag_client.retrieval_client import retrieve
        resp = retrieve(query, kb_ids, top_k=top_k, timeout=timeout)
        results = []
        for c in resp.chunks:
            results.append(SearchResult(
                id=c.chunk_id,
                content=c.content,
                metadata={
                    "document_id": str(c.document_id),
                    "document_name": c.document_name,
                    "chunk_index": str(c.chunk_index),
                },
                score=c.score,
                source_backend=self.name,
            ))
        return results

    def health_check(self) -> bool:
        try:
            import grpc
            cfg = get_config()["clients"]["rag_python"]
            grpc.channel_ready_future(
                grpc.insecure_channel(f"{cfg['host']}:{cfg['port']}")
            ).result(timeout=3)
            return True
        except Exception:
            return False


class LLMBackend(SearchBackend):
    """Backend that answers directly via LLM (no retrieval)."""

    @property
    def name(self) -> str:
        return "direct_llm"

    @property
    def capabilities(self) -> list[BackendCapability]:
        return [BackendCapability.REASONING]

    @property
    def route_patterns(self) -> list[str]:
        return [
            r"^(?:what is the definition|define|translate|calculate|compute|convert)",
            r"^(?:how would you|what do you think|what is your opinion)",
        ]

    def search(self, query: str, context: dict[str, str], top_k: int = 10,
               timeout: float = 30.0) -> list[SearchResult]:
        from infrastructure.llm.adapter import chat
        answer = chat([{"role": "user", "content": query}], temperature=0.3, max_tokens=1024)
        return [SearchResult(
            id="llm_direct",
            content=answer,
            metadata={"method": "direct_llm"},
            score=1.0,
            source_backend=self.name,
        )]

    def health_check(self) -> bool:
        try:
            from infrastructure.llm.adapter import health_check
            return health_check()
        except Exception:
            return False


class MemoryBackend(SearchBackend):
    """Backend that searches user memory via RAG-MEMORY."""

    @property
    def name(self) -> str:
        return "memory_lookup"

    @property
    def capabilities(self) -> list[BackendCapability]:
        return [BackendCapability.MEMORY]

    @property
    def route_patterns(self) -> list[str]:
        return [
            r"\b(?:I|my|me|mine|we|our|us)\b",
            r"(?:上次|之前|以前|曾经|说过|提到过|我记得)",
            r"(?:what did I|what have I|what was my|remember when)",
        ]

    def search(self, query: str, context: dict[str, str], top_k: int = 10,
               timeout: float = 30.0) -> list[SearchResult]:
        user_id = int(context.get("user_id", "0"))
        from infrastructure.memory_client.search_client import search_memory
        resp = search_memory(user_id, query, top_k=top_k, timeout=timeout)
        results = []
        for f in resp.result.facts:
            results.append(SearchResult(
                id=f.fact_id,
                content=f.content,
                metadata={"category": f.category, "importance": str(f.importance)},
                score=f.importance,
                source_backend=self.name,
            ))
        return results

    def health_check(self) -> bool:
        try:
            import grpc
            cfg = get_config()["clients"]["rag_memory"]
            grpc.channel_ready_future(
                grpc.insecure_channel(f"{cfg['host']}:{cfg['port']}")
            ).result(timeout=3)
            return True
        except Exception:
            return False


def _parse_kb_ids(context: dict[str, str]) -> list[int]:
    """Extract kb_ids from context map."""
    raw = context.get("kb_ids", "")
    if not raw:
        return []
    try:
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except (ValueError, TypeError):
        return []


def register_builtin_backends() -> None:
    """Register all built-in backends. Call once at startup."""
    registry = get_registry()
    registry.register(RAGRetrievalBackend())
    registry.register(LLMBackend())
    registry.register(MemoryBackend())
    logger.info("Built-in backends registered: rag_retrieval, direct_llm, memory_lookup")
