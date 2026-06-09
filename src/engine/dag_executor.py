"""DAG Executor — wave-based parallel sub-query execution with caching and fault tolerance."""
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Any

try:
    from infrastructure.redis.client import cache_get, cache_set
    _REDIS_AVAILABLE = True
except Exception:
    _REDIS_AVAILABLE = False

from engine.models import (
    DAGPlan, SubQuery, SubQueryResult, ExecutionTrace,
)
from common.enums import RouteType
from common.config_loader import get_config
from loguru import logger


class DAGExecutor:
    """Executes a DAG plan wave-by-wave using ThreadPoolExecutor.

    Features:
    - Wave-based parallel execution (independent nodes in same wave run concurrently)
    - In-memory result cache (query_hash → SubQueryResult, TTL-based)
    - Per-sub-query timeout; failure marked as success=false
    - Dependency result injection for multi-hop chains
    """

    def __init__(self, max_workers: int | None = None):
        if max_workers is None:
            max_workers = int(get_config()["engine"].get("max_workers", 10))
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._cache: dict[str, SubQueryResult] = {}
        self._cache_lock = threading.Lock()
        self._cache_timestamps: dict[str, float] = {}
        self._cache_ttl = float(get_config()["engine"].get("cache_ttl", 300))

    def execute(
        self,
        plan: DAGPlan,
        user_id: int,
        session_id: str,
        kb_ids: list[int],
        trace: ExecutionTrace,
        timeout_ms: int = 30000,
    ) -> list[SubQueryResult]:
        """Execute the DAG plan.

        Args:
            plan: The execution plan with sub-queries.
            user_id: For memory lookups.
            session_id: For memory lookups.
            kb_ids: Knowledge base IDs for RAG retrieval.
            trace: Execution trace to record execution steps.
            timeout_ms: Total timeout budget.

        Returns:
            List of SubQueryResult, one per sub-query.
        """
        start_all = time.time()
        results: dict[str, SubQueryResult] = {}

        # Group by wave
        wave_map: dict[int, list[SubQuery]] = {}
        for sq in plan.sub_queries:
            wave_map.setdefault(sq.parallel_group, []).append(sq)

        waves = [wave_map[i] for i in sorted(wave_map.keys())]

        # Per-wave timeout (divide total budget across waves + 1)
        per_wave_timeout = timeout_ms / 1000 / (len(waves) + 1) if waves else timeout_ms / 1000

        for wave_idx, wave_queries in enumerate(waves):
            wave_start = time.time()
            logger.info(f"Wave {wave_idx}: executing {len(wave_queries)} sub-queries")

            futures_map: dict[Any, SubQuery] = {}
            for sq in wave_queries:
                # Check cache
                cache_key = _make_cache_key(sq.query_text, kb_ids)
                cached = self._get_cached(cache_key)
                if cached:
                    results[sq.query_id] = cached
                    logger.debug(f"Cache hit: {sq.query_id}")
                    continue

                # Inject dependency results
                enriched_text = _inject_dependencies(sq, results)
                sq.query_text = enriched_text

                # Submit
                future = self._executor.submit(
                    _execute_one, sq, user_id, session_id, kb_ids
                )
                futures_map[future] = sq

            # Wait for wave completion
            for future in as_completed(futures_map, timeout=per_wave_timeout):
                sq = futures_map[future]
                try:
                    result = future.result(timeout=10)
                    results[sq.query_id] = result
                    self._set_cache(_make_cache_key(sq.query_text, kb_ids), result)
                except Exception as e:
                    logger.error(f"Sub-query {sq.query_id} failed: {e}")
                    results[sq.query_id] = SubQueryResult(
                        query_id=sq.query_id,
                        query_text=sq.query_text,
                        route=sq.route,
                        success=False,
                        error=str(e),
                    )

            # Mark timed-out futures as failed
            for future, sq in futures_map.items():
                if sq.query_id not in results:
                    results[sq.query_id] = SubQueryResult(
                        query_id=sq.query_id,
                        query_text=sq.query_text,
                        route=sq.route,
                        success=False,
                        error="timeout",
                    )

            wave_elapsed = (time.time() - wave_start) * 1000
            trace.add("execute", f"Wave {wave_idx}: {len(wave_queries)} queries, {wave_elapsed:.0f}ms")

        total_elapsed = (time.time() - start_all) * 1000
        logger.info(f"DAG execution complete: {len(results)} results in {total_elapsed:.0f}ms")
        return [results.get(sq.query_id) or SubQueryResult(query_id=sq.query_id)
                for sq in plan.sub_queries]

    def _get_cached(self, key: str) -> SubQueryResult | None:
        if _REDIS_AVAILABLE:
            try:
                data = cache_get(key)
                if data:
                    return SubQueryResult(**data)
            except Exception:
                pass
        with self._cache_lock:
            ts = self._cache_timestamps.get(key, 0)
            if time.time() - ts > self._cache_ttl:
                self._cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
                return None
            return self._cache.get(key)

    def _set_cache(self, key: str, result: SubQueryResult) -> None:
        with self._cache_lock:
            self._cache[key] = result
            self._cache_timestamps[key] = time.time()
        if _REDIS_AVAILABLE:
            try:
                cache_set(key, {
                    "query_id": result.query_id, "query_text": result.query_text,
                    "route": result.route, "chunks": result.chunks,
                    "direct_answer": result.direct_answer,
                    "latency_ms": result.latency_ms,
                    "success": result.success, "error": result.error,
                }, ttl=self._cache_ttl)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)


def _execute_one(
    sq: SubQuery,
    user_id: int,
    session_id: str,
    kb_ids: list[int],
) -> SubQueryResult:
    """Execute a single sub-query against the appropriate backend."""
    start = time.time()
    try:
        if sq.route == RouteType.RAG_RETRIEVAL:
            from infrastructure.rag_client.retrieval_client import retrieve
            resp = retrieve(sq.query_text, kb_ids, top_k=10)
            chunks = [
                {
                    "chunk_id": c.chunk_id,
                    "document_id": c.document_id,
                    "document_name": c.document_name,
                    "content": c.content,
                    "score": c.score,
                    "chunk_index": c.chunk_index,
                }
                for c in resp.chunks
            ]
            return SubQueryResult(
                query_id=sq.query_id, query_text=sq.query_text, route=sq.route,
                chunks=chunks, latency_ms=(time.time() - start) * 1000, success=True,
            )

        elif sq.route == RouteType.DIRECT_LLM:
            from infrastructure.llm.adapter import chat
            answer = chat(
                [{"role": "user", "content": sq.query_text}],
                temperature=0.3, max_tokens=1024,
            )
            return SubQueryResult(
                query_id=sq.query_id, query_text=sq.query_text, route=sq.route,
                direct_answer=answer, latency_ms=(time.time() - start) * 1000, success=True,
            )

        elif sq.route == RouteType.MEMORY_LOOKUP:
            from infrastructure.memory_client.search_client import search_memory
            resp = search_memory(user_id, sq.query_text, top_k=10)
            chunks = [
                {
                    "chunk_id": f.fact_id,
                    "document_id": 0,
                    "document_name": f"memory:{f.category}",
                    "content": f.content,
                    "score": f.importance,
                    "chunk_index": 0,
                }
                for f in resp.result.facts
            ]
            return SubQueryResult(
                query_id=sq.query_id, query_text=sq.query_text, route=sq.route,
                chunks=chunks, latency_ms=(time.time() - start) * 1000, success=True,
            )

        else:
            return SubQueryResult(
                query_id=sq.query_id, query_text=sq.query_text, route=sq.route,
                success=False, error=f"Unknown route: {sq.route}",
            )

    except Exception as e:
        return SubQueryResult(
            query_id=sq.query_id, query_text=sq.query_text, route=sq.route,
            success=False, error=str(e),
            latency_ms=(time.time() - start) * 1000,
        )


def _make_cache_key(query_text: str, kb_ids: list[int]) -> str:
    raw = f"{query_text}:{sorted(kb_ids)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _inject_dependencies(sq: SubQuery, results: dict[str, SubQueryResult]) -> str:
    """Prepend dependency results to the sub-query text for multi-hop reasoning."""
    if not sq.dependencies:
        return sq.query_text

    dep_contexts: list[str] = []
    for dep_id in sq.dependencies:
        if dep_id in results and results[dep_id].success:
            dep = results[dep_id]
            if dep.chunks:
                chunk_summary = "\n".join(c.get("content", "")[:200] for c in dep.chunks[:3])
                dep_contexts.append(f"[Result from previous step]: {chunk_summary}")
            elif dep.direct_answer:
                dep_contexts.append(f"[Result from previous step]: {dep.direct_answer}")

    if dep_contexts:
        enriched = "\n".join(dep_contexts) + "\n\nQuestion: " + sq.query_text
        return enriched
    return sq.query_text
