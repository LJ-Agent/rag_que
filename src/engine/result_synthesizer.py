"""Result Synthesizer — merges multi-path results into a single context for LLM generation."""
import time
from typing import Any


def _ngram_jaccard(text1: str, text2: str, n: int = 3) -> float:
    """Compute n-gram Jaccard similarity between two texts."""
    if not text1 or not text2:
        return 0.0

    def _ngrams(s: str, k: int) -> set:
        s = s.lower().strip()
        if len(s) < k:
            return {s}
        return {s[i:i+k] for i in range(len(s) - k + 1)}

    ngrams1 = _ngrams(text1, n)
    ngrams2 = _ngrams(text2, n)
    if not ngrams1 or not ngrams2:
        return 0.0
    intersection = ngrams1 & ngrams2
    union = ngrams1 | ngrams2
    return len(intersection) / len(union) if union else 0.0


def _is_duplicate(content: str, seen_contents: list, threshold: float = 0.85) -> bool:
    """Check if content is a near-duplicate of any previously seen content."""
    if not content:
        return True
    for seen in seen_contents:
        if _ngram_jaccard(content, seen) >= threshold:
            return True
    return False


from engine.models import (
    DAGPlan, SubQueryResult, IntentResult, ExecutionTrace,
)
from common.enums import IntentCategory, SynthesizerMode
from infrastructure.llm.adapter import chat
from infrastructure.llm.prompts import JUDGE_CONFLICT_PROMPT
from loguru import logger


def synthesize(
    original_query: str,
    intent: IntentResult,
    plan: DAGPlan,
    results: list[SubQueryResult],
    trace: ExecutionTrace,
) -> str:
    """Synthesize multi-path results into a single structured context string.

    Strategy depends on intent type:
    - comparison → side-by-side
    - multi_hop → chained
    - compound → aggregated + deduplicated
    - simple → direct concatenation
    """
    start = time.time()

    # Filter successful results
    ok_results = [r for r in results if r.success]
    if not ok_results:
        trace.add("synthesize", "No successful results, using fallback")
        return _fallback(original_query, results)

    mode = _select_mode(intent)
    context = ""

    if mode == SynthesizerMode.COMPARE:
        context = _synthesize_compare(ok_results)
    elif mode == SynthesizerMode.CHAIN:
        context = _synthesize_chain(ok_results, plan)
    elif mode == SynthesizerMode.AGGREGATE:
        context = _synthesize_aggregate(ok_results)
    else:
        context = _synthesize_simple(ok_results)

    elapsed = (time.time() - start) * 1000
    trace.add("synthesize", f"Mode={mode}, {len(ok_results)} results, {elapsed:.0f}ms",
              context_length=str(len(context)))
    logger.info(f"Synthesized: mode={mode}, context={len(context)} chars, {elapsed:.0f}ms")
    return context


def _select_mode(intent: IntentResult) -> str:
    if intent.primary_intent == IntentCategory.COMPARISON:
        return SynthesizerMode.COMPARE
    if intent.primary_intent == IntentCategory.MULTI_HOP:
        return SynthesizerMode.CHAIN
    if intent.complexity_level == "compound":
        return SynthesizerMode.AGGREGATE
    return SynthesizerMode.AGGREGATE  # default


def _synthesize_compare(results: list[SubQueryResult]) -> str:
    """Side-by-side comparison format."""
    parts: list[str] = ["[COMPARISON CONTEXT]\n"]

    if len(results) >= 2:
        a, b = results[0], results[1]
        parts.append("=== Aspect A ===")
        for c in a.chunks:
            parts.append(f"- [{c.get('metadata', {}).get('document_name', c.get('source_backend', 'source'))}] {c.get('content', '')}")
        parts.append("\n=== Aspect B ===")
        for c in b.chunks:
            parts.append(f"- [{c.get('metadata', {}).get('document_name', c.get('source_backend', 'source'))}] {c.get('content', '')}")

    # Remaining results
    for r in results[2:]:
        parts.append(f"\n=== Additional: {r.query_text[:80]} ===")
        for c in r.chunks:
            parts.append(f"- {c.get('content', '')}")

    return "\n".join(parts)


def _synthesize_chain(results: list[SubQueryResult], plan: DAGPlan) -> str:
    """Causal chain format for multi-hop results."""
    parts: list[str] = ["[MULTI-HOP REASONING CHAIN]\n"]

    sorted_results = sorted(results, key=lambda r: _find_wave(r.query_id, plan))
    for i, r in enumerate(sorted_results):
        parts.append(f"--- Step {i + 1}: {r.query_text[:100]} ---")
        for c in r.chunks:
            parts.append(f"- [{c.get('metadata', {}).get('document_name', c.get('source_backend', 'source'))}] {c.get('content', '')}")
        if r.direct_answer:
            parts.append(f"- [Direct]: {r.direct_answer}")

    return "\n".join(parts)


def _synthesize_aggregate(results: list[SubQueryResult]) -> str:
    """Aggregate + deduplicate multi-path results."""
    parts: list[str] = ["[AGGREGATED KNOWLEDGE CONTEXT]\n"]

    # N-gram Jaccard similarity dedup (threshold=0.85)
    seen_contents: list[str] = []
    for r in results:
        for c in r.chunks:
            content = c.get("content", "")
            if _is_duplicate(content, seen_contents, threshold=0.85):
                continue
            seen_contents.append(content)
            score = c.get("score", 0)
            parts.append(f"[score={score:.3f}] [{c.get('metadata', {}).get('document_name', c.get('source_backend', 'source'))}] {content}")

    # Add direct answers
    for r in results:
        if r.direct_answer:
            parts.append(f"\n[Direct Answer from {r.query_text[:60]}]: {r.direct_answer}")

    return "\n".join(parts)


def _synthesize_simple(results: list[SubQueryResult]) -> str:
    """Simple concatenation for single-retrieval results."""
    parts: list[str] = ["[RETRIEVAL CONTEXT]\n"]
    for r in results:
        for c in r.chunks:
            parts.append(f"[{c.get('metadata', {}).get('document_name', c.get('source_backend', 'source'))}] {c.get('content', '')}")
    return "\n".join(parts)


def _fallback(query: str, results: list[SubQueryResult]) -> str:
    """Fallback when all sub-queries failed."""
    errors = [f"{r.query_id}: {r.error}" for r in results if r.error]
    return f"[RETRIEVAL FAILED] All sub-queries for '{query}' failed. Errors: {'; '.join(errors[:3])}"


def _find_wave(query_id: str, plan: DAGPlan) -> int:
    for sq in plan.sub_queries:
        if sq.query_id == query_id:
            return sq.parallel_group
    return 999
