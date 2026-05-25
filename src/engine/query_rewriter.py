"""Query Rewriter — coreference resolution, multi-query expansion, HyDE, completion."""
import json
import re
import time

from engine.models import RewriteResult, IntentResult
from infrastructure.llm.adapter import chat_structured
from infrastructure.llm.prompts import (
    REWRITE_COREFERENCE_PROMPT,
    MULTI_QUERY_EXPANSION_PROMPT,
    HYDE_PROMPT,
    QUERY_COMPLETION_PROMPT,
)
from common.config_loader import get_config
from loguru import logger


def _resolve_coreferences(
    query: str, user_id: int, session_id: str
) -> tuple[str, list[str]]:
    """Replace pronouns and vague references with explicit entities.

    Fetches working memory from RAG-MEMORY to build context.
    """
    # Try to fetch working memory for context
    context = ""
    try:
        from infrastructure.memory_client.search_client import get_working_memory
        resp = get_working_memory(user_id, session_id, timeout=5)
        wm_entries = resp.result.working_entries if resp.result else []
        if wm_entries:
            items = [f"{e.key}: {e.value}" for e in wm_entries[-10:]]
            context = "\n".join(items)
    except Exception as e:
        logger.debug(f"Could not fetch working memory for coreference: {e}")

    # Check for pronouns / vague references
    pronoun_hints = re.findall(
        r"(?:这个|那个|它|他|她|他们|她们|这些|那些|刚才|上次|之前|上面|前面|the|this|that|these|those|it|they)",
        query, re.IGNORECASE,
    )

    if not pronoun_hints and not context:
        return query, []

    if not context:
        return query, []

    try:
        result = chat_structured(
            [{"role": "user", "content": REWRITE_COREFERENCE_PROMPT.format(
                context=context, query=query
            )}],
            temperature=0.1,
            max_tokens=512,
        )
        resolved = result.get("resolved_query", query)
        entities = result.get("resolved_entities", [])
        logger.info(f"Coreference resolved: entities={entities}")
        return resolved, entities
    except Exception as e:
        logger.warning(f"Coreference resolution failed: {e}")
        return query, []


def _expand_multi_query(query: str, count: int = 4) -> list[str]:
    """Generate 3-5 semantically equivalent queries to boost recall."""
    try:
        result = chat_structured(
            [{"role": "user", "content": MULTI_QUERY_EXPANSION_PROMPT.format(
                query=query, count=count
            )}],
            temperature=0.4,
            max_tokens=1024,
        )
        expanded = result.get("expanded_queries", [])
        logger.info(f"Multi-query expansion: generated {len(expanded)} variants")
        return expanded[:count]
    except Exception as e:
        logger.warning(f"Multi-query expansion failed: {e}")
        return []


def _generate_hyde(query: str) -> str:
    """Generate Hypothetical Document Embedding for better retrieval."""
    try:
        result = chat_structured(
            [{"role": "user", "content": HYDE_PROMPT.format(query=query)}],
            temperature=0.3,
            max_tokens=1024,
        )
        hyde_doc = result.get("hypothetical_document", "")
        logger.info(f"HyDE generated: {len(hyde_doc)} chars")
        return hyde_doc
    except Exception as e:
        logger.warning(f"HyDE generation failed: {e}")
        return ""


def _complete_query(query: str) -> str:
    """Expand short/ambiguous queries."""
    if len(query) >= 15 and not query.rstrip().endswith("?"):
        return query

    try:
        result = chat_structured(
            [{"role": "user", "content": QUERY_COMPLETION_PROMPT.format(query=query)}],
            temperature=0.2,
            max_tokens=512,
        )
        completed = result.get("completed_query", query)
        logger.info(f"Query completed: '{query}' -> '{completed}'")
        return completed
    except Exception as e:
        logger.warning(f"Query completion failed: {e}")
        return query


def rewrite(
    query: str,
    user_id: int = 0,
    session_id: str = "",
    intent: IntentResult | None = None,
    enable_hyde: bool = True,
    enable_multi_query: bool = True,
) -> RewriteResult:
    """Execute all rewrite operations.

    Pipeline: coreference → completion → (multi_query || hyde)
    """
    start = time.time()
    engine_cfg = get_config()["engine"]

    if enable_hyde is None:
        enable_hyde = engine_cfg.get("hyde_enabled", True)
    if enable_multi_query is None:
        enable_multi_query = engine_cfg.get("multi_query_enabled", True)

    result = RewriteResult()

    # Step 1: Coreference resolution
    coreferenced, entities = _resolve_coreferences(query, user_id, session_id)
    result.coreferenced_query = coreferenced
    working_query = coreferenced if coreferenced != query else query

    # Step 2: Query completion (for short/ambiguous queries)
    completed = _complete_query(working_query)
    result.completed_query = completed
    working_query = completed if completed != working_query else working_query

    # Step 3 & 4: Multi-query expansion and HyDE (parallel-able)
    if enable_multi_query and intent and intent.complexity_level != "simple":
        result.expanded_queries = _expand_multi_query(working_query)

    if enable_hyde:
        result.hyde_document = _generate_hyde(working_query)

    elapsed = (time.time() - start) * 1000
    logger.info(
        f"Rewrite complete: coreferenced={coreferenced != query}, "
        f"expanded={len(result.expanded_queries)}, hyde={len(result.hyde_document) > 0}, "
        f"{elapsed:.0f}ms"
    )
    return result


