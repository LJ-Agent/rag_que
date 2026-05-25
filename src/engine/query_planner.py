"""Query Planner — complexity assessment and DAG plan generation."""
import time
from uuid import uuid4

from engine.models import (
    DAGPlan, SubQuery, IntentResult, RewriteResult,
)
from common.enums import IntentCategory, ComplexityLevel, RouteType
from common.config_loader import get_config
from infrastructure.llm.adapter import chat_structured
from loguru import logger


def _split_comparison(query: str) -> list[SubQuery]:
    """Split a comparison question into A/B sub-queries."""
    try:
        result = chat_structured(
            [{"role": "user", "content": f"""Split this comparison question into exactly 2 independent sub-queries
(one for each side of the comparison). Each sub-query should request facts about one side only.

Question: {query}

Output JSON:
{{
  "side_a": "<query about side A only>",
  "side_b": "<query about side B only>",
  "aspect": "<what is being compared>"
}}

Return JSON only."""}],
            temperature=0.1,
            max_tokens=512,
        )
        qa_id = str(uuid4())
        qb_id = str(uuid4())
        return [
            SubQuery(query_id=qa_id, query_text=result.get("side_a", f"facts about the first item in: {query}"),
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
            SubQuery(query_id=qb_id, query_text=result.get("side_b", f"facts about the second item in: {query}"),
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
        ]
    except Exception as e:
        logger.warning(f"LLM comparison split failed: {e}, using fallback")
        return [
            SubQuery(query_id=str(uuid4()), query_text=f"information about the first subject in: {query}",
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
            SubQuery(query_id=str(uuid4()), query_text=f"information about the second subject in: {query}",
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
        ]


def _split_multi_hop(query: str) -> list[SubQuery]:
    """Decompose a multi-hop question into a dependency chain."""
    try:
        result = chat_structured(
            [{"role": "user", "content": f"""Decompose this multi-hop question into a chain of 2-3 sub-questions.
Each sub-question should build on the answer from the previous one.
The last sub-question should be the original question.

Question: {query}

Output JSON:
{{
  "sub_queries": [
    {{"step": 1, "query": "<first sub-question, no dependencies>", "depends_on": []}},
    {{"step": 2, "query": "<second sub-question>", "depends_on": [1]}}
  ]
}}

Return JSON only."""}],
            temperature=0.1,
            max_tokens=512,
        )
        sub_queries_raw = result.get("sub_queries", [])
        queries: list[SubQuery] = []
        for sq in sub_queries_raw:
            queries.append(SubQuery(
                query_id=str(uuid4()),
                query_text=sq["query"],
                route=RouteType.RAG_RETRIEVAL,
                dependencies=[queries[i - 1].query_id for i in sq.get("depends_on", []) if i - 1 < len(queries)],
                parallel_group=sq.get("step", 1) - 1,
            ))
        return queries
    except Exception as e:
        logger.warning(f"LLM multi-hop split failed: {e}")
        return [SubQuery(query_id=str(uuid4()), query_text=query, route=RouteType.RAG_RETRIEVAL, parallel_group=0)]


def plan(
    query: str,
    intent: IntentResult,
    rewrite: RewriteResult | None = None,
    max_sub_queries: int | None = None,
) -> DAGPlan:
    """Generate the DAG execution plan based on intent and complexity.

    Args:
        query: The (potentially rewritten) user query.
        intent: Classified intent with complexity level.
        rewrite: Rewrite result (expanded queries may become sub-queries).
        max_sub_queries: Cap on sub-queries.

    Returns:
        DAGPlan with sub_queries and parallel wave count.
    """
    if max_sub_queries is None:
        max_sub_queries = int(get_config()["engine"]["max_sub_queries"])
    start = time.time()

    working_query = query
    if rewrite and rewrite.coreferenced_query:
        working_query = rewrite.coreferenced_query

    sub_queries: list[SubQuery] = []

    complexity = intent.complexity_level
    primary = intent.primary_intent

    # ---------- Plan by complexity ----------
    if complexity == ComplexityLevel.SIMPLE or primary == IntentCategory.FACT_LOOKUP:
        # Single retrieval
        sub_queries = [
            SubQuery(query_id=str(uuid4()), query_text=working_query,
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
        ]
        # Optionally add expanded queries as parallel sub-queries
        if rewrite and rewrite.expanded_queries:
            for eq in rewrite.expanded_queries[:max_sub_queries - 1]:
                sub_queries.append(
                    SubQuery(query_id=str(uuid4()), query_text=eq,
                             route=RouteType.RAG_RETRIEVAL, parallel_group=0)
                )

    elif primary == IntentCategory.COMPARISON:
        sub_queries = _split_comparison(working_query)

    elif primary == IntentCategory.MULTI_HOP:
        sub_queries = _split_multi_hop(working_query)

    elif primary == IntentCategory.PROCEDURAL:
        sub_queries = [
            SubQuery(query_id=str(uuid4()), query_text=working_query,
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
        ]

    elif primary == IntentCategory.OPEN_DISCUSSION:
        sub_queries = [
            SubQuery(query_id=str(uuid4()), query_text=working_query,
                     route=RouteType.DIRECT_LLM, parallel_group=0),
        ]

    else:
        sub_queries = [
            SubQuery(query_id=str(uuid4()), query_text=working_query,
                     route=RouteType.RAG_RETRIEVAL, parallel_group=0),
        ]

    # Cap sub-queries
    sub_queries = sub_queries[:max_sub_queries]

    # Compute parallel waves
    max_wave = max((sq.parallel_group for sq in sub_queries), default=0) + 1
    max_depth = max_wave

    dag = DAGPlan(
        sub_queries=sub_queries,
        total_queries=len(sub_queries),
        parallel_waves=max_wave,
        max_depth=max_depth,
        complexity_level=complexity,
        primary_intent=primary,
    )

    elapsed = (time.time() - start) * 1000
    logger.info(
        f"Plan: {dag.total_queries} sub-queries, {dag.parallel_waves} waves, "
        f"intent={primary}, {elapsed:.0f}ms"
    )
    return dag
