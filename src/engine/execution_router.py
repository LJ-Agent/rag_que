"""Execution Router — routes each sub-query to the correct execution channel."""
import re

from engine.models import SubQuery, IntentResult
from common.enums import RouteType, IntentCategory


# Patterns that indicate the sub-query should use direct LLM reasoning
_DIRECT_LLM_PATTERNS = [
    r"^(?:what is the definition of|define|explain\s+the\s+concept|summarize|paraphrase)",
    r"^(?:translate|calculate|compute|convert)",
    r"^(?:how would you|what do you think|what is your opinion|imagine|suppose)",
]

# Patterns that indicate user-specific memory lookup
_MEMORY_PATTERNS = [
    r"\b(?:I|my|me|mine|we|our|us)\b",
    r"(?:上次|之前|以前|曾经|说过|提到过|我记得)",
    r"(?:what did I|what have I|what was my|remember when)",
]


def route(sub_query: SubQuery, intent: IntentResult) -> str:
    """Assign the sub-query to the best execution channel.

    Priority:
    1. Pre-assigned route from planner
    2. Memory lookup if has user-specific references
    3. Direct LLM if definitional/opinion/translation
    4. Default: RAG retrieval

    Args:
        sub_query: The sub-query to route.
        intent: The classified intent for context.

    Returns:
        Route string: rag_retrieval, direct_llm, or memory_lookup
    """
    query = sub_query.query_text

    # Honor pre-assigned route from planner (unless default)
    if sub_query.route != RouteType.RAG_RETRIEVAL:
        return sub_query.route

    # Check memory lookup
    if _matches_any(query, _MEMORY_PATTERNS):
        return RouteType.MEMORY_LOOKUP

    # Check direct LLM
    if intent.primary_intent == IntentCategory.OPEN_DISCUSSION:
        return RouteType.DIRECT_LLM

    if _matches_any(query, _DIRECT_LLM_PATTERNS):
        return RouteType.DIRECT_LLM

    # Default
    return RouteType.RAG_RETRIEVAL


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)
