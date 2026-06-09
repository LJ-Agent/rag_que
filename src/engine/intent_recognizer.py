"""Intent Recognizer — rule-based classification with LLM fallback."""
import re
import time
from typing import Any

from engine.models import IntentResult
from infrastructure.llm.adapter import chat_structured
from infrastructure.llm.prompts import get_intent_prompt
from common.enums import IntentCategory, ComplexityLevel
from common.config_loader import get_config
from loguru import logger


# --------------- rule-based patterns ---------------

_COMPARISON_PATTERNS = [
    r"(?:vs\.?|versus|和|与|相比|比较|对比|哪个(?:更|最)?好|区别|差异|不同|选哪个|or\b.{0,20}\bor\b)",
    r"(?:compare|difference between|which is better|what is the difference)",
]

_MULTI_HOP_PATTERNS = [
    r"(?:谁的|哪(?:个|家|位).*(?:的|拥有|属于|生产|制造|开发|发行))",
    r"(?:what is the.*of the.*that|who (?:owns|created|built|made))",
]

_PROCEDURAL_PATTERNS = [
    r"(?:如何|怎么|怎样|步骤|方法|流程|教程|指南|how\s+(?:to|do|can|should|would))",
]

_FACT_LOOKUP_PATTERNS = [
    r"(?:什么是|什么是|定义|概念|含义|解释|who is|what (?:is|are|was|were)|when (?:is|was|did))",
]


def _match_any(text: str, patterns: list[str]) -> float:
    """Return highest confidence score from matching patterns."""
    best = 0.0
    for p in patterns:
        if re.search(p, text.lower()):
            best = max(best, 0.75 if len(p) > 20 else 0.6)
    return best


def _rule_classify(query: str) -> IntentResult:
    """Fast rule-based classification."""
    # Check comparison
    comp_score = _match_any(query, _COMPARISON_PATTERNS)
    if comp_score > 0.6:
        return IntentResult(
            primary_intent=IntentCategory.COMPARISON,
            sub_intents=[IntentCategory.FACT_LOOKUP],
            confidence=comp_score,
            required_data_sources=["kb"],
            complexity_level=ComplexityLevel.COMPOUND,
        )

    # Check multi-hop
    mh_score = _match_any(query, _MULTI_HOP_PATTERNS)
    if mh_score > 0.6:
        return IntentResult(
            primary_intent=IntentCategory.MULTI_HOP,
            confidence=mh_score,
            required_data_sources=["kb"],
            complexity_level=ComplexityLevel.MULTI_HOP,
        )

    # Check procedural
    proc_score = _match_any(query, _PROCEDURAL_PATTERNS)
    if proc_score > 0.6:
        return IntentResult(
            primary_intent=IntentCategory.PROCEDURAL,
            confidence=proc_score,
            required_data_sources=["kb"],
            complexity_level=ComplexityLevel.SIMPLE,
        )

    # Check fact lookup
    fact_score = _match_any(query, _FACT_LOOKUP_PATTERNS)
    if fact_score > 0.4:
        return IntentResult(
            primary_intent=IntentCategory.FACT_LOOKUP,
            confidence=fact_score,
            required_data_sources=["kb"],
            complexity_level=ComplexityLevel.SIMPLE,
        )

    # Default: low-confidence fact lookup
    return IntentResult(
        primary_intent=IntentCategory.FACT_LOOKUP,
        confidence=0.35,
        required_data_sources=["kb"],
        complexity_level=ComplexityLevel.SIMPLE,
    )


def recognize(query: str, user_id: int = 0, session_id: str = "") -> IntentResult:
    """Classify user intent — rule-first, LLM fallback on low confidence.

    Args:
        query: User's raw question
        user_id: For future memory-augmented intent (currently unused)
        session_id: For future memory-augmented intent (currently unused)

    Returns:
        IntentResult with primary_intent, confidence, complexity_level
    """
    start = time.time()
    threshold = float(get_config()["engine"]["rule_confidence_threshold"])

    # Stage 1: Rule-based classification (fast path)
    result = _rule_classify(query)

    # Stage 2: LLM fallback for low confidence
    if result.confidence < threshold:
        logger.info(f"Rule confidence {result.confidence:.2f} < {threshold}, falling back to LLM")
        try:
            llm_result = chat_structured(
                [{"role": "user", "content": get_intent_prompt(query)}],
                temperature=0.1,
                max_tokens=512,
            )
            result = IntentResult(
                primary_intent=llm_result.get("primary_intent", IntentCategory.FACT_LOOKUP),
                sub_intents=llm_result.get("sub_intents", []),
                confidence=llm_result.get("confidence", 0.7),
                required_data_sources=llm_result.get("required_data_sources", ["kb"]),
                complexity_level=llm_result.get("complexity_level", ComplexityLevel.SIMPLE),
            )
        except Exception as e:
            logger.warning(f"LLM intent fallback failed: {e}, using rule result")

    elapsed = (time.time() - start) * 1000
    logger.info(
        f"Intent: {result.primary_intent} (confidence={result.confidence:.2f}, "
        f"complexity={result.complexity_level}, {elapsed:.0f}ms)"
    )
    return result
