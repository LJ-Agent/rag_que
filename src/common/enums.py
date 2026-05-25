"""Enumerations for QUE Engine domain."""


class IntentCategory:
    FACT_LOOKUP = "fact_lookup"
    COMPARISON = "comparison"
    MULTI_HOP = "multi_hop"
    PROCEDURAL = "procedural"
    OPEN_DISCUSSION = "open_discussion"

    ALL = {FACT_LOOKUP, COMPARISON, MULTI_HOP, PROCEDURAL, OPEN_DISCUSSION}


class ComplexityLevel:
    SIMPLE = "simple"
    MULTI_HOP = "multi-hop"
    COMPOUND = "compound"


class RouteType:
    RAG_RETRIEVAL = "rag_retrieval"
    DIRECT_LLM = "direct_llm"
    MEMORY_LOOKUP = "memory_lookup"


class SynthesizerMode:
    COMPARE = "compare"
    CHAIN = "chain"
    AGGREGATE = "aggregate"
    JUDGE = "judge"
