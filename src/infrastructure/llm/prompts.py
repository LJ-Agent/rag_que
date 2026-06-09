"""Prompt templates for QUE Engine operations."""

INTENT_PROMPT = """Classify the user's question. Identify the primary intent, any sub-intents, and the complexity level.

Intent types:
- fact_lookup: simple fact or knowledge retrieval from documents
- comparison: comparing/contrasting A vs B
- multi_hop: requires chaining multiple facts (e.g. "what is X of the Y that Z")
- procedural: "how to" / step-by-step instructions
- open_discussion: opinion, analysis, or general reasoning

Complexity levels:
- simple: single fact, single retrieval step
- multi-hop: requires intermediate results to answer
- compound: multiple independent sub-questions

Output JSON:
{
  "primary_intent": "fact_lookup|comparison|multi_hop|procedural|open_discussion",
  "sub_intents": ["..."],
  "confidence": 0.0-1.0,
  "required_data_sources": ["kb", "memory", "general"],
  "complexity_level": "simple|multi-hop|compound"
}

User Question: {query}

Return JSON only."""


REWRITE_COREFERENCE_PROMPT = """Resolve ambiguous references in the user's query using conversation context.

Replace pronouns (it, they, this, that, he, she), vague references (the company, the project, the document), and implicit references with their explicit entities from the recent conversation context.

Recent context:
{context}

User query: {query}

Output JSON:
{
  "resolved_query": "<query with all references resolved>",
  "resolved_entities": ["entity1", "entity2"],
  "has_ambiguity": true/false
}

Return JSON only."""


MULTI_QUERY_EXPANSION_PROMPT = """Generate {count} semantically different reformulations of the user's question to improve document retrieval recall.

Each reformulation should express the same information need but using different:
- wording
- perspective
- level of specificity
- implicit assumptions

Original question: {query}

Output JSON:
{
  "expanded_queries": ["reformulated query 1", "reformulated query 2", ...]
}

Return JSON only."""


HYDE_PROMPT = """Generate a hypothetical ideal answer document for the following question, as if you were writing a comprehensive knowledge base article.

The document should be 3-5 sentences, factual, and contain the kind of information that would be found in a well-written reference document. This hypothetical document will be used to search for similar real documents.

Question: {query}

Output JSON:
{
  "hypothetical_document": "<3-5 sentence factual passage>"
}

Return JSON only."""


QUERY_COMPLETION_PROMPT = """The following user query is too short, vague, or incomplete. Expand it into a clear, specific, well-formed question suitable for document retrieval.

Add necessary context, clarify the intent, and make implicit assumptions explicit while staying true to what the user likely meant.

Short/vague query: {query}

Output JSON:
{
  "completed_query": "<expanded clear question>",
  "assumed_context": "<what was assumed>"
}

Return JSON only."""


JUDGE_CONFLICT_PROMPT = """Two or more retrieved sources provide conflicting information. Based on the source quality, recency, and coherence, determine the most reliable answer.

Original question: {query}

Conflicting sources:
{sources}

Output JSON:
{
  "resolved_answer": "<best supported conclusion>",
  "confidence": 0.0-1.0,
  "reasoning": "<brief explanation of why this source was preferred>",
  "preferred_source_ids": ["source_id_1"]
}

Return JSON only."""


# --------------- language detection ---------------

def _detect_language(text: str) -> str:
    """Detect whether the text is primarily Chinese or English."""
    chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    return "zh" if chinese_chars > len(text) * 0.15 else "en"


def get_intent_prompt(query: str) -> str:
    if _detect_language(query) == "zh":
        return INTENT_PROMPT_ZH.format(query=query)
    return INTENT_PROMPT.format(query=query)


def get_rewrite_coreference_prompt(context: str, query: str) -> str:
    if _detect_language(query) == "zh":
        return REWRITE_COREFERENCE_PROMPT_ZH.format(context=context, query=query)
    return REWRITE_COREFERENCE_PROMPT.format(context=context, query=query)


def get_multi_query_expansion_prompt(query: str, count: int = 4) -> str:
    if _detect_language(query) == "zh":
        return MULTI_QUERY_EXPANSION_PROMPT_ZH.format(query=query, count=count)
    return MULTI_QUERY_EXPANSION_PROMPT.format(query=query, count=count)


def get_hyde_prompt(query: str) -> str:
    if _detect_language(query) == "zh":
        return HYDE_PROMPT_ZH.format(query=query)
    return HYDE_PROMPT.format(query=query)


def get_query_completion_prompt(query: str) -> str:
    if _detect_language(query) == "zh":
        return QUERY_COMPLETION_PROMPT_ZH.format(query=query)
    return QUERY_COMPLETION_PROMPT.format(query=query)


# --------------- Chinese Prompt Templates ---------------

INTENT_PROMPT_ZH = """Analyze the user's question. Identify the primary intent, sub-intents, and complexity level.

Intent types:
- fact_lookup: simple fact or knowledge retrieval (e.g. "What is RAG")
- comparison: comparing/contrasting (e.g. "What's the difference between A and B")
- multi_hop: requires chaining multiple facts (e.g. "Who developed X and what company did they later found")
- procedural: "how to" / step-by-step instructions
- open_discussion: opinion, analysis, or general reasoning

Complexity levels:
- simple: single retrieval step
- multi-hop: requires intermediate results
- compound: multiple independent sub-questions

Return JSON:
{
  "primary_intent": "fact_lookup|comparison|multi_hop|procedural|open_discussion",
  "sub_intents": ["..."],
  "confidence": 0.0-1.0,
  "required_data_sources": ["kb", "memory", "general"],
  "complexity_level": "simple|multi-hop|compound"
}

User Question: {query}

Return JSON only."""


REWRITE_COREFERENCE_PROMPT_ZH = """Resolve ambiguous references in the user's query using conversation context.

Replace pronouns (it, they, this, that), vague references (the company, the project), and implicit references with their explicit entities.

Recent context:
{context}

User query: {query}

Return JSON:
{
  "resolved_query": "<query with all references resolved>",
  "resolved_entities": ["entity1", "entity2"],
  "has_ambiguity": true/false
}

Return JSON only."""


MULTI_QUERY_EXPANSION_PROMPT_ZH = """Generate {count} semantically different reformulations of the user's question to improve document retrieval recall.

Each reformulation should express the same information need but using different wording, perspective, specificity, and implicit assumptions.

Original question: {query}

Return JSON:
{
  "expanded_queries": ["reformulated query 1", "reformulated query 2", ...]
}

Return JSON only."""


HYDE_PROMPT_ZH = """Generate a hypothetical ideal answer document for the following question, as if you were writing a comprehensive knowledge base article.

The document should be 3-5 sentences, factual, and contain the kind of information found in a well-written reference document. This hypothetical document will be used to search for similar real documents.

Question: {query}

Return JSON:
{
  "hypothetical_document": "<3-5 sentence factual passage>"
}

Return JSON only."""


QUERY_COMPLETION_PROMPT_ZH = """The following user query is too short, vague, or incomplete. Expand it into a clear, specific, well-formed question suitable for document retrieval.

Add necessary context, clarify the intent, and make implicit assumptions explicit while staying true to what the user likely meant.

Short/vague query: {query}

Return JSON:
{
  "completed_query": "<expanded clear question>",
  "assumed_context": "<what was assumed>"
}

Return JSON only."""
