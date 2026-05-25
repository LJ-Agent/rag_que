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
