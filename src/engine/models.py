"""Internal domain dataclasses for QUE Engine pipeline."""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class IntentResult:
    primary_intent: str          # fact_lookup, comparison, multi_hop, procedural, open_discussion
    sub_intents: list[str] = field(default_factory=list)
    confidence: float = 0.5
    required_data_sources: list[str] = field(default_factory=list)
    complexity_level: str = "simple"  # simple, multi-hop, compound


@dataclass
class RewriteResult:
    coreferenced_query: str = ""
    expanded_queries: list[str] = field(default_factory=list)
    hyde_document: str = ""
    completed_query: str = ""


@dataclass
class SubQuery:
    query_id: str
    query_text: str
    route: str = "rag_retrieval"     # rag_retrieval, direct_llm, memory_lookup
    dependencies: list[str] = field(default_factory=list)
    parallel_group: int = 0
    result: Any = None                # SubQueryResult, filled after execution


@dataclass
class SubQueryResult:
    query_id: str
    query_text: str = ""
    route: str = ""
    chunks: list[Any] = field(default_factory=list)
    direct_answer: str = ""
    latency_ms: float = 0.0
    success: bool = True
    error: str = ""


@dataclass
class DAGPlan:
    sub_queries: list[SubQuery] = field(default_factory=list)
    total_queries: int = 0
    parallel_waves: int = 1
    max_depth: int = 1
    complexity_level: str = "simple"
    primary_intent: str = "fact_lookup"


@dataclass
class TraceEntry:
    stage: str
    description: str
    latency_ms: float = 0.0
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class ExecutionTrace:
    entries: list[TraceEntry] = field(default_factory=list)

    def add(self, stage: str, description: str, latency_ms: float = 0.0, **kwargs) -> None:
        self.entries.append(TraceEntry(
            stage=stage,
            description=description,
            latency_ms=latency_ms,
            metadata={k: str(v) for k, v in kwargs.items()},
        ))
