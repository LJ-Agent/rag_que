# QUE SDK

Python client for [QUE Engine](https://github.com/LJ-Agent/rag_que) — Query Understanding & Execution middleware.

## Installation

```bash
# In-process mode (no network overhead, requires que-engine source)
pip install que-sdk

# With gRPC support
pip install que-sdk[grpc]

# With REST support
pip install que-sdk[rest]

# All modes
pip install que-sdk[all]
```

## Quick Start

```python
from que_sdk import QueClient

# In-process (fastest)
client = QueClient(mode="inprocess")
result = client.execute("What is Retrieval-Augmented Generation?")
print(result.synthesized_context)

# Remote gRPC
client = QueClient(mode="grpc", host="que.example.com", port=50055)
result = client.execute("How does RAG work?")

# Remote REST
client = QueClient(mode="rest", base_url="http://que.example.com:8080")
result = client.execute("Compare RAG vs fine-tuning")
```

## Result

```python
@dataclass
class QueResult:
    original_query: str
    synthesized_context: str      # LLM-ready context
    intent: str                    # e.g. "comparison", "fact_lookup"
    sub_queries_executed: int
    total_latency_ms: float
    rewritten_queries: list[str]
    trace_id: str
```
