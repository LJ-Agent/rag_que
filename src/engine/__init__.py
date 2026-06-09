"""QUE Engine package — v2 Phase 2: Plugin Architecture."""
from engine.backend_registry import (
    BackendRegistry, SearchBackend, SearchResult, BackendScore,
    BackendCapability, get_registry,
)
from engine.models import (
    IntentResult, RewriteResult, DAGPlan, SubQuery, SubQueryResult,
    ExecutionTrace, TraceEntry,
)
from engine.builtin_backends import register_builtin_backends
from engine.pipeline import QuePipeline, PipelineStage, PipelineResult
from engine.tenant import TenantManager, TenantConfig, get_tenant_manager

__all__ = [
    # Backend
    "BackendRegistry", "SearchBackend", "SearchResult", "BackendScore",
    "BackendCapability", "get_registry",
    # Models
    "IntentResult", "RewriteResult", "DAGPlan", "SubQuery", "SubQueryResult",
    "ExecutionTrace", "TraceEntry",
    # Pipeline
    "QuePipeline", "PipelineStage", "PipelineResult",
    # Tenant
    "TenantManager", "TenantConfig", "get_tenant_manager",
    # Built-in
    "register_builtin_backends",
]
