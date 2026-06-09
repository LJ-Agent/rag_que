"""REST API Gateway for QUE Engine — FastAPI-based HTTP interface.

Endpoints:
  POST /api/v1/execute     — Run QUE pipeline
  GET  /api/v1/health      — Health check
  GET  /api/v1/backends    — List registered backends
  GET  /api/v1/tenants     — List registered tenants
  GET  /api/v1/presets     — List pipeline presets
  GET  /metrics            — Prometheus metrics

Start: uvicorn api.rest_server:app --host 0.0.0.0 --port 8080
"""
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from engine.intent_recognizer import recognize
from engine.query_rewriter import rewrite
from engine.query_planner import plan
from engine.execution_router import route
from engine.dag_executor import DAGExecutor
from engine.result_synthesizer import synthesize
from engine.backend_registry import get_registry
from engine.builtin_backends import register_builtin_backends
from engine.tenant import get_tenant_manager
from engine.pipeline import QuePipeline
from engine.models import ExecutionTrace
from infrastructure.telemetry import (
    get_tracer, get_metrics, QUE_REQUEST_COUNT, QUE_LATENCY_HISTOGRAM,
)
from common.config_loader import get_config
from loguru import logger


# ---- App ----
app = FastAPI(
    title="QUE Engine API",
    description="Query Understanding & Execution Engine",
    version="2.0.0",
)

_executor = DAGExecutor()
_initialized = False


def _ensure_init():
    global _initialized
    if not _initialized:
        register_builtin_backends()
        _initialized = True


# ---- Request/Response Models ----

class ExecuteRequest(BaseModel):
    query: str
    context: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    max_sub_queries: int = 8
    timeout_ms: int = 30000
    enable_hyde: bool = True
    enable_multi_query: bool = True
    tenant_id: str = "default"
    trace_id: str = ""


class SubQueryResultModel(BaseModel):
    query_id: str
    query_text: str
    route: str
    results_count: int
    success: bool
    latency_ms: float


class ExecuteResponse(BaseModel):
    original_query: str
    rewritten_queries: list[str]
    synthesized_context: str
    sub_results: list[SubQueryResultModel]
    plan_summary: dict
    total_latency_ms: float
    trace_id: str
    tenant_id: str


class HealthResponse(BaseModel):
    status: str
    version: str
    backends: list[dict]
    tenants: int


# ---- Routes ----

@app.post("/api/v1/execute", response_model=ExecuteResponse)
async def execute(req: ExecuteRequest):
    """Execute the full QUE pipeline."""
    _ensure_init()
    tracer = get_tracer()
    metrics = get_metrics()
    trace_id = req.trace_id or str(uuid.uuid4())[:8]
    t0 = time.time()

    # Tenant check
    tenant_mgr = get_tenant_manager()
    tenant = tenant_mgr.get(req.tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail=f"Tenant '{req.tenant_id}' not found")
    if not tenant_mgr.check_rate_limit(req.tenant_id):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    with tracer.start_as_current_span("QUE.Execute") as span:
        span.set_attribute("que.query", req.query[:100])
        span.set_attribute("que.tenant_id", req.tenant_id)
        span.set_attribute("que.trace_id", trace_id)

        trace = ExecutionTrace()

        # ICAR Pipeline
        # Stage 1: Intent
        with tracer.start_as_current_span("QUE.intent"):
            intent = recognize(req.query)
            trace.add("intent", f"primary={intent.primary_intent}", latency_ms=0)

        # Stage 2: Rewrite
        with tracer.start_as_current_span("QUE.rewrite"):
            rw = rewrite(req.query, enable_hyde=req.enable_hyde,
                         enable_multi_query=req.enable_multi_query, intent=intent)
            trace.add("rewrite", f"expanded={len(rw.expanded_queries)}")

        # Stage 3: Plan
        with tracer.start_as_current_span("QUE.plan"):
            dag = plan(req.query, intent, rw, req.max_sub_queries)
            trace.add("plan", f"sub_queries={dag.total_queries}")

        # Stage 4: Execute
        with tracer.start_as_current_span("QUE.execute"):
            context = dict(req.context)
            for sq in dag.sub_queries:
                sq.route = route(sq, intent)
            results = _executor.execute(dag, context, trace, req.timeout_ms)
            trace.add("execute", f"ok={sum(1 for r in results if r.success)}/{len(results)}")

        # Stage 5: Synthesize
        with tracer.start_as_current_span("QUE.synthesize"):
            wq = rw.completed_query or rw.coreferenced_query or req.query
            ctx = synthesize(wq, intent, dag, results, trace)

        latency = (time.time() - t0) * 1000
        span.set_attribute("que.latency_ms", latency)

        # Metrics
        QUE_REQUEST_COUNT.add(1, {"tenant": req.tenant_id, "intent": intent.primary_intent})
        QUE_LATENCY_HISTOGRAM.record(latency, {"tenant": req.tenant_id})

        # Build response
        rewritten = []
        if rw.coreferenced_query and rw.coreferenced_query != req.query:
            rewritten.append(rw.coreferenced_query)
        if rw.completed_query:
            rewritten.append(rw.completed_query)
        rewritten.extend(rw.expanded_queries)

        return ExecuteResponse(
            original_query=req.query,
            rewritten_queries=rewritten,
            synthesized_context=ctx,
            sub_results=[
                SubQueryResultModel(
                    query_id=r.query_id, query_text=r.query_text,
                    route=r.route, results_count=len(r.chunks),
                    success=r.success, latency_ms=r.latency_ms,
                )
                for r in results
            ],
            plan_summary={
                "total_queries": dag.total_queries,
                "parallel_waves": dag.parallel_waves,
                "primary_intent": intent.primary_intent,
                "complexity": intent.complexity_level,
            },
            total_latency_ms=latency,
            trace_id=trace_id,
            tenant_id=req.tenant_id,
        )


@app.get("/api/v1/health", response_model=HealthResponse)
async def health():
    """Health check with backend status."""
    _ensure_init()
    registry = get_registry()
    tenant_mgr = get_tenant_manager()
    return HealthResponse(
        status="healthy",
        version="2.0.0",
        backends=registry.health_summary(),
        tenants=len(tenant_mgr.list_all()),
    )


@app.get("/api/v1/backends")
async def list_backends():
    """List registered backends."""
    _ensure_init()
    registry = get_registry()
    return {
        "backends": [
            {"name": b.name, "capabilities": [c.value for c in b.capabilities],
             "healthy": registry.is_healthy(b.name)}
            for b in registry.list_all()
        ]
    }


@app.get("/api/v1/tenants")
async def list_tenants():
    """List registered tenants with quota info."""
    tenant_mgr = get_tenant_manager()
    return {
        "tenants": [
            {
                "tenant_id": t.tenant_id,
                "qps_limit": t.quota_max_qps,
                "available_qps": tenant_mgr.get_available_qps(t.tenant_id),
                "pipeline": t.pipeline_name,
            }
            for t in tenant_mgr.list_all()
        ]
    }


@app.get("/api/v1/presets")
async def list_presets():
    """List available pipeline presets."""
    return {"presets": QuePipeline.list_presets()}


# ---- Startup ----
@app.on_event("startup")
async def startup():
    _ensure_init()
    logger.info("REST API Gateway started on :8080")


@app.on_event("shutdown")
async def shutdown():
    _executor.shutdown()
    logger.info("REST API Gateway shut down")
