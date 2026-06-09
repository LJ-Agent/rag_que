"""QueEngineService — gRPC service implementing the ICAR pipeline end-to-end."""
import time

import grpc

from communication.grpc_server.generated import que_pb2, que_pb2_grpc
from common.config_loader import get_config
from engine.models import (
    IntentResult, RewriteResult, DAGPlan, SubQuery, SubQueryResult,
    ExecutionTrace,
)
from engine.intent_recognizer import recognize
from engine.query_rewriter import rewrite
from engine.query_planner import plan
from engine.execution_router import route
from engine.dag_executor import DAGExecutor
from engine.result_synthesizer import synthesize
from engine.backend_registry import get_registry
from engine.builtin_backends import register_builtin_backends
from infrastructure.llm.adapter import health_check as llm_health
from loguru import logger


class QueEngineService(que_pb2_grpc.QueEngineServiceServicer):
    """ICAR pipeline orchestrator."""

    def __init__(self):
        self._executor = DAGExecutor()
        register_builtin_backends()

    def Execute(self, request, context):
        """Run the full ICAR pipeline: Intent → Clarify → Augment → Retrieve → Synthesize."""
        start_all = time.time()
        trace = ExecutionTrace()

        query = request.query
        context = dict(request.context)
        timeout_ms = request.timeout_ms if request.timeout_ms > 0 else 30000
        enable_hyde = request.enable_hyde
        enable_multi_query = request.enable_multi_query

        user_id = int(context.get("user_id", "0"))
        session_id = context.get("session_id", "")
        kb_ids = _parse_kb_ids(context)

        logger.info(
            f"QUE Execute: query='{query[:100]}', context_keys={list(context.keys())[:5]}"
        )

        # --- Stage 1: Intent Recognition ---
        stage_start = time.time()
        intent = recognize(query, user_id, session_id)
        trace.add("intent", f"primary={intent.primary_intent}, confidence={intent.confidence:.2f}",
                   latency_ms=(time.time() - stage_start) * 1000)

        # --- Stage 2: Query Rewrite (Clarify + Augment) ---
        stage_start = time.time()
        rewrite_result = rewrite(
            query, user_id, session_id, intent,
            enable_hyde=enable_hyde, enable_multi_query=enable_multi_query,
        )
        trace.add("rewrite",
                   f"coref={rewrite_result.coreferenced_query != query}, "
                   f"expanded={len(rewrite_result.expanded_queries)}, "
                   f"hyde={len(rewrite_result.hyde_document) > 0}",
                   latency_ms=(time.time() - stage_start) * 1000)

        # --- Stage 3: Query Planning ---
        stage_start = time.time()
        dag_plan = plan(query, intent, rewrite_result, max_sub_queries)
        trace.add("plan",
                   f"sub_queries={dag_plan.total_queries}, waves={dag_plan.parallel_waves}",
                   latency_ms=(time.time() - stage_start) * 1000)

        # --- Stage 4: Route + Execute ---
        stage_start = time.time()
        for sq in dag_plan.sub_queries:
            sq.route = route(sq, intent)
        results = self._executor.execute(
            dag_plan, context, trace, timeout_ms,
        )
        trace.add("execute",
                   f"ok={sum(1 for r in results if r.success)}/{len(results)}",
                   latency_ms=(time.time() - stage_start) * 1000)

        # --- Stage 5: Synthesize ---
        stage_start = time.time()
        working_query = rewrite_result.completed_query or rewrite_result.coreferenced_query or query
        context = synthesize(working_query, intent, dag_plan, results, trace)
        trace.add("synthesize",
                   f"context_length={len(context)}",
                   latency_ms=(time.time() - stage_start) * 1000)

        total_latency = (time.time() - start_all) * 1000
        logger.info(f"QUE Execute complete: {total_latency:.0f}ms")

        # Build rewritten queries list
        rewritten = []
        if rewrite_result.coreferenced_query and rewrite_result.coreferenced_query != query:
            rewritten.append(rewrite_result.coreferenced_query)
        if rewrite_result.completed_query:
            rewritten.append(rewrite_result.completed_query)
        rewritten.extend(rewrite_result.expanded_queries)
        if rewrite_result.hyde_document:
            rewritten.append(f"[HyDE]: {rewrite_result.hyde_document[:200]}")

        return que_pb2.QueResponse(
            original_query=query,
            rewritten_queries=rewritten,
            plan=_dag_plan_to_proto(dag_plan),
            sub_results=[_sub_result_to_proto(r) for r in results],
            synthesized_context=context,
            trace=_trace_to_proto(trace),
            total_latency_ms=total_latency,
        )

    def ExecuteStream(self, request, context):
        """Server-streaming ICAR pipeline with live progress events."""
        start_all = time.time()
        trace = ExecutionTrace()

        query = request.query
        context = dict(request.context)
        timeout_ms = request.timeout_ms if request.timeout_ms > 0 else 30000
        enable_hyde = request.enable_hyde
        enable_multi_query = request.enable_multi_query

        user_id = int(context.get("user_id", "0"))
        session_id = context.get("session_id", "")

        logger.info(
            f"QUE ExecuteStream: query='{query[:100]}', context_keys={list(context.keys())[:5]}"
        )

        def _emit(event_type, stage, message, progress, final_response=None, chunk_content=""):
            event = que_pb2.QueStreamEvent(
                event_type=event_type, stage=stage, message=message,
                progress_pct=progress,
            )
            if final_response is not None:
                event.final_response.CopyFrom(final_response)
            if chunk_content:
                event.chunk_content = chunk_content
            context.write(event)

        # Stage 1: Intent
        _emit("stage_start", "intent", "Analyzing query intent...", 0.0)
        t0 = time.time()
        intent = recognize(query, user_id, session_id)
        trace.add("intent", f"primary={intent.primary_intent}, confidence={intent.confidence:.2f}",
                   latency_ms=(time.time() - t0) * 1000)
        _emit("stage_complete", "intent",
               f"Intent: {intent.primary_intent} (confidence={intent.confidence:.2f})", 10.0)

        # Stage 2: Rewrite
        _emit("stage_start", "rewrite", "Rewriting and expanding query...", 15.0)
        t0 = time.time()
        rewrite_result = rewrite(
            query, user_id, session_id, intent,
            enable_hyde=enable_hyde, enable_multi_query=enable_multi_query,
        )
        trace.add("rewrite",
                   f"coref={rewrite_result.coreferenced_query != query}, "
                   f"expanded={len(rewrite_result.expanded_queries)}",
                   latency_ms=(time.time() - t0) * 1000)
        _emit("stage_complete", "rewrite",
               f"Query rewritten: {len(rewrite_result.expanded_queries)} expansions, "
               f"HyDE={'yes' if rewrite_result.hyde_document else 'no'}", 30.0)

        # Stage 3: Plan
        _emit("stage_start", "plan", "Planning retrieval strategy...", 35.0)
        t0 = time.time()
        dag_plan = plan(query, intent, rewrite_result, max_sub_queries)
        trace.add("plan",
                   f"sub_queries={dag_plan.total_queries}, waves={dag_plan.parallel_waves}",
                   latency_ms=(time.time() - t0) * 1000)
        _emit("stage_complete", "plan",
               f"Plan: {dag_plan.total_queries} sub-queries in {dag_plan.parallel_waves} waves", 45.0)

        # Stage 4: Route + Execute
        _emit("stage_start", "execute", f"Executing {dag_plan.total_queries} sub-queries...", 50.0)
        t0 = time.time()
        for sq in dag_plan.sub_queries:
            sq.route = route(sq, intent)
        results = self._executor.execute(
            dag_plan, context, trace, timeout_ms,
        )
        ok_count = sum(1 for r in results if r.success)
        trace.add("execute",
                   f"ok={ok_count}/{len(results)}",
                   latency_ms=(time.time() - t0) * 1000)
        _emit("stage_complete", "execute",
               f"Execution: {ok_count}/{len(results)} sub-queries succeeded", 70.0)

        # Stage 5: Synthesize
        _emit("stage_start", "synthesize", "Synthesizing results...", 75.0)
        t0 = time.time()
        working_query = rewrite_result.completed_query or rewrite_result.coreferenced_query or query
        context = synthesize(working_query, intent, dag_plan, results, trace)
        trace.add("synthesize",
                   f"context_length={len(context)}",
                   latency_ms=(time.time() - t0) * 1000)
        _emit("stage_complete", "synthesize",
               f"Context synthesized: {len(context)} chars", 90.0)

        total_latency = (time.time() - start_all) * 1000
        logger.info(f"QUE ExecuteStream complete: {total_latency:.0f}ms")

        # Build final response
        rewritten = []
        if rewrite_result.coreferenced_query and rewrite_result.coreferenced_query != query:
            rewritten.append(rewrite_result.coreferenced_query)
        if rewrite_result.completed_query:
            rewritten.append(rewrite_result.completed_query)
        rewritten.extend(rewrite_result.expanded_queries)
        if rewrite_result.hyde_document:
            rewritten.append(f"[HyDE]: {rewrite_result.hyde_document[:200]}")

        final_resp = que_pb2.QueResponse(
            original_query=query,
            rewritten_queries=rewritten,
            plan=_dag_plan_to_proto(dag_plan),
            sub_results=[_sub_result_to_proto(r) for r in results],
            synthesized_context=context,
            trace=_trace_to_proto(trace),
            total_latency_ms=total_latency,
        )
        _emit("final", "synthesize", f"Complete - {total_latency:.0f}ms", 100.0,
              final_response=final_resp)


    def HealthCheck(self, request, context):
        """Health check — uses BackendRegistry for dynamic status."""
        llm_status = "unknown"
        try:
            llm_status = "healthy" if llm_health() else "unhealthy"
        except Exception as e:
            llm_status = f"error: {e}"

        registry = get_registry()
        backends = registry.health_summary()

        healthy = llm_status == "healthy" and all(
            b.get("status") == "healthy" for b in backends
        )
        return que_pb2.HealthCheckResponse(
            healthy=healthy,
            backend_health=[
                que_pb2.BackendHealth(
                    backend_name=b["backend_name"],
                    status=b["status"],
                )
                for b in backends
            ],
            llm_status=llm_status,
        )

    def ListBackends(self, request, context):
        """List all registered backends and their capabilities."""
        registry = get_registry()
        backends = registry.list_all()
        return que_pb2.ListBackendsResponse(
            backends=[
                que_pb2.BackendInfo(
                    name=b.name,
                    capabilities=[c.value for c in b.capabilities],
                    status="healthy" if b.health_check() else "unhealthy",
                )
                for b in backends
            ]
        )

    def shutdown(self):
        self._executor.shutdown()


# --------------- proto conversion helpers ---------------

def _dag_plan_to_proto(p: DAGPlan) -> que_pb2.DAGPlan:
    return que_pb2.DAGPlan(
        sub_queries=[
            que_pb2.QueSubQuery(
                query_id=sq.query_id,
                query_text=sq.query_text,
                route=sq.route,
                dependency_ids=sq.dependencies,
                parallel_group=sq.parallel_group,
            )
            for sq in p.sub_queries
        ],
        parallel_waves=p.parallel_waves,
        total_queries=p.total_queries,
        complexity_level=p.complexity_level,
        primary_intent=p.primary_intent,
    )


def _sub_result_to_proto(r: SubQueryResult) -> que_pb2.SubQueryResult:
    return que_pb2.SubQueryResult(
        query_id=r.query_id,
        query_text=r.query_text,
        route=r.route,
        results=[_search_result_to_proto(c) for c in r.chunks],
        direct_answer=r.direct_answer,
        latency_ms=r.latency_ms,
        success=r.success,
        error=r.error,
    )


def _search_result_to_proto(r: dict) -> "SearchResult":
    """Convert internal SearchResult dict to proto."""
    meta = r.get("metadata", {}) or {}
    return que_pb2.SearchResult(
        id=str(r.get("id", "")),
        content=r.get("content", ""),
        metadata={k: str(v) for k, v in meta.items()},
        score=float(r.get("score", 0)),
        source_backend=r.get("source_backend", ""),
    )


def _trace_to_proto(t: ExecutionTrace) -> que_pb2.ExecutionTrace:
    return que_pb2.ExecutionTrace(
        entries=[
            que_pb2.TraceEntry(
                stage=e.stage,
                description=e.description,
                latency_ms=e.latency_ms,
                metadata=e.metadata,
            )
            for e in t.entries
        ]
    )


def _parse_kb_ids(context: dict[str, str]) -> list[int]:
    """Extract kb_ids from context map."""
    raw = context.get("kb_ids", "")
    if not raw:
        return []
    try:
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except (ValueError, TypeError):
        return []
