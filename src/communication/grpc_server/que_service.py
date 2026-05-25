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
from infrastructure.llm.adapter import health_check as llm_health
from loguru import logger


class QueEngineService(que_pb2_grpc.QueEngineServiceServicer):
    """ICAR pipeline orchestrator."""

    def __init__(self):
        self._executor = DAGExecutor()

    def Execute(self, request, context):
        """Run the full ICAR pipeline: Intent → Clarify → Augment → Retrieve → Synthesize."""
        start_all = time.time()
        trace = ExecutionTrace()

        query = request.query
        user_id = request.user_id
        session_id = request.session_id
        kb_ids = list(request.kb_ids)
        max_sub_queries = request.max_sub_queries if request.max_sub_queries > 0 else None
        timeout_ms = request.timeout_ms if request.timeout_ms > 0 else 30000
        enable_hyde = request.enable_hyde
        enable_multi_query = request.enable_multi_query

        logger.info(
            f"QUE Execute: user={user_id}, session={session_id}, "
            f"query='{query[:100]}', kb_ids={kb_ids[:5]}"
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
            dag_plan, user_id, session_id, kb_ids, trace, timeout_ms,
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

    def HealthCheck(self, request, context):
        """Health check — verify all downstream dependencies."""
        cfg = get_config()
        rag_status = "unknown"
        memory_status = "unknown"
        llm_status = "unknown"

        # LLM
        try:
            if llm_health():
                llm_status = "healthy"
            else:
                llm_status = "unhealthy"
        except Exception as e:
            llm_status = f"error: {e}"

        # RAG-PYTHON
        try:
            grpc.channel_ready_future(
                grpc.insecure_channel(
                    f"{cfg['clients']['rag_python']['host']}:{cfg['clients']['rag_python']['port']}"
                )
            ).result(timeout=3)
            rag_status = "healthy"
        except Exception as e:
            rag_status = f"unreachable: {e}"

        # RAG-MEMORY
        try:
            grpc.channel_ready_future(
                grpc.insecure_channel(
                    f"{cfg['clients']['rag_memory']['host']}:{cfg['clients']['rag_memory']['port']}"
                )
            ).result(timeout=3)
            memory_status = "healthy"
        except Exception as e:
            memory_status = f"unreachable: {e}"

        healthy = rag_status == "healthy" and llm_status == "healthy"
        return que_pb2.HealthCheckResponse(
            healthy=healthy,
            rag_retrieval_status=rag_status,
            rag_memory_status=memory_status,
            llm_status=llm_status,
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
        chunks=[_chunk_to_proto(c) for c in r.chunks],
        direct_answer=r.direct_answer,
        latency_ms=r.latency_ms,
        success=r.success,
        error=r.error,
    )


def _chunk_to_proto(c: dict) -> "DocumentChunk":
    # DocumentChunk is defined in retrieval_pb2
    from communication.grpc_server.generated import retrieval_pb2
    return retrieval_pb2.DocumentChunk(
        chunk_id=str(c.get("chunk_id", "")),
        document_id=str(c.get("document_id", 0)),
        document_name=c.get("document_name", ""),
        content=c.get("content", ""),
        score=float(c.get("score", 0)),
        chunk_index=int(c.get("chunk_index", 0)),
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
