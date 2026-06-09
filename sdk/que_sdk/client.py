"""QUE SDK Client — in-process, gRPC, or REST modes."""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueResult:
    """Result from QUE pipeline execution."""
    original_query: str = ""
    synthesized_context: str = ""
    intent: str = ""
    sub_queries_executed: int = 0
    total_latency_ms: float = 0.0
    rewritten_queries: list[str] = field(default_factory=list)
    trace_id: str = ""
    raw: Any = None


class QueClient:
    """Unified QUE client — works in-process, over gRPC, or over REST.

    In-process mode (default):
        Zero network overhead. Requires QUE Engine source in PYTHONPATH.

    gRPC mode:
        Requires: pip install grpcio
        Connects to a remote QUE gRPC server on port 50055.

    REST mode:
        Requires: pip install httpx
        Connects to the REST API on port 8080.
    """

    def __init__(
        self,
        mode: str = "inprocess",
        host: str = "localhost",
        port: int = 50055,
        base_url: str = "",
        timeout: int = 30,
    ):
        self._mode = mode
        self._host = host
        self._port = port
        self._base_url = base_url.rstrip("/") if base_url else f"http://{host}:8080"
        self._timeout = timeout
        self._initialized = False

    def execute(
        self,
        query: str,
        context: dict[str, str] | None = None,
        tenant_id: str = "default",
        enable_hyde: bool = True,
        enable_multi_query: bool = True,
        trace_id: str = "",
    ) -> QueResult:
        """Execute the QUE pipeline and return structured result."""
        ctx = context or {}

        if self._mode == "inprocess":
            return self._execute_inprocess(query, ctx, tenant_id, enable_hyde, enable_multi_query, trace_id)
        elif self._mode == "grpc":
            return self._execute_grpc(query, ctx, tenant_id, enable_hyde, enable_multi_query, trace_id)
        elif self._mode == "rest":
            return self._execute_rest(query, ctx, tenant_id, enable_hyde, enable_multi_query, trace_id)
        else:
            raise ValueError(f"Unknown mode: {self._mode}")

    def _execute_inprocess(self, query, context, tenant_id, enable_hyde, enable_multi_query, trace_id) -> QueResult:
        """Execute QUE pipeline in the current process."""
        import time
        from engine.intent_recognizer import recognize
        from engine.query_rewriter import rewrite
        from engine.query_planner import plan
        from engine.execution_router import route
        from engine.dag_executor import DAGExecutor
        from engine.result_synthesizer import synthesize
        from engine.models import ExecutionTrace
        from engine.backend_registry import get_registry
        from engine.builtin_backends import register_builtin_backends

        if not self._initialized:
            register_builtin_backends()
            self._initialized = True

        t0 = time.time()
        trace = ExecutionTrace()

        intent = recognize(query)
        rw = rewrite(query, enable_hyde=enable_hyde, enable_multi_query=enable_multi_query, intent=intent)
        dag = plan(query, intent, rw)
        for sq in dag.sub_queries:
            sq.route = route(sq, intent)
        results = DAGExecutor().execute(dag, context, trace, 30000)
        wq = rw.completed_query or rw.coreferenced_query or query
        ctx = synthesize(wq, intent, dag, results, trace)

        rewritten = []
        if rw.coreferenced_query and rw.coreferenced_query != query:
            rewritten.append(rw.coreferenced_query)
        rewritten.extend(rw.expanded_queries)

        return QueResult(
            original_query=query,
            synthesized_context=ctx,
            intent=intent.primary_intent,
            sub_queries_executed=dag.total_queries,
            total_latency_ms=(time.time() - t0) * 1000,
            rewritten_queries=rewritten,
            trace_id=trace_id,
        )

    def _execute_grpc(self, query, context, tenant_id, enable_hyde, enable_multi_query, trace_id) -> QueResult:
        """Execute QUE pipeline via gRPC."""
        import time
        try:
            import grpc
            # Dynamic import of generated stubs
            from communication.grpc_server.generated import que_pb2, que_pb2_grpc

            channel = grpc.insecure_channel(f"{self._host}:{self._port}")
            stub = que_pb2_grpc.QueEngineServiceStub(channel)

            t0 = time.time()
            request = que_pb2.QueRequest(
                query=query,
                context=context,
                enable_hyde=enable_hyde,
                enable_multi_query=enable_multi_query,
            )
            # Add tenant_id to context
            request.context["tenant_id"] = tenant_id

            response = stub.Execute(request, timeout=self._timeout)
            channel.close()

            return QueResult(
                original_query=query,
                synthesized_context=response.synthesized_context,
                intent=response.plan.primary_intent,
                sub_queries_executed=response.plan.total_queries,
                total_latency_ms=response.total_latency_ms,
                rewritten_queries=list(response.rewritten_queries),
                trace_id=trace_id,
                raw=response,
            )
        except ImportError:
            raise ImportError("grpc mode requires: pip install grpcio")

    def _execute_rest(self, query, context, tenant_id, enable_hyde, enable_multi_query, trace_id) -> QueResult:
        """Execute QUE pipeline via REST API."""
        try:
            import httpx

            t0 = __import__("time").time()
            resp = httpx.post(
                f"{self._base_url}/api/v1/execute",
                json={
                    "query": query,
                    "context": context,
                    "tenant_id": tenant_id,
                    "enable_hyde": enable_hyde,
                    "enable_multi_query": enable_multi_query,
                    "trace_id": trace_id,
                },
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            return QueResult(
                original_query=query,
                synthesized_context=data.get("synthesized_context", ""),
                intent=data.get("plan_summary", {}).get("primary_intent", ""),
                sub_queries_executed=data.get("plan_summary", {}).get("total_queries", 0),
                total_latency_ms=data.get("total_latency_ms", 0),
                rewritten_queries=data.get("rewritten_queries", []),
                trace_id=trace_id,
            )
        except ImportError:
            raise ImportError("rest mode requires: pip install httpx")


def health_check(mode: str = "inprocess", host: str = "localhost", port: int = 50055) -> dict:
    """Quick health check against QUE Engine."""
    if mode == "rest":
        try:
            import httpx
            resp = httpx.get(f"http://{host}:8080/api/v1/health", timeout=5)
            return resp.json()
        except ImportError:
            return {"error": "httpx not installed"}
    elif mode == "grpc":
        try:
            import grpc
            from communication.grpc_server.generated import que_pb2, que_pb2_grpc
            channel = grpc.insecure_channel(f"{host}:{port}")
            stub = que_pb2_grpc.QueEngineServiceStub(channel)
            resp = stub.HealthCheck(que_pb2.HealthCheckRequest(), timeout=5)
            channel.close()
            return {"healthy": resp.healthy, "backends": [
                {"name": b.backend_name, "status": b.status}
                for b in resp.backend_health
            ]}
        except ImportError:
            return {"error": "grpcio not installed"}
    else:
        try:
            from engine.backend_registry import get_registry
            from engine.builtin_backends import register_builtin_backends
            register_builtin_backends()
            registry = get_registry()
            return {"healthy": True, "backends": registry.health_summary()}
        except Exception as e:
            return {"healthy": False, "error": str(e)}
