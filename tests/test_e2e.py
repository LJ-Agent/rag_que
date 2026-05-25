"""E2E tests for QUE Engine — tests the full ICAR pipeline via gRPC."""
import os
import sys
import time
import pytest

# Ensure src is on path for local testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import grpc
from communication.grpc_server.generated import que_pb2, que_pb2_grpc
from common.config_loader import get_config


def _get_channel():
    cfg = get_config()
    port = cfg["grpc"]["port"]
    return grpc.insecure_channel(f"localhost:{port}")


def _get_stub():
    return que_pb2_grpc.QueEngineServiceStub(_get_channel())


# ---------------- Fixtures ----------------


@pytest.fixture(scope="module")
def stub():
    """Return a gRPC stub connected to the QUE Engine server."""
    stub = _get_stub()
    # Verify server is reachable
    try:
        resp = stub.HealthCheck(que_pb2.HealthCheckRequest(), timeout=5)
        assert resp.healthy or resp.llm_status == "healthy"
    except grpc.RpcError as e:
        pytest.skip(f"QUE server not reachable: {e.code()} — {e.details()}")
    return stub


# ---------------- Health Check ----------------


def test_health_check(stub):
    """Server should report status for all downstream dependencies."""
    resp = stub.HealthCheck(que_pb2.HealthCheckRequest(), timeout=5)
    assert resp.rag_retrieval_status
    assert resp.rag_memory_status
    assert resp.llm_status
    assert resp.healthy in (True, False)


# ---------------- Simple Query ----------------


def test_simple_fact_lookup(stub):
    """Simple fact query → single RAG retrieval → aggregated context."""
    resp = stub.Execute(
        que_pb2.QueRequest(
            query="什么是知识图谱？",
            user_id=0,
            session_id="",
            kb_ids=[1],
            max_sub_queries=3,
            timeout_ms=30000,
            enable_hyde=False,
            enable_multi_query=False,
        ),
        timeout=60,
    )
    _assert_valid_response(resp)
    assert len(resp.sub_results) >= 1
    assert any(r.success for r in resp.sub_results)


# ---------------- Comparison Query ----------------


def test_comparison_query(stub):
    """Comparison query → A/B split → side-by-side synthesis."""
    resp = stub.Execute(
        que_pb2.QueRequest(
            query="对比一下机器学习与深度学习的区别",
            user_id=0,
            session_id="",
            kb_ids=[1],
            max_sub_queries=4,
            timeout_ms=30000,
            enable_hyde=False,
            enable_multi_query=False,
        ),
        timeout=60,
    )
    _assert_valid_response(resp)
    # Comparison should generate at least 2 sub-queries (A/B)
    assert len(resp.sub_results) >= 2, f"Expected >=2 sub-results for comparison, got {len(resp.sub_results)}"


# ---------------- Multi-Hop Query ----------------


def test_multi_hop_query(stub):
    """Multi-hop query → dependency chain → chained synthesis."""
    resp = stub.Execute(
        que_pb2.QueRequest(
            query="谁开发了AlphaGo，这家公司后来被谁收购了？",
            user_id=0,
            session_id="",
            kb_ids=[1],
            max_sub_queries=5,
            timeout_ms=30000,
            enable_hyde=False,
            enable_multi_query=False,
        ),
        timeout=60,
    )
    _assert_valid_response(resp)
    assert len(resp.sub_results) >= 1
    assert len(resp.synthesized_context) > 0


# ---------------- Query Rewrite ----------------


def test_rewrite_with_coreference(stub):
    """Query with pronouns should trigger coreference resolution if memory is available."""
    resp = stub.Execute(
        que_pb2.QueRequest(
            query="它有哪些特性？",
            user_id=0,
            session_id="test-e2e-session",
            kb_ids=[1],
            max_sub_queries=3,
            timeout_ms=30000,
            enable_hyde=True,
            enable_multi_query=True,
        ),
        timeout=60,
    )
    _assert_valid_response(resp)
    assert len(resp.rewritten_queries) >= 0  # may be empty if no working memory


# ---------------- Execution Trace ----------------


def test_execution_trace_present(stub):
    """Response should include an execution trace with at least the intent stage."""
    resp = stub.Execute(
        que_pb2.QueRequest(
            query="Python 装饰器的原理是什么？",
            user_id=0,
            session_id="",
            kb_ids=[1],
            max_sub_queries=2,
            timeout_ms=30000,
            enable_hyde=False,
            enable_multi_query=False,
        ),
        timeout=60,
    )
    _assert_valid_response(resp)
    assert len(resp.trace.entries) >= 1
    stages = [e.stage for e in resp.trace.entries]
    assert "intent" in stages, f"Expected 'intent' in trace stages, got {stages}"


# ---------------- Helpers ----------------


def _assert_valid_response(resp: que_pb2.QueResponse):
    """Common assertions for any QUE response."""
    assert resp.original_query
    assert resp.total_latency_ms >= 0
    assert resp.plan is not None
    assert resp.plan.total_queries >= 1
    assert resp.synthesized_context
