"""gRPC client for RAG-PYTHON RetrievalService (port 50051)."""
from typing import Any

import grpc
from loguru import logger

from common.config_loader import get_config
from communication.grpc_server.generated import retrieval_pb2, retrieval_pb2_grpc

_channel: grpc.Channel | None = None
_stub: retrieval_pb2_grpc.RetrievalServiceStub | None = None


def _get_cfg() -> dict[str, Any]:
    return get_config()["clients"]["rag_python"]


def _get_stub() -> retrieval_pb2_grpc.RetrievalServiceStub:
    global _channel, _stub
    if _stub is not None:
        return _stub
    c = _get_cfg()
    _channel = grpc.insecure_channel(
        f"{c['host']}:{c['port']}",
        options=[("grpc.keepalive_time_ms", 30000)],
    )
    _stub = retrieval_pb2_grpc.RetrievalServiceStub(_channel)
    logger.info(f"RAG retrieval client connected: {c['host']}:{c['port']}")
    return _stub


def retrieve(
    query: str,
    kb_ids: list[int],
    top_k: int = 10,
    score_threshold: float = 0.0,
    timeout: float | None = None,
) -> retrieval_pb2.RetrievalResponse:
    """Call RAG-PYTHON retrieval service."""
    if timeout is None:
        timeout = float(_get_cfg().get("timeout", 30))

    stub = _get_stub()
    request = retrieval_pb2.RetrievalRequest(
        query=query,
        kb_ids=kb_ids,
        top_k=top_k,
        score_threshold=score_threshold,
    )
    try:
        return stub.Retrieve(request, timeout=timeout)
    except grpc.RpcError as e:
        logger.error(f"Retrieval gRPC call failed: {e.code()} - {e.details()}")
        raise


def close() -> None:
    global _channel, _stub
    if _channel:
        _channel.close()
        _channel = None
        _stub = None
