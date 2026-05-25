"""gRPC client for RAG-MEMORY MemorySearchService (port 50054)."""
from typing import Any

import grpc
from loguru import logger

from common.config_loader import get_config
from communication.grpc_server.generated import memory_pb2, memory_pb2_grpc

_channel: grpc.Channel | None = None
_stub: memory_pb2_grpc.MemorySearchServiceStub | None = None


def _get_cfg() -> dict[str, Any]:
    return get_config()["clients"]["rag_memory"]


def _get_stub() -> memory_pb2_grpc.MemorySearchServiceStub:
    global _channel, _stub
    if _stub is not None:
        return _stub
    c = _get_cfg()
    _channel = grpc.insecure_channel(
        f"{c['host']}:{c['port']}",
        options=[("grpc.keepalive_time_ms", 30000)],
    )
    _stub = memory_pb2_grpc.MemorySearchServiceStub(_channel)
    logger.info(f"Memory search client connected: {c['host']}:{c['port']}")
    return _stub


def search_memory(
    user_id: int,
    query: str,
    top_k: int = 10,
    timeout: float | None = None,
) -> memory_pb2.MemorySearchResponse:
    """Search user memory via RAG-MEMORY."""
    if timeout is None:
        timeout = float(_get_cfg().get("timeout", 10))

    stub = _get_stub()
    request = memory_pb2.MemorySearchRequest(
        user_id=user_id,
        query=query,
        top_k=top_k,
    )
    try:
        return stub.Search(request, timeout=timeout)
    except grpc.RpcError as e:
        logger.error(f"Memory search gRPC call failed: {e.code()} - {e.details()}")
        raise


def get_working_memory(
    user_id: int,
    session_id: str,
    timeout: float | None = None,
) -> memory_pb2.MemorySearchResponse:
    """Get working memory entries for a user session."""
    if timeout is None:
        timeout = float(_get_cfg().get("timeout", 10))

    stub = _get_stub()
    request = memory_pb2.GetMemoryStatsRequest(user_id=user_id)
    try:
        return stub.GetWorkingMemory(request, timeout=timeout)
    except grpc.RpcError as e:
        logger.error(f"Working memory call failed: {e.code()} - {e.details()}")
        raise


def close() -> None:
    global _channel, _stub
    if _channel:
        _channel.close()
        _channel = None
        _stub = None
