"""QUE SDK — Python client for QUE Engine.

Usage (in-process):
    from que_sdk import QueClient
    client = QueClient()
    result = client.execute("What is RAG?")

Usage (remote gRPC):
    from que_sdk import QueClient
    client = QueClient(mode="grpc", host="localhost", port=50055)
    result = client.execute("What is RAG?")

Usage (remote REST):
    from que_sdk import QueClient
    client = QueClient(mode="rest", base_url="http://localhost:8080")
    result = client.execute("What is RAG?")
"""
from que_sdk.client import QueClient, QueResult

__version__ = "2.0.0"
__all__ = ["QueClient", "QueResult"]
