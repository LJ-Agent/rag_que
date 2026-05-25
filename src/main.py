"""QUE Engine — main entry point.

Starts gRPC server on port 50055 (configurable via GRPC_QUE_PORT).
"""
import signal
import sys
import time
from concurrent import futures

import grpc

from common.config_loader import get_config
from common.logger import setup_logging, get_logger
from communication.grpc_server.generated import que_pb2_grpc
from communication.grpc_server.que_service import QueEngineService


def main():
    cfg = get_config()
    setup_logging(level=cfg["logging"]["level"], format=cfg["logging"]["format"])
    logger = get_logger()

    logger.info("=" * 60)
    logger.info("QUE Engine Starting...")
    logger.info("=" * 60)

    port = str(cfg["grpc"]["port"])
    max_workers = int(cfg["grpc"].get("max_workers", 10))

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ],
    )

    service = QueEngineService()
    que_pb2_grpc.add_QueEngineServiceServicer_to_server(service, server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"QUE Engine gRPC server started on 0.0.0.0:{port}")

    def _shutdown(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        service.shutdown()
        server.stop(grace=10)
        logger.info("Shutdown complete")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("QUE Engine is ready")

    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
