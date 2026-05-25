"""QUE Engine — main entry point.

Starts gRPC server on port 50055.
"""
import signal
import sys
import time
from concurrent import futures

import grpc

from common.config_loader import get_config
from common.logger import setup_logging, get_logger


def main():
    cfg = get_config()
    setup_logging(level=cfg["logging"]["level"], format=cfg["logging"]["format"])
    logger = get_logger()

    logger.info("=" * 60)
    logger.info("QUE Engine Starting...")
    logger.info("=" * 60)

    # TODO: Phase 4 — start gRPC server with QueEngineService
    logger.info("QUE Engine scaffold ready. gRPC service pending Phase 4.")


if __name__ == "__main__":
    main()
