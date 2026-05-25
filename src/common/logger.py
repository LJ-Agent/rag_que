"""Logging setup via loguru."""
import sys
from loguru import logger


def setup_logging(level: str = "INFO", format: str | None = None) -> None:
    logger.remove()
    fmt = format or (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
        "{name}:{function}:{line} | {message}"
    )
    logger.add(sys.stdout, level=level, format=fmt, colorize=True)
    logger.add(
        "logs/que.log", level="DEBUG", format=fmt,
        rotation="100 MB", retention="30 days", compression="gz", encoding="utf-8",
    )


def get_logger():
    return logger
