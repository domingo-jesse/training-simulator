from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from loguru import logger

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()

# Human-friendly console logs for local development.
logger.add(
    sys.stderr,
    level="DEBUG",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{extra[module]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    backtrace=False,
    diagnose=False,
    enqueue=True,
)

# Main application log.
logger.add(
    LOG_DIR / "app.log",
    level="INFO",
    rotation="10 MB",
    retention="14 days",
    compression="zip",
    enqueue=True,
)

# Error-focused log.
logger.add(
    LOG_DIR / "errors.log",
    level="ERROR",
    rotation="5 MB",
    retention="30 days",
    compression="zip",
    enqueue=True,
)

# Structured machine-readable log.
logger.add(
    LOG_DIR / "structured.json",
    level="DEBUG",
    rotation="10 MB",
    retention="14 days",
    compression="zip",
    serialize=True,
    enqueue=True,
)


def get_logger(**context: Any):
    base_context = {"module": context.pop("module", "app")}
    base_context.update(context)
    return logger.bind(**base_context)
