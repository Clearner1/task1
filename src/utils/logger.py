"""
Logger configuration — structured logging with loguru.
"""

import os
import sys

from loguru import logger


def setup_logger(log_dir: str = "logs", level: str = "INFO"):
    """Configure loguru with console + file output."""
    os.makedirs(log_dir, exist_ok=True)

    # Remove default handler
    logger.remove()

    # Console: concise, colored
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <7}</level> | "
            "<cyan>{message}</cyan>"
        ),
        colorize=True,
    )

    # File: detailed, rotated
    logger.add(
        os.path.join(log_dir, "crawler_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <7} | {name}:{function}:{line} | {message}",
        rotation="50 MB",
        retention="7 days",
        encoding="utf-8",
    )

    return logger
