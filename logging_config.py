import logging
import os
import sys
from typing import Optional

from dotenv import load_dotenv

LEVEL_COLORS = {
    logging.DEBUG: "\033[36m",      # Cyan
    logging.INFO: "\033[32m",       # Green
    logging.WARNING: "\033[33m",    # Yellow
    logging.ERROR: "\033[31m",      # Red
    logging.CRITICAL: "\033[35m",   # Magenta
}

RESET_COLOR = "\033[0m"


class ColorFormatter(logging.Formatter):
    """Formatter that adds ANSI colors while keeping structured output."""

    def format(self, record: logging.LogRecord) -> str:
        color = LEVEL_COLORS.get(record.levelno, "")
        message = super().format(record)
        if color:
            message = f"{color}{message}{RESET_COLOR}"
        return message


def _resolve_log_level(default: int) -> int:
    level_name = os.getenv("DOUYIN_LOG_LEVEL") or os.getenv("LOG_LEVEL")
    if not level_name:
        return default
    converted = logging.getLevelName(level_name.upper())
    if isinstance(converted, int):
        return converted
    try:
        numeric = int(level_name)
        if numeric >= 0:
            return numeric
    except ValueError:
        pass
    logging.getLogger("douyin").warning(
        "Invalid log level '%s'; falling back to default %s",
        level_name,
        logging.getLevelName(default),
    )
    return default


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure application-wide logging once and return the root app logger."""
    root_logger = logging.getLogger()
    if not getattr(configure_logging, "_configured", False):
        load_dotenv()
        handler = logging.StreamHandler(sys.stdout)
        formatter = ColorFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        root_logger.setLevel(_resolve_log_level(level))
        configure_logging._configured = True
    return logging.getLogger("douyin")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Retrieve a namespaced logger underneath the root 'douyin' logger."""
    base_logger = configure_logging()
    if name:
        return base_logger.getChild(name)
    return base_logger
