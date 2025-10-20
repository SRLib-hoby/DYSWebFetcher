import json
import logging
import os
import sys
from typing import Any, Dict, Iterable, Optional

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
        summary = None
        message = record.getMessage()
        parsed = self._try_parse_json(message)
        if parsed is not None:
            summary = self._summarize_json(parsed)

        color = LEVEL_COLORS.get(record.levelno, "")
        formatted = super().format(record)
        if summary:
            formatted = f"{formatted}\n    summary: {summary}"
        if color:
            formatted = f"{color}{formatted}{RESET_COLOR}"
        return formatted

    @staticmethod
    def _try_parse_json(message: Any) -> Optional[Any]:
        if not isinstance(message, str):
            return None
        text = message.strip()
        if not text or text[0] not in ("{", "["):
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _summarize_json(data: Any) -> str:
        if isinstance(data, list):
            if not data:
                return "[]"
            if all(isinstance(item, dict) for item in data):
                return f"array[{len(data)}] first={ColorFormatter._summarize_dict(data[0])}"
            return f"array[{len(data)}] sample={ColorFormatter._shorten_value(data[0])}"
        if isinstance(data, dict):
            return ColorFormatter._summarize_dict(data)
        return ColorFormatter._shorten_value(data)

    @staticmethod
    def _summarize_dict(item: Dict[str, Any]) -> str:
        prioritized_keys: Iterable[str] = (
            "event_type",
            "status",
            "code",
            "message",
            "live_id",
            "room_id",
            "room_status",
            "anchor_name",
            "ts",
        )
        parts = []
        for key in prioritized_keys:
            if key in item:
                parts.append(f"{key}={ColorFormatter._shorten_value(item[key])}")
        payload = item.get("payload") or item.get("event_payload")
        if isinstance(payload, dict):
            payload_keys = ("type", "user_id", "nickname", "gift_name", "value", "count")
            payload_parts = [
                f"{key}={ColorFormatter._shorten_value(payload[key])}"
                for key in payload_keys
                if key in payload
            ]
            if payload_parts:
                parts.append(f"payload[{', '.join(payload_parts)}]")
        if not parts:
            for key, value in item.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    parts.append(f"{key}={ColorFormatter._shorten_value(value)}")
                elif isinstance(value, dict) and value:
                    parts.append(f"{key}={{...}}")
                elif isinstance(value, list) and value:
                    parts.append(f"{key}=[{len(value)}]")
                if len(parts) >= 4:
                    break
        return ", ".join(parts) if parts else "{}"

    @staticmethod
    def _shorten_value(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        text = str(value)
        if len(text) > 60:
            return f"{text[:57]}..."
        return text


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
