import json
import os
from dataclasses import dataclass
from typing import Dict

from dotenv import load_dotenv


load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def _env_tables(default: Dict[str, str]) -> Dict[str, str]:
    raw = os.getenv("SUPABASE_TABLES")
    if not raw:
        return default
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    except json.JSONDecodeError:
        pass
    return default


@dataclass(frozen=True)
class MonitorConfig:
    supabase_url: str
    supabase_key: str
    supabase_schema: str
    supabase_tables: Dict[str, str]
    supabase_driver: str
    supabase_batch_size: int
    supabase_flush_interval: float
    supabase_queue_maxsize: int
    supabase_payload_column: str
    supabase_payload_fallback_column: str | None
    supabase_timestamp_column: str
    supabase_timestamp_fallback_column: str | None
    monitor_interval_minutes: int
    test_mode: bool
    anti_crawl_min_delay: float
    anti_crawl_max_delay: float

    @property
    def events_table(self) -> str:
        return self.supabase_tables.get("events", "douyin_events")

    @property
    def streamers_table(self) -> str:
        return self.supabase_tables.get("streamers", "douyin_streamers")

    @classmethod
    def load(cls, *, force_test_mode: bool | None = None) -> "MonitorConfig":
        test_mode = force_test_mode if force_test_mode is not None else _env_bool("FAST_MONITOR_TEST", False)
        default_interval = 1 if test_mode else 60
        interval = _env_int("MONITOR_INTERVAL_MINUTES", default_interval)
        if interval < 1:
            interval = default_interval

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in the environment.")

        supabase_schema = os.getenv("SUPABASE_SCHEMA", "public")
        supabase_driver = os.getenv("SUPABASE_DRIVER", "rest")
        supabase_tables = _env_tables({"events": "douyin_events", "streamers": "douyin_streamers"})
        supabase_batch_size = _env_int("SUPABASE_BATCH_SIZE", 50)
        supabase_flush_interval = _env_float("SUPABASE_FLUSH_INTERVAL", 1.0)
        supabase_queue_maxsize = _env_int("SUPABASE_QUEUE_MAXSIZE", 1000)
        supabase_payload_column = os.getenv("SUPABASE_EVENT_PAYLOAD_COLUMN", "data").strip() or "data"
        fallback_override = os.getenv("SUPABASE_EVENT_PAYLOAD_FALLBACK_COLUMN")
        if fallback_override is not None:
            fallback_override = fallback_override.strip()
            if fallback_override.lower() in {"", "none", "null"}:
                supabase_payload_fallback_column = None
            else:
                supabase_payload_fallback_column = fallback_override
        else:
            # Try the legacy column name if Supabase cached the previous schema.
            legacy_candidates = [
                candidate for candidate in ("payload", "event_payload") if candidate != supabase_payload_column
            ]
            supabase_payload_fallback_column = legacy_candidates[0] if legacy_candidates else None
        supabase_timestamp_column = os.getenv("SUPABASE_EVENT_TIMESTAMP_COLUMN", "ts").strip() or "ts"
        timestamp_fallback_override = os.getenv("SUPABASE_EVENT_TIMESTAMP_FALLBACK_COLUMN")
        if timestamp_fallback_override is not None:
            timestamp_fallback_override = timestamp_fallback_override.strip()
            if timestamp_fallback_override.lower() in {"", "none", "null"}:
                supabase_timestamp_fallback_column = None
            else:
                supabase_timestamp_fallback_column = timestamp_fallback_override
        else:
            # Default to the common schema mirror if Supabase is lagging.
            legacy_ts_candidates = [
                candidate for candidate in ("received_at", "created_at", "ts") if candidate != supabase_timestamp_column
            ]
            supabase_timestamp_fallback_column = legacy_ts_candidates[0] if legacy_ts_candidates else None

        min_delay = _env_float("ANTI_CRAWL_MIN_DELAY_SECONDS", 1.5 if not test_mode else 0.2)
        max_delay = _env_float("ANTI_CRAWL_MAX_DELAY_SECONDS", 4.0 if not test_mode else 0.8)

        if max_delay < min_delay:
            max_delay = min_delay

        return cls(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            supabase_schema=supabase_schema,
            supabase_tables=supabase_tables,
            supabase_driver=supabase_driver,
            supabase_batch_size=supabase_batch_size,
            supabase_flush_interval=supabase_flush_interval,
            supabase_queue_maxsize=supabase_queue_maxsize,
            supabase_payload_column=supabase_payload_column,
            supabase_payload_fallback_column=supabase_payload_fallback_column,
            supabase_timestamp_column=supabase_timestamp_column,
            supabase_timestamp_fallback_column=supabase_timestamp_fallback_column,
            monitor_interval_minutes=interval,
            test_mode=test_mode,
            anti_crawl_min_delay=min_delay,
            anti_crawl_max_delay=max_delay,
        )
