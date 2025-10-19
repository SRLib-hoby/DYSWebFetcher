from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Dict, List

import requests

from config import MonitorConfig
from logging_config import get_logger
from liveMan import ChatMessage, ControlMessage, EmojiChatMessage, FansclubMessage, GiftMessage, LikeMessage, MemberMessage, RoomMessage, RoomRankMessage, RoomStatsMessage, RoomStreamAdaptationMessage, RoomUserSeqMessage


class SupabaseService:
    """REST-based Supabase client with buffered writes to reduce rate-limit exposure."""

    def __init__(self, config: MonitorConfig):
        self.config = config
        self.logger = get_logger("supabase")
        self.base_rest_url = f"{config.supabase_url.rstrip('/')}/rest/v1"
        self.streamers_table = config.streamers_table
        self.events_table = config.events_table
        self.payload_column = config.supabase_payload_column
        self._configured_payload_column = config.supabase_payload_column
        self._payload_column_warning_emitted = False
        self.payload_fallback_column = config.supabase_payload_fallback_column
        self.timestamp_column = config.supabase_timestamp_column
        self._configured_timestamp_column = config.supabase_timestamp_column
        self._timestamp_column_warning_emitted = False
        self.timestamp_fallback_column = config.supabase_timestamp_fallback_column
        self._timestamp_parse_warning_emitted = False
        self.session = requests.Session()
        self._last_schema_reload_at = 0.0
        self._schema_reload_supported = True
        self.logger.info(
            "Supabase target table=%s.%s | payload column=%s | payload fallback=%s | timestamp column=%s | timestamp fallback=%s",
            config.supabase_schema,
            self.events_table,
            self.payload_column,
            self.payload_fallback_column or "disabled",
            self.timestamp_column,
            self.timestamp_fallback_column or "disabled",
        )
        self.read_headers = {
            "apikey": config.supabase_key,
            "Authorization": f"Bearer {config.supabase_key}",
            "Accept": "application/json",
            "Accept-Profile": config.supabase_schema,
        }
        self.write_headers = {
            "apikey": config.supabase_key,
            "Authorization": f"Bearer {config.supabase_key}",
            "Content-Type": "application/json",
            "Content-Profile": config.supabase_schema,
            "Prefer": "return=minimal",
        }

        self._queue: Queue[Dict[str, Any]] = Queue(maxsize=config.supabase_queue_maxsize)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._run_worker, daemon=True)
        self._worker.start()

        if config.supabase_driver.lower() != "rest":
            self.logger.warning(
                "SUPABASE_DRIVER=%s is not fully supported. Falling back to REST client.",
                config.supabase_driver,
            )

    def close(self) -> None:
        """Flush pending events and stop the background worker."""
        self._stop_event.set()
        self._worker.join(timeout=max(5.0, self.config.supabase_flush_interval * 2))
        if self._worker.is_alive():
            self.logger.warning("Supabase worker thread did not exit cleanly; forcing flush.")
        self._drain_queue()
        self.session.close()

    def fetch_streamers(self) -> List[Dict[str, Any]]:
        """Return streamers that should be monitored (audit_realtime == True)."""
        params = {
            "select": "*",
            "audit_realtime": "eq.true",
        }
        url = f"{self.base_rest_url}/{self.streamers_table}"
        try:
            response = self.session.get(url, headers=self.read_headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                self.logger.debug("Fetched %s streamers for monitoring", len(data))
                return data
            self.logger.warning("Unexpected streamers payload: %s", data)
            return []
        except Exception:
            self.logger.exception("Failed to fetch streamers from Supabase")
            return []

    def insert_event(self, event: Dict[str, Any]) -> None:
        """Queue a crawler event for batched insertion into Supabase."""
        try:
            self._queue.put(event, timeout=1)
        except Full:
            self.logger.error("Supabase event queue is full; dropping event: %s", event.get("event_type"))

    # Internal helpers -----------------------------------------------------

    def _run_worker(self) -> None:
        batch: List[Dict[str, Any]] = []
        last_flush = time.time()
        flush_interval = self.config.supabase_flush_interval
        batch_size = self.config.supabase_batch_size

        while not self._stop_event.is_set() or not self._queue.empty() or batch:
            timeout = max(0.1, flush_interval - (time.time() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
                if len(batch) >= batch_size:
                    self._flush(batch)
                    batch.clear()
                    last_flush = time.time()
            except Empty:
                if batch:
                    self._flush(batch)
                    batch.clear()
                    last_flush = time.time()
                continue
            except Exception:
                self.logger.exception("Unexpected error in Supabase worker loop")

        if batch:
            self._flush(batch)

    def _drain_queue(self) -> None:
        remaining: List[Dict[str, Any]] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except Empty:
                break
        if remaining:
            self._flush(remaining)

    def _flush(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return
        url = f"{self.base_rest_url}/{self.events_table}"
        attempt = 0
        max_attempts = 3
        try:
            while attempt < max_attempts:
                sanitized_batch = [self._sanitize_record(record) for record in batch]
                preview = json.dumps(sanitized_batch[:3], ensure_ascii=False)[:2000]
                if attempt == 0:
                    self.logger.info("Flushing %s events | preview=%s", len(sanitized_batch), preview)
                else:
                    self.logger.info(
                        "Retrying flush with payload column '%s' and timestamp column '%s' | events=%s",
                        self.payload_column,
                        self.timestamp_column,
                        len(sanitized_batch),
                    )

                response = self.session.post(url, headers=self.write_headers, json=sanitized_batch, timeout=10)
                if response.status_code in {200, 201, 204}:
                    self.logger.debug("Inserted %s events into Supabase", len(batch))
                    return

                if attempt == 0 and self._should_retry_with_payload_fallback(response):
                    self.logger.warning(
                        "Supabase rejected payload column '%s' | status=%s body=%s",
                        self.payload_column,
                        response.status_code,
                        response.text,
                    )
                    fallback_column = self._payload_fallback_column()
                    if fallback_column:
                        if not self._payload_column_warning_emitted:
                            self.logger.warning(
                                "Supabase column '%s' is missing; falling back to '%s'",
                                self.payload_column,
                                fallback_column,
                            )
                            self._payload_column_warning_emitted = True
                        self.payload_column = fallback_column
                        attempt += 1
                        continue

                if attempt == 0 and self._should_retry_with_timestamp_fallback(response):
                    self.logger.warning(
                        "Supabase rejected timestamp column '%s' | status=%s body=%s",
                        self.timestamp_column,
                        response.status_code,
                        response.text,
                    )
                    timestamp_fallback = self._timestamp_fallback_column()
                    if timestamp_fallback:
                        if not self._timestamp_column_warning_emitted:
                            self.logger.warning(
                                "Supabase column '%s' is missing; falling back to '%s'",
                                self.timestamp_column,
                                timestamp_fallback,
                            )
                            self._timestamp_column_warning_emitted = True
                        self.timestamp_column = timestamp_fallback
                        attempt += 1
                        continue

                if self._maybe_reload_schema_cache(response):
                    attempt += 1
                    time.sleep(0.5)
                    continue

                self.logger.error(
                    "Failed to insert events (column='%s') | status=%s body=%s",
                    self.payload_column,
                    response.status_code,
                    response.text,
                )
                self.logger.error("Request payload sent to Supabase: %s", json.dumps(sanitized_batch, ensure_ascii=False))
                return

            self.logger.error("Exhausted Supabase flush retries for %s events", len(batch))
        except Exception:
            self.logger.exception("Failed to flush %s events to Supabase", len(batch))
        finally:
            self.payload_column = self._configured_payload_column
            self.timestamp_column = self._configured_timestamp_column

    def _sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}
        for key, value in record.items():
            if key == "event_payload":
                target_key = self.payload_column
            elif key in {"received_at", "ts"}:
                target_key = self.timestamp_column
            else:
                target_key = str(key)

            if target_key == self.timestamp_column:
                sanitized[target_key] = self._coerce_timestamp_value(value)
            else:
                sanitized[target_key] = self._sanitize_value(value)

        if self.payload_column in sanitized and sanitized[self.payload_column] is None:
            sanitized[self.payload_column] = {}

        return sanitized

    def _sanitize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return "".join(ch for ch in value if ch >= " " or ch in "\r\n\t")
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): self._sanitize_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._sanitize_value(item) for item in value]
        if hasattr(value, "to_dict"):
            return self._sanitize_value(value.to_dict())
        return str(value)

    def _coerce_timestamp_value(self, value: Any) -> Any:
        now = datetime.now(timezone.utc)
        if value is None:
            return self._default_timestamp_value(now)

        if isinstance(value, datetime):
            dt = value.astimezone(timezone.utc)
        elif isinstance(value, (int, float)):
            dt = self._epoch_to_datetime(float(value))
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return self._default_timestamp_value(now)
            if text.isdigit():
                dt = self._epoch_to_datetime(float(text))
            else:
                try:
                    normalised = text[:-1] + "+00:00" if text.endswith("Z") else text
                    dt = datetime.fromisoformat(normalised)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                except ValueError:
                    if not self._timestamp_parse_warning_emitted:
                        self.logger.warning("Failed to parse timestamp value '%s'; defaulting to current time.", value)
                        self._timestamp_parse_warning_emitted = True
                    dt = now
        else:
            if not self._timestamp_parse_warning_emitted:
                self.logger.warning(
                    "Unsupported timestamp value type %s; defaulting to current time.",
                    type(value).__name__,
                )
                self._timestamp_parse_warning_emitted = True
            dt = now

        if self.timestamp_column == "ts":
            return int(dt.timestamp() * 1000)
        return dt.isoformat()

    def _default_timestamp_value(self, reference: datetime | None = None) -> Any:
        reference = reference or datetime.now(timezone.utc)
        if self.timestamp_column == "ts":
            return int(reference.timestamp() * 1000)
        return reference.isoformat()

    def _epoch_to_datetime(self, value: float) -> datetime:
        # Values above ~1e12 indicate millisecond precision.
        if value > 1e12:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)

    def _should_retry_with_payload_fallback(self, response: requests.Response) -> bool:
        if not self.payload_fallback_column:
            return False
        if response.status_code != 400:
            return False
        message = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                message = str(body.get("message") or "")
        except ValueError:
            message = ""
        if not message:
            message = response.text or ""
        needle = f"'{self.payload_column}'"
        return "Could not find" in message and needle in message

    def _payload_fallback_column(self) -> str | None:
        fallback = self.payload_fallback_column
        if not fallback or self.payload_column == fallback:
            return None
        return fallback

    def _should_retry_with_timestamp_fallback(self, response: requests.Response) -> bool:
        if not self.timestamp_fallback_column:
            return False
        if response.status_code != 400:
            return False
        message = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                message = str(body.get("message") or "")
        except ValueError:
            message = ""
        if not message:
            message = response.text or ""
        needle = f"'{self.timestamp_column}'"
        return "Could not find" in message and needle in message

    def _timestamp_fallback_column(self) -> str | None:
        fallback = self.timestamp_fallback_column
        if not fallback or self.timestamp_column == fallback:
            return None
        return fallback

    def _maybe_reload_schema_cache(self, response: requests.Response) -> bool:
        if response.status_code != 400:
            return False
        message = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                message = str(body.get("message") or "")
        except ValueError:
            message = response.text or ""
        message_lower = (message or "").lower()
        if "schema cache" not in message_lower:
            return False
        now = time.time()
        if now - self._last_schema_reload_at < 30:
            self.logger.debug("Schema reload recently triggered; skipping duplicate request.")
            return False
        if not self._schema_reload_supported:
            return False
        try:
            self.logger.warning("Requesting Supabase schema cache reload via pg_notify")
            notify_url = f"{self.base_rest_url}/rpc/pg_notify"
            payload = {"channel": "pgrst", "payload": "reload schema"}
            notify_response = self.session.post(notify_url, headers=self.write_headers, json=payload, timeout=5)
            if notify_response.status_code not in {200, 204}:
                if notify_response.status_code == 404:
                    self._schema_reload_supported = False
                    self.logger.warning(
                        "Schema reload request failed (pg_notify unavailable). Please run "
                        "\"SELECT pg_notify('pgrst','reload schema');\" in Supabase SQL editor to refresh manually."
                    )
                else:
                    self.logger.warning(
                        "Schema reload request failed | status=%s body=%s",
                        notify_response.status_code,
                        notify_response.text,
                    )
                return False
            self._last_schema_reload_at = now
            self.logger.info("Schema reload signal dispatched; will retry insert shortly.")
            return True
        except Exception:
            self.logger.exception("Failed to dispatch schema reload signal")
            self._schema_reload_supported = False
            return False
