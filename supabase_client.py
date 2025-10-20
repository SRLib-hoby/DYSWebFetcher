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
        """Configure Supabase REST endpoints, headers, and start the worker thread."""
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
        """Continuously batch events from the queue and trigger flushes on size or interval."""
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
        """Empty the queue synchronously to ensure pending events are written during shutdown."""
        remaining: List[Dict[str, Any]] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except Empty:
                break
        if remaining:
            self._flush(remaining)

    def _flush(self, batch: List[Dict[str, Any]]) -> None:
        """Send a batch of events to Supabase with column fallback and schema reload retries."""
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
        """Prepare a single event for Supabase by normalising keys and coercing values."""
        sanitized: Dict[str, Any] = {}
        event_type = record.get("event_type")
        payload = record.get("event_payload")
        msg_summary = self._extract_event_message(event_type, payload)
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

        if "msg" in record:
            sanitized["msg"] = self._sanitize_value(record["msg"])
        else:
            sanitized["msg"] = self._sanitize_value(msg_summary) if msg_summary is not None else None

        return sanitized

    def _sanitize_value(self, value: Any) -> Any:
        """Recursively clean individual values to ensure they are JSON serialisable."""
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

    def _extract_event_message(self, event_type: str | None, payload: Any) -> Any:
        """Create event-type-specific summary for the msg column."""
        if not event_type:
            return None

        payload_dict = payload if isinstance(payload, dict) else None

        summary: Any = None
        if event_type == "chat":
            summary = self._summarize_chat_event(payload_dict)
        elif event_type == "gift":
            summary = self._summarize_gift_event(payload_dict)
        elif event_type == "like":
            summary = self._summarize_like_event(payload_dict)
        elif event_type == "member":
            summary = self._summarize_member_event(payload_dict)
        elif event_type == "social":
            summary = self._summarize_social_event(payload_dict)
        elif event_type == "room_user_seq":
            summary = self._summarize_room_user_seq_event(payload_dict)
        elif event_type == "fansclub":
            summary = self._summarize_fansclub_event(payload_dict)
        elif event_type == "emoji_chat":
            summary = self._summarize_emoji_chat_event(payload_dict)
        elif event_type == "room_info":
            summary = self._summarize_room_info_event(payload_dict)
        elif event_type == "room_stats":
            summary = self._summarize_room_stats_event(payload_dict)
        elif event_type == "room_rank":
            summary = self._summarize_room_rank_event(payload_dict)
        elif event_type == "control":
            summary = self._summarize_control_event(payload_dict)
        elif event_type == "stream_adaptation":
            summary = self._summarize_stream_adaptation_event(payload_dict)
        elif event_type == "room_status":
            summary = self._summarize_room_status_event(payload_dict)
        elif event_type in {"connection_attempt", "connection_open"}:
            summary = self._compact_dict({"live_id": self._get_field(payload_dict, "live_id")})
        elif event_type == "connection_error":
            summary = self._compact_dict(
                {
                    "live_id": self._get_field(payload_dict, "live_id"),
                    "error": self._get_field(payload_dict, "error"),
                }
            )
        elif event_type == "connection_closed":
            summary = self._compact_dict(
                {
                    "live_id": self._get_field(payload_dict, "live_id"),
                    "args": self._get_field(payload_dict, "args"),
                }
            )

        if isinstance(summary, dict) and payload_dict:
            for meta_key in ("streamer_id", "source_nickname"):
                meta_value = self._get_field(payload_dict, meta_key)
                if meta_value is not None and meta_key not in summary:
                    summary[meta_key] = meta_value

        return summary

    def _get_field(self, source: Dict[str, Any] | None, key: str, *alternates: str) -> Any:
        """Return the first matching field accounting for snake/camel case variants."""
        if not isinstance(source, dict):
            return None
        candidates = (key,) + alternates
        for candidate in candidates:
            if candidate in source:
                return source[candidate]
            for variant in self._key_variants(candidate):
                if variant in source:
                    return source[variant]
        return None

    def _key_variants(self, key: str) -> List[str]:
        """Generate common casing variants for a dictionary key."""
        variants = []
        if "_" in key:
            camel = self._snake_to_camel(key)
            variants.append(camel)
            variants.append(camel[0].upper() + camel[1:])
            variants.append(key.replace("_", ""))
        else:
            snake = self._camel_to_snake(key)
            if snake != key:
                variants.append(snake)
        return variants

    def _snake_to_camel(self, value: str) -> str:
        parts = value.split("_")
        if not parts:
            return value
        return parts[0] + "".join(part.title() for part in parts[1:])

    def _camel_to_snake(self, value: str) -> str:
        if not value:
            return value
        chars: List[str] = []
        for ch in value:
            if ch.isupper():
                if chars:
                    chars.append("_")
                chars.append(ch.lower())
            else:
                chars.append(ch)
        return "".join(chars)

    def _summarize_chat_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key chat message fields."""
        if not payload:
            return None
        content = self._get_field(payload, "content") or self._extract_text_value(
            self._get_field(payload, "rtf_content")
        )
        summary = {
            "content": content,
            "event_time": self._get_field(payload, "event_time"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_gift_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key gift message fields."""
        if not payload:
            return None
        gift = self._get_field(payload, "gift")
        gift_dict = gift if isinstance(gift, dict) else {}
        summary = {
            "gift_name": self._get_field(gift_dict, "name"),
            "gift_desc": self._get_field(gift_dict, "describe", "description"),
            "combo_count": self._get_field(payload, "combo_count"),
            "repeat_count": self._get_field(payload, "repeat_count"),
            "diamond_count": self._get_field(gift_dict, "diamond_count") or self._get_field(payload, "diamond_count"),
            "fan_ticket_count": self._get_field(payload, "fan_ticket_count"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        to_user = self._summarize_user(self._get_field(payload, "to_user", "toUser"))
        if to_user:
            summary["to_user"] = to_user
        return self._compact_dict(summary)

    def _summarize_like_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key like message fields."""
        if not payload:
            return None
        summary = {
            "count": self._get_field(payload, "count"),
            "total": self._get_field(payload, "total"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_member_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key member join/leave fields."""
        if not payload:
            return None
        summary = {
            "action": self._get_field(payload, "action_description", "action"),
            "member_count": self._get_field(payload, "member_count"),
            "rank_score": self._get_field(payload, "rank_score"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_social_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key follow/share social event fields."""
        if not payload:
            return None
        summary = {
            "action": self._get_field(payload, "action"),
            "share_type": self._get_field(payload, "share_type"),
            "share_target": self._get_field(payload, "share_target"),
            "follow_count": self._get_field(payload, "follow_count"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_room_user_seq_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key room user sequence metrics."""
        if not payload:
            return None
        summary = {
            "total": self._get_field(payload, "total"),
            "total_pv_for_anchor": self._get_field(payload, "total_pv_for_anchor"),
            "popularity": self._get_field(payload, "popularity"),
            "total_user": self._get_field(payload, "total_user"),
            "online_user_for_anchor": self._get_field(payload, "online_user_for_anchor"),
        }
        return self._compact_dict(summary)

    def _summarize_fansclub_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key fansclub message fields."""
        if not payload:
            return None
        summary = {
            "type": self._get_field(payload, "type"),
            "content": self._get_field(payload, "content"),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_emoji_chat_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key emoji chat fields."""
        if not payload:
            return None
        summary = {
            "emoji_id": self._get_field(payload, "emoji_id"),
            "content": self._get_field(payload, "default_content")
            or self._extract_text_value(self._get_field(payload, "emoji_content")),
        }
        user_summary = self._summarize_user(self._get_field(payload, "user"))
        if user_summary:
            summary["user"] = user_summary
        return self._compact_dict(summary)

    def _summarize_room_info_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key room info message fields."""
        if not payload:
            return None
        summary = {
            "content": self._get_field(payload, "content"),
            "message_type": self._get_field(payload, "roommessagetype", "room_message_type"),
            "biz_scene": self._get_field(payload, "biz_scene"),
            "system_top_msg": self._get_field(payload, "system_top_msg"),
        }
        return self._compact_dict(summary)

    def _summarize_room_stats_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract key room stats values."""
        if not payload:
            return None
        summary = {
            "display": self._get_field(payload, "display_long")
            or self._get_field(payload, "display_middle")
            or self._get_field(payload, "display_short"),
            "display_value": self._get_field(payload, "display_value"),
            "total": self._get_field(payload, "total"),
            "incremental": self._get_field(payload, "incremental"),
        }
        return self._compact_dict(summary)

    def _summarize_room_rank_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract top ranks summary."""
        if not payload:
            return None
        ranks = self._get_field(payload, "ranks_list", "ranks")
        summarized_ranks: List[Dict[str, Any]] = []
        if isinstance(ranks, list):
            for item in ranks[:3]:
                if not isinstance(item, dict):
                    continue
                rank_summary = {
                    "score": self._get_field(item, "score_str", "score"),
                    "profile_hidden": self._get_field(item, "profile_hidden"),
                }
                user_summary = self._summarize_user(self._get_field(item, "user"))
                if user_summary:
                    rank_summary["user"] = user_summary
                summarized = self._compact_dict(rank_summary)
                if summarized:
                    summarized_ranks.append(summarized)
        summary = {"ranks": summarized_ranks}
        return self._compact_dict(summary)

    def _summarize_control_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract room control status updates."""
        if not payload:
            return None
        summary = {"status": self._get_field(payload, "status")}
        return self._compact_dict(summary)

    def _summarize_stream_adaptation_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract stream adaptation parameters."""
        if not payload:
            return None
        summary = {
            "adaptation_type": self._get_field(payload, "adaptation_type"),
            "height_ratio": self._get_field(payload, "adaptation_height_ratio"),
            "body_center_ratio": self._get_field(payload, "adaptation_body_center_ratio"),
        }
        return self._compact_dict(summary)

    def _summarize_room_status_event(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Extract room status milestone data."""
        if not payload:
            return None
        summary = {
            "room_status": self._get_field(payload, "room_status"),
            "nickname": self._get_field(payload, "nickname"),
            "user_id": self._get_field(payload, "user_id"),
        }
        return self._compact_dict(summary)

    def _summarize_user(self, user: Any) -> Dict[str, Any] | None:
        """Return a compact representation of a user dictionary."""
        if not isinstance(user, dict):
            return None
        summary = {
            "id": self._stringify_id(
                self._get_field(user, "id", "id_str")
            ),
            "sec_uid": self._get_field(user, "sec_uid"),
            "display_id": self._get_field(user, "display_id"),
            "short_id": self._stringify_id(self._get_field(user, "short_id")),
            "nickname": self._get_field(user, "nick_name", "nickname"),
        }
        pay_grade = self._get_field(user, "pay_grade")
        if isinstance(pay_grade, dict):
            level = self._get_field(pay_grade, "level")
            if level is not None:
                summary["level"] = level
        fans_club = self._get_field(user, "fans_club")
        if isinstance(fans_club, dict):
            data = self._get_field(fans_club, "data")
            if isinstance(data, dict):
                level = self._get_field(data, "level")
                if level is not None:
                    summary["fans_club_level"] = level
        return self._compact_dict(summary)

    def _extract_text_value(self, text_payload: Any) -> str | None:
        """Return a human readable string from a Text protobuf dictionary."""
        if isinstance(text_payload, str):
            return text_payload or None
        if not isinstance(text_payload, dict):
            return None

        value = self._get_field(text_payload, "default_pattern", "default_content")
        if value:
            return value
        pieces = self._get_field(text_payload, "pieces_list", "pieces")
        if isinstance(pieces, list):
            parts: List[str] = []
            for item in pieces:
                if not isinstance(item, dict):
                    continue
                string_value = self._get_field(item, "string_value", "stringValue")
                if string_value:
                    parts.append(str(string_value))
            if parts:
                return "".join(parts)
        return None

    def _compact_dict(self, data: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Remove null or empty entries from a dictionary while preserving zero values."""
        if not isinstance(data, dict):
            return None
        compacted: Dict[str, Any] = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, (dict, list, tuple)) and not value:
                continue
            compacted[key] = value
        return compacted or None

    def _stringify_id(self, value: Any) -> str | None:
        """Convert numeric identifiers to strings to avoid JSON precision loss."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return str(int(value))
        text = str(value).strip()
        return text or None

    def _coerce_timestamp_value(self, value: Any) -> Any:
        """Convert diverse timestamp representations into the configured Supabase column format."""
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
        """Return a default timestamp using the column's precision (ms epoch vs ISO string)."""
        reference = reference or datetime.now(timezone.utc)
        if self.timestamp_column == "ts":
            return int(reference.timestamp() * 1000)
        return reference.isoformat()

    def _epoch_to_datetime(self, value: float) -> datetime:
        """Translate epoch values (seconds or milliseconds) into UTC-aware datetimes."""
        if value > 1e12:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)

    def _should_retry_with_payload_fallback(self, response: requests.Response) -> bool:
        """Detect missing payload column errors that warrant retrying with a fallback column."""
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
        """Return the configured payload fallback column when it differs from the primary."""
        fallback = self.payload_fallback_column
        if not fallback or self.payload_column == fallback:
            return None
        return fallback

    def _should_retry_with_timestamp_fallback(self, response: requests.Response) -> bool:
        """Detect missing timestamp column errors that require retrying with a fallback column."""
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
        """Return the configured timestamp fallback column when it differs from the primary."""
        fallback = self.timestamp_fallback_column
        if not fallback or self.timestamp_column == fallback:
            return None
        return fallback

    def _maybe_reload_schema_cache(self, response: requests.Response) -> bool:
        """Request a PostgREST schema cache reload when Supabase reports stale metadata."""
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
