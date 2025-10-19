from __future__ import annotations

import threading
from typing import Any, Dict, List

from supabase import Client, create_client

from logging_config import get_logger


class SupabaseService:
    """Thin wrapper around Supabase client with basic error handling and locking."""

    def __init__(self, url: str, key: str):
        self._client: Client = create_client(url, key)
        self._write_lock = threading.RLock()
        self.logger = get_logger("supabase")

    def fetch_streamers(self) -> List[Dict[str, Any]]:
        """Return streamers that should be monitored (audit_realtime == True)."""
        try:
            response = (
                self._client.table("douyin_streamers")
                .select("*")
                .eq("audit_realtime", True)
                .execute()
            )
            data = response.data or []
            self.logger.debug("Fetched %s streamers for monitoring", len(data))
            return data
        except Exception:
            self.logger.exception("Failed to fetch streamers from Supabase")
            return []

    def insert_event(self, event: Dict[str, Any]) -> None:
        """Persist a crawler event into douyin_events."""
        with self._write_lock:
            try:
                self._client.table("douyin_events").insert(event).execute()
            except Exception:
                self.logger.exception(
                    "Failed to insert event into douyin_events",
                    extra={"event_type": event.get("event_type")},
                )
