from __future__ import annotations

import argparse
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from concurrent.futures import ThreadPoolExecutor, as_completed

from apscheduler.executors.pool import ThreadPoolExecutor as APSchedulerThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

from config import MonitorConfig
from logging_config import get_logger
from liveMan import DouyinLiveWebFetcher
from supabase_client import SupabaseService


@dataclass
class FetcherState:
    fetcher: DouyinLiveWebFetcher
    thread: threading.Thread
    streamer: Dict[str, Any]


class StreamMonitor:
    def __init__(self, config: MonitorConfig, supabase: SupabaseService):
        self.config = config
        self.supabase = supabase
        self.logger = get_logger("monitor")
        self._lock = threading.RLock()
        self._active_fetchers: Dict[str, FetcherState] = {}
        self._cooldowns: Dict[str, datetime] = {}

    def run(self) -> None:
        scheduler = BlockingScheduler(
            jobstores={"default": MemoryJobStore()},
            executors={"default": APSchedulerThreadPoolExecutor(max_workers=1)},
            timezone=timezone.utc,
        )
        scheduler.add_job(
            self.check_streamers,
            trigger="interval",
            minutes=self.config.monitor_interval_minutes,
            id="poll_streamers",
            max_instances=1,
            coalesce=True,
        )
        self.logger.info(
            "Starting stream monitor | interval_minutes=%s test_mode=%s",
            self.config.monitor_interval_minutes,
            self.config.test_mode,
        )
        try:
            self.check_streamers()  # initial run
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Shutdown signal received; stopping monitor.")
        finally:
            self._shutdown_fetchers()
            self.supabase.close()

    def check_streamers(self) -> None:
        streamers = self.supabase.fetch_streamers()
        now = datetime.now(timezone.utc)
        if not streamers:
            self.logger.info("No audit_realtime streamers found during check.")
            return

        with ThreadPoolExecutor(max_workers=len(streamers)) as executor:
            futures = [
                executor.submit(self._evaluate_streamer, streamer, now)
                for streamer in streamers
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    self.logger.exception("Streamer evaluation task failed")

    def _evaluate_streamer(self, streamer: Dict[str, Any], now: datetime) -> None:
        live_id = self._extract_live_id(streamer)
        if not live_id:
            return

        with self._lock:
            if live_id in self._active_fetchers:
                self.logger.debug("Streamer already active | live_id=%s", live_id)
                return
            cooldown_until = self._cooldowns.get(live_id)

        if cooldown_until and cooldown_until > now:
            self.logger.debug(
                "Skipping live_id=%s still in cooldown until %s",
                live_id,
                cooldown_until.isoformat(),
            )
            return

        fetcher = DouyinLiveWebFetcher(
            live_id=live_id,
            streamer=streamer,
            config=self.config,
            event_sink=self._build_event_sink(streamer),
            stop_callback=self._on_fetcher_stopped,
        )

        try:
            is_streaming = fetcher.is_streaming()
        except Exception:
            self.logger.exception("Failed to evaluate stream status | live_id=%s", live_id)
            return

        if not is_streaming:
            self.logger.info("Streamer offline | live_id=%s streamer_id=%s", live_id, streamer.get("id"))
            with self._lock:
                self._cooldowns[live_id] = now + timedelta(minutes=self.config.monitor_interval_minutes)
            return

        thread = threading.Thread(target=self._run_fetcher, args=(fetcher,), daemon=True)

        with self._lock:
            if live_id in self._active_fetchers:
                self.logger.debug(
                    "Streamer became active while evaluating | live_id=%s streamer_id=%s",
                    live_id,
                    streamer.get("id"),
                )
                return
            self._active_fetchers[live_id] = FetcherState(fetcher=fetcher, thread=thread, streamer=streamer)

        thread.start()
        self.logger.info("Started monitoring | live_id=%s streamer_id=%s", live_id, streamer.get("id"))

    def _run_fetcher(self, fetcher: DouyinLiveWebFetcher) -> None:
        try:
            fetcher.start()
        except Exception:
            self.logger.exception("Fetcher crashed | live_id=%s", fetcher.live_id)
        finally:
            self._on_fetcher_stopped(fetcher.live_id, reason="thread_exit")

    def _on_fetcher_stopped(self, live_id: str, *, reason: str = "unknown") -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._active_fetchers.pop(live_id, None)
            if state is None and reason == "thread_exit" and live_id in self._cooldowns:
                # Callback already processed shutdown; avoid duplicate cooldown/logs.
                return
            self._cooldowns[live_id] = now + timedelta(minutes=self.config.monitor_interval_minutes)

        if state:
            self.logger.info(
                "Stopped monitoring | live_id=%s streamer_id=%s reason=%s",
                live_id,
                state.streamer.get("id"),
                reason,
            )
        else:
            self.logger.debug("Fetcher stop called without active state | live_id=%s reason=%s", live_id, reason)

    def _shutdown_fetchers(self) -> None:
        with self._lock:
            states = list(self._active_fetchers.values())
            self._active_fetchers.clear()

        for state in states:
            state.fetcher.stop(reason="shutdown")

        for state in states:
            state.thread.join(timeout=10)
            if state.thread.is_alive():
                self.logger.warning("Fetcher thread failed to exit cleanly | live_id=%s", state.fetcher.live_id)

    def _build_event_sink(self, streamer: Dict[str, Any]):
        def sink(event: Dict[str, Any]) -> None:
            live_identifier = event.get("live_id") or streamer.get("live_id") or streamer.get("room_id")
            if not live_identifier:
                self.logger.error("Dropping event without live_id | event_type=%s", event.get("event_type"))
                return

            payload = self._prepare_event_payload(event.get("payload"), streamer)
            ts_value = self._coerce_event_timestamp(event)

            room_identifier = event.get("room_id") or streamer.get("room_id") or streamer.get("live_id")

            record = {
                "live_id": str(live_identifier),
                "room_id": str(room_identifier) if room_identifier is not None else None,
                "event_type": event.get("event_type"),
                "event_payload": payload,
                "ts": ts_value,
            }
            self.supabase.insert_event(record)

        return sink

    def _prepare_event_payload(self, payload: Any, streamer: Dict[str, Any]) -> Any:
        if payload is None:
            enriched: Dict[str, Any] = {}
        elif isinstance(payload, dict):
            enriched = dict(payload)
        else:
            return payload

        streamer_id = streamer.get("id")
        if streamer_id is not None and "streamer_id" not in enriched:
            enriched["streamer_id"] = str(streamer_id)

        nickname = streamer.get("nickname")
        if nickname and "source_nickname" not in enriched:
            enriched["source_nickname"] = str(nickname)

        return enriched

    def _coerce_event_timestamp(self, event: Dict[str, Any]) -> int:
        ts_value = event.get("ts")
        if isinstance(ts_value, (int, float)):
            return int(ts_value)
        if isinstance(ts_value, str) and ts_value.strip().isdigit():
            return int(ts_value.strip())

        received_at = event.get("received_at")
        if isinstance(received_at, str):
            text = received_at.strip()
            if text:
                try:
                    if text.endswith("Z"):
                        text = text[:-1] + "+00:00"
                    timestamp = datetime.fromisoformat(text)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)
                    else:
                        timestamp = timestamp.astimezone(timezone.utc)
                    return int(timestamp.timestamp() * 1000)
                except ValueError:
                    self.logger.debug("Failed to parse received_at=%s; falling back to current time", received_at)

        return int(time.time() * 1000)

    @staticmethod
    def _extract_live_id(streamer: Dict[str, Any]) -> Optional[str]:
        live_id = streamer.get("live_id") or streamer.get("room_id")
        if not live_id:
            get_logger("monitor").warning("Streamer record missing live_id | streamer=%s", streamer)
            return None
        return str(live_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Douyin stream monitor")
    parser.add_argument("--test-interval", action="store_true", help="Enable 1-minute interval for testing.")
    args = parser.parse_args()

    config = MonitorConfig.load(force_test_mode=args.test_interval)
    supabase = SupabaseService(config)
    monitor = StreamMonitor(config, supabase)
    monitor.run()


if __name__ == "__main__":
    main()
