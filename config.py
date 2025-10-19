import os
from dataclasses import dataclass


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


@dataclass(frozen=True)
class MonitorConfig:
    supabase_url: str
    supabase_key: str
    monitor_interval_minutes: int
    test_mode: bool
    anti_crawl_min_delay: float
    anti_crawl_max_delay: float

    @property
    def cron_minute_expression(self) -> str:
        """
        APScheduler cron trigger expects strings like '*/5'. For hourly default, '*/60' resolves to minute 0.
        """
        return f"*/{self.monitor_interval_minutes}"

    @classmethod
    def load(cls, *, force_test_mode: bool | None = None) -> "MonitorConfig":
        test_mode = force_test_mode if force_test_mode is not None else _env_bool("FAST_MONITOR_TEST", False)
        default_interval = 1 if test_mode else 60
        interval = int(os.getenv("MONITOR_INTERVAL_MINUTES", default_interval))
        if interval < 1:
            interval = default_interval

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

        if not supabase_url or not supabase_key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) must be set in the environment."
            )

        min_delay = _env_float("ANTI_CRAWL_MIN_DELAY_SECONDS", 1.5 if not test_mode else 0.2)
        max_delay = _env_float("ANTI_CRAWL_MAX_DELAY_SECONDS", 4.0 if not test_mode else 0.8)

        if max_delay < min_delay:
            max_delay = min_delay

        return cls(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            monitor_interval_minutes=interval,
            test_mode=test_mode,
            anti_crawl_min_delay=min_delay,
            anti_crawl_max_delay=max_delay,
        )
