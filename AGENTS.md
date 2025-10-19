# Repository Guidelines

## Project Structure & Module Organization
- `main.py`: thin wrapper that boots `monitor.main()` for service deployments.
- `monitor.py`: APScheduler-driven orchestrator that polls Supabase, spins up crawlers, and enforces per-room cooldowns.
- `liveMan.py`: crawler core handling tokens, anti-crawl pacing, WebSocket parsing, logging, and event emission.
- `supabase_client.py`: minimal Supabase wrapper for reading `douyin_streamers` and inserting `douyin_events`.
- `config.py`: centralizes env-driven settings (Supabase URL/key, monitor interval, anti-crawl delays, test toggle).
- `logging_config.py`: configures process-wide colorful structured logging.
- `protobuf/`: generated protocol buffers (`douyin.py`) plus schema (`douyin.proto`); regenerate whenever the schema changes.
- JavaScript helpers (`sign.js`, `sign_v0.js`, `a_bogus.js`, `webmssdk.js`) contain reverse-engineered signing routines.
- `requirements.txt`: authoritative dependency list; keep in sync when adding libraries (Supabase client, APScheduler, etc.).

## Build, Test, and Development Commands
- `python3 -m venv .venv && source .venv/bin/activate` (Windows: `.\.venv\Scripts\activate`) creates an isolated interpreter.
- `pip install -r requirements.txt` installs requests, betterproto, websocket-client, ExecJS/MiniRacer, Supabase, and APScheduler.
- Export `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` (or `SUPABASE_ANON_KEY`) before launching the monitor.
- `python monitor.py` starts the hourly cron-like scheduler; add `--test-interval` to switch to 1-minute polling while QAing.
- `python -m protobuf.douyin` (requires `protoc`) regenerates message classes after editing `douyin.proto`.
- For single-room debugging, flag only one `douyin_streamers` row with `audit_realtime=true` and run `python monitor.py --test-interval`.

## Coding Style & Naming Conventions
- Follow PEP 8: 4-space indentation, `snake_case` helpers, `CamelCase` classes (`DouyinLiveWebFetcher`, `StreamMonitor`).
- Use the shared logger via `logging_config.get_logger`; never revert to `print`.
- Emit crawler data through `_emit_event` so Supabase persistence stays centralized; avoid ad-hoc inserts.
- When adjusting anti-crawl logic (delays, retries, jitter), document the rationale with brief comments.
- Keep new modules at repo root unless a clear domain-specific package is required.

## Testing Guidelines
- No automated suite exists; validate by running `python monitor.py --test-interval` against staging rows and confirming `douyin_events` receives records.
- After touching signing logic (`sign.js`, `a_bogus.js`, MiniRacer integration), run smoke tests for token generation plus a short websocket session.
- Document manual verification steps, event samples, and Supabase table diffs in PR descriptions.
- When altering scheduler intervals or cooldowns, ensure duplicate crawlers never spin up by inspecting monitor logs.

## Commit & Pull Request Guidelines
- History favors concise Chinese summaries (e.g., `测试成功`); append an English clause for clarity (`测试成功 - add Supabase monitor`).
- Reference related GitHub issues in commits/PRs (`Fixes #74`) and describe expected impact or regressions.
- PRs should include purpose, setup instructions, manual test evidence, and attach screenshots only when user-visible artifacts change.
- Commit regenerated assets (e.g., refreshed `douyin.py`) only when tied to source schema updates; omit transient logs or payload captures.

## Security & Configuration Tips
- Never commit personal cookies, msToken values, Supabase keys, or private room identifiers.
- Store Supabase secrets in environment variables or your platform’s secret manager; avoid checking in `.env` files.
- Respect anti-crawl protections: keep randomized delays, jittered heartbeats, and cooldown windows intact or justify adjustments.
- Refresh `a_bogus.js` and other helpers locally; note upstream signature changes in PR descriptions.
- When deploying via cron/systemd, ensure only one monitor instance runs at a time (service-level lock or supervisor).
