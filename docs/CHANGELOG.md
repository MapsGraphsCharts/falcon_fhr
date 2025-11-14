# Changelog (Unreleased)

## Browser routing & pacing
- Added optional Hyperbrowser routing (via `hyperbrowser` Python dependency) so sweeps can run inside Hyperbrowser-managed Chromium sessions with built-in stealth/cookie consent helpers. New settings/env toggles include `hyperbrowser_enabled`, `hyperbrowser_api_key`, `hyperbrowser_region`, `hyperbrowser_use_stealth`, and `hyperbrowser_accept_cookies`.
- Browser sessions now detect `TargetClosedError`/`PatchrightError` signals, rebuild the Playwright context, and relogin automatically instead of crashing a sweep when the remote browser is recycled.
- Introduced `browser.destination_pause_s` / `SCRAPER_DESTINATION_PAUSE_S` to enforce a configurable pause between destination API calls when you need to mimic human pacing.
- Added `max_consecutive_backend_failures` / `SCRAPER_MAX_CONSECUTIVE_BACKEND_FAILURES` so long sweeps stop hammering the properties API once it returns repeated 5xx responses.

## Run profiles & config
- Added `config/global-90d-sample.toml`, a ready-made three-night sweep that hits every catalog destination roughly 90 days in the future for quick coverage snapshots.
- Hyperbrowser is now the default browser mode; keep `SCRAPER_HYPERBROWSER_ENABLED=false` (or pass `--override hyperbrowser_enabled=false`) to reuse the local Patchright profile.

## Storage + Workflow overhaul
- Swapped the JSON download pipeline for structured SQLite persistence (`SqliteStore`) with automatic resume/skip semantics per destination run.
- Added new `[storage]` run-config knobs (`sqlite_enabled`, `sqlite_path`, `sqlite_busy_timeout_ms`) plus corresponding environment variables to control the database path/timeouts.
- `SqliteStore` now forces Write-Ahead Logging (`sqlite_journal_mode`, `sqlite_synchronous`) by default so analysis scripts can read while the scraper writes; both PRAGMAs are overridable from `.env`/run configs.
- Scraper CLI now seeds `search_runs`, `hotels`, `room_types`, `rate_snapshots`, nightly prices, and raw payloads instead of writing `data/downloads/*` bundles. JSON download helpers (`DownloadTask`, `storage/json_writer.py`) were removed.
- Introduced turnkey run profiles (`config/birthdays.toml`, `config/catalog-snapshot.toml`, updated `config/japan-range.toml`) that target long sweeps while defaulting to SQLite persistence.
- Added `scripts/analyze_value_windows.py` for quick price-volatility summaries directly against `data/storage/hotels.sqlite3`.

## Resilience improvements
- `SearchClient` now validates auth cookies before requesting tokens, surfaces backend 5xx responses as `BackendUnavailableError`, and retries appropriately.
- Session refresh failures from the auth endpoint (HTTP 200 without a `clientCustomerId`) now trigger an explicit relogin instead of spinning forever.
- `SqliteStore` exposes a configurable busy timeout (default 2s) so local development fails fast when the DB is locked (e.g., open in DataGrip) and logs migration failures for easier diagnosis.
- Added `SqliteStore.fetch_latest_runs_bulk()` plus test coverage so sweeps can preflight every destination in a single query and skip already-complete runs with minimal SQLite round trips.

## Data fidelity
- `HotelRateRecord.to_dict()` now lifts `special_offer`, `supplier_rate_promotion`, and `comparison_amenity` to top-level keys so downstream storage can persist them verbatim.
- New pytest coverage (`tests/test_sqlite_store.py`, updated `tests/test_placeholder.py`) exercises the end-to-end persistence path and directory bootstrap logic.
- Default stealth/fingerprint toggles were dialed back to `False` so unusual fingerprints are only applied when explicitly requested.
