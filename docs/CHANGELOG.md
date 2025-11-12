# Changelog (Unreleased)

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

## Data fidelity
- `HotelRateRecord.to_dict()` now lifts `special_offer`, `supplier_rate_promotion`, and `comparison_amenity` to top-level keys so downstream storage can persist them verbatim.
- New pytest coverage (`tests/test_sqlite_store.py`, updated `tests/test_placeholder.py`) exercises the end-to-end persistence path and directory bootstrap logic.
- Default stealth/fingerprint toggles were dialed back to `False` so unusual fingerprints are only applied when explicitly requested.
