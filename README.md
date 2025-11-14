# Secure Scraper

A modular Playwright-powered scraper that supports both headed and headless execution against
fingerprint-aware targets. The project is structured to keep authentication, workflows, storage,
and browser orchestration cleanly separated while making it easy to toggle stealth capabilities.

## Key Features
- Async-first Patchright (Playwright-compatible) workflows for headed or headless sessions
- Stealth layer built around [`playwright-stealth`](https://github.com/mattwmaster58/playwright_stealth)
- Config management through environment variables with type validation
- Hooks for login/search orchestration with SQLite persistence
- Extensible task modules and selector registries for complex pages

## Repository Layout
```
.
├── pyproject.toml
├── README.md
├── scripts/
│   ├── bootstrap.py        # Environment bootstrap & browser installs
│   └── run_scraper.py      # CLI entrypoint orchestrating workflows
├── src/
│   └── secure_scraper/
│       ├── auth/           # Login and multi-factor orchestration
│       ├── config/         # Settings & secrets management
│       ├── core/           # Browser factories, stealth binding, logging
│       ├── selectors/      # Centralised locator maps per page/task
│       ├── storage/        # Data writers (JSON, DB, cloud, ...)
│       ├── tasks/          # High-level scraping workflows (search orchestration)
│       └── utils/          # Shared utilities (throttling, retries, metrics)
├── tests/                  # Async unit/integration tests
└── data/                   # Local persistence (logs, storage)
```

## Getting Started
1. Create and populate `.env` from `.env.example`.
   ```
   SCRAPER_USERNAME=<your amex username>
   SCRAPER_PASSWORD=<your password>
   SCRAPER_MFA_SECRET=<optional base32 TOTP secret>
   SCRAPER_STORAGE_STATE_PATH=data/logs/network/storage_state_latest.json
   SCRAPER_HEADLESS=true  # pass --headed or set false when you want to watch the browser
   SCRAPER_SEARCH_LOCATION_ID=ZMETRO-EXPEDIA-179899
   SCRAPER_SEARCH_LOCATION_NAME=Rome (and vicinity), Lazio, Italy
   SCRAPER_SEARCH_ADULTS=2
   SCRAPER_SEARCH_LATITUDE=41.903755
   SCRAPER_SEARCH_LONGITUDE=12.479556
   SCRAPER_DESTINATION_CATALOG_PATH=data/destinations/catalog.json
   SCRAPER_HYPERBROWSER_ENABLED=true  # set false to keep using the local Patchright profile
   SCRAPER_HYPERBROWSER_API_KEY=<your hyperbrowser api key>
   ```
   - If `SCRAPER_MFA_SECRET` is not supplied, the login flow will prompt for the SMS/email code.
   - Provide `SCRAPER_FASTMAIL_API_TOKEN` (plus optional `SCRAPER_FASTMAIL_*` filters) to auto-resolve OTP codes from Fastmail without manual input. By default the scraper looks for mail from `AmericanExpress@welcome.americanexpress.com` with the subject `Your American Express one-time verification code` and a six-digit code in the message body.
   - Hyperbrowser routing is opt-out: leave `SCRAPER_HYPERBROWSER_ENABLED=true` (default) to run on Hyperbrowser's managed Chromium sessions, or set it to `false` (and skip the API key) to reuse the local Patchright profile.
   - Once a session is established, the storage-state file can be reused to skip fresh logins.
2. Tune `config/run_config.toml` for day-to-day runs.
   - `search.check_in` accepts ISO dates (`2025-12-01`) or relative offsets such as `+14d`, `+2w`, or `+1m`.
   - List catalog keys or groups under `search.destinations`. Leave the list empty to fall back to the manual destination from `.env`.
   - Use additional profiles by pointing the runner at another file: `python scripts/run_scraper.py --config config/europe.toml`.
   - Routine-ready configs now live under `config/routines/` (see `config/README.md`). For example, `config/routines/global/next-7-days.toml` runs a rolling week of 3-night sweeps across every catalog destination with a single flag change.
   - `config/global-90d-sample.toml` still ships as the baseline far-future sweep that hits every catalog destination roughly 90 days out—ideal for sanity-checking coverage or generating wide snapshots without editing the default profile.
   - Browser toggles (headless, log level, viewport) can stay in this file so you rarely touch environment variables; headless defaults to `true`, so flip it to `false` (or use `--headed`) when you want to watch a run. Set `search_warmup_enabled = true` only if you want to capture the slow warm-up page; it defaults to `false` for faster runs.
   - A persistent Chrome profile lives in `data/chrome-profile/` by default. Remove that directory to reset cached cookies/devices.
   - **Hyperbrowser routing (default):** provide `HYPERBROWSER_API_KEY` / `SCRAPER_HYPERBROWSER_API_KEY` when `hyperbrowser_enabled=true` so sessions launch inside Hyperbrowser's cloud browsers. Disable the setting (or pass `--override hyperbrowser_enabled=false`) to keep using the bundled Patchright profile. Optional knobs include `hyperbrowser_region`, `hyperbrowser_use_stealth`, and `hyperbrowser_accept_cookies`. Sessions run roughly $0.10/hour (see [Hyperbrowser pricing](https://www.hyperbrowser.ai/pricing)).
   - We default to the bundled Chromium build. If you install retail Chrome (`patchright install chrome` on supported distros), set `chromium_channel="chrome"` via `.env` or `--override` to opt in.
   - `login_monitor_markers = false` (default) skips the legacy credentials/session network wait so runs resume immediately after OTP. Flip it to `true` only if you need the old debug traces.
   - `browser.destination_pause_s = 2.5` (or `SCRAPER_DESTINATION_PAUSE_S`) controls the pause between destinations; set it to `0` for maximum throughput or bump it higher if Amex starts rate-limiting.
   - `max_consecutive_backend_failures = 5` (override via `SCRAPER_MAX_CONSECUTIVE_BACKEND_FAILURES`) stops a sweep when the properties API keeps returning 5xx responses so you don’t burn time hammering an outage.
   - To sweep a range of check-in dates, add a `[date_range]` block. Example:
     ```toml
     [date_range]
     start = "2025-12-01"      # ISO or relative offset
     end = "2026-02-28"        # optional; +3m works too
     step_days = 1             # run every night
     nights = 1                # override stay length per iteration
     ```
     Each iteration records a row in the `search_runs` table (plus `hotels`, `rates`, etc.) so sweeps can resume mid-way without replaying completed destinations.
   - `sweep_priority` controls iteration order when a `[date_range]` is present. Leave it unset (default `"date-first"`) to iterate every date before switching destinations,
     or set `sweep_priority = "destination-first"` to finish the entire date range for each destination before moving on.
3. Install project dependencies:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e .[dev]
   ```
4. Install Patchright's patched Chromium build (and optional Linux deps) via the helper script:
   ```bash
   python scripts/bootstrap.py
   ```
5. Capture network artefacts (optional but recommended for new targets):
   ```bash
   PYTHONPATH=src python scripts/capture_network.py --interactive
   ```
6. Run the automated login + placeholder workflow:
   ```bash
   PYTHONPATH=src python scripts/run_scraper.py  # add --headed to watch the browser
   # Layer quick tweaks without editing config:
   #   PYTHONPATH=src python scripts/run_scraper.py --override search_check_in="2025-11-20"
   ```
   After login the script warms up the search results page and calls the `hotel/properties` API directly, persisting the raw payload plus the derived hotel/rate rows into `data/storage/hotels.sqlite3`.

### Inspecting capture artefacts
Use the analyzer to summarise tokens and cookies stored during a capture run:
```bash
PYTHONPATH=src python -m secure_scraper.analysis.analyze_capture \\
    --capture data/logs/network/network_capture_<timestamp>.json \\
    --storage data/logs/network/storage_state_<timestamp>.json
```
The summary lists high-value endpoints, extracted tokens (e.g. `publicGuid`, `assessmentToken`), and the complete cookie inventory grouped by domain.

### API-based search
After login the scraper opens the travel search-results route to satisfy Amex's anti-bot checks before POSTing to `…/hotel/properties`. Payloads are generated via `SearchParams`, and every response is normalized directly into SQLite (`destinations`, `search_runs`, `hotels`, `room_types`, `rate_snapshots`, etc.).

Rerunning the CLI will now consult the DB before each destination: if the latest run for a given signature (label + check-in/check-out + adults/program filters) is already `complete`, the scraper skips it automatically so you resume exactly where the last sweep failed. Override the default destination/dates/adults through `config/run_config.toml` (or `SCRAPER_SEARCH_*` env overrides) or construct custom payloads programmatically.

### Structured storage (SQLite)
- Flip `SCRAPER_SQLITE_STORAGE_ENABLED=true` (or set `[storage] sqlite_enabled = true` in your run_config) to persist each destination run into `data/storage/hotels.sqlite3`.
- The store tracks `search_runs` (status, request IDs, labels), immutable `destinations`, deduplicated `hotels`, `room_types`, and one row per stay-specific `rate_snapshot` along with nightly prices and fee/tax components.
- Full JSON payloads are kept in the `search_payloads` table (per run) plus the `raw_json` column on `hotels`, so you retain every field even before it is mapped onto columns.
- Resume support becomes simpler: every run is marked `running`/`complete`/`failed`, so you can spot and rerun destinations that crashed mid-way without losing history.
- Point BI tools or ad-hoc SQL at the file whenever you want deeper analysis without juggling dozens of JSON dumps.
- Writers enable WAL journaling by default (`SCRAPER_SQLITE_JOURNAL_MODE=wal`, `SCRAPER_SQLITE_SYNCHRONOUS=normal`) so read-only tools can tail the DB without blocking the scraper. Override either knob in `.env` or `[storage]` when a stricter mode is required.
- If you're developing with the DB open in another tool, tune `SCRAPER_SQLITE_BUSY_TIMEOUT_MS` (or `[storage] sqlite_busy_timeout_ms`) so the writer waits a little longer before erroring. WAL plus a larger timeout usually eliminates the repeated “database is locked” failures.

### Known issues
- Newly added sweep profiles (e.g. `config/caribbean-winter.toml`) still reference the old per-region Canada/Mexico destination keys even though `data/destinations/catalog.json` now consolidates them. Until the catalog is expanded again, override those configs with the new aggregate keys or limit runs to destinations that exist in the catalog.

### Value analysis helpers
- Use `scripts/analyze_value_windows.py` to surface large price swings per room type (default query inspects Japan FHR sweeps). Point it at any SQLite capture with `--db data/storage/hotels.sqlite3` and trim the window/destination clauses as needed.

### Destination catalog
- The maintained catalog lives at `data/destinations/catalog.json`. Each entry contains a unique key, the Amex display name, and placeholders for the required metadata (`location_id`, `latitude`, `longitude`).
- Set `search.destinations` in `config/run_config.toml` (or `SCRAPER_SEARCH_DESTINATION_KEYS`) to drive catalog selections. Use `*` to run every ready destination or prefix with `group:` (e.g. `group:United States`) to select by region. When present, defaults from `SCRAPER_SEARCH_LOCATION_*` are ignored.
- Run the helper to inspect or hydrate catalog entries:
  ```bash
  PYTHONPATH=src python scripts/manage_destinations.py --missing
  PYTHONPATH=src python scripts/manage_destinations.py --hydrate-missing   # fill location_id/lat/lon
  ```
  This prints entries that still need metadata before they can be searched. Populate the missing fields manually or extend the script to harvest them automatically from captured traffic.
- Per-destination results now live exclusively in SQLite; inspect them via SQL or ad-hoc tooling instead of chasing JSON files.

### Filtering & automation tips
- Provide the JSON list `SCRAPER_SEARCH_PROGRAM_FILTER` to restrict results to Fine Hotels + Resorts® or The Hotel Collection, for example:
  ```bash
  SCRAPER_SEARCH_PROGRAM_FILTER='["FHR"]' PYTHONPATH=src python scripts/run_scraper.py
  ```
  Multiple programs are allowed: `["FHR","THC"]`.
- Alternatively, add `program_filter = ["FHR"]` to your run profile so the setting is shared across runs.
- Pagination is automatic: when program filters are present the scraper walks every results page until Amex stops returning hotels.
- Runs are headless by default; set `browser.headless = false`, `SCRAPER_HEADLESS=false`, or pass `--headed` to watch the UI.
- Use `--override key=value` (repeatable, JSON-friendly) for ad-hoc tweaks: `--override fingerprint_enabled=false --override headless=false`.
- All environment variables can live in `.env` or be supplied inline on the command line.

## Next Steps
- Expand `SearchParams` to cover additional filters (price ranges, loyalty tiers, amenities).
- Improve `LoginFlow` resilience with analytics around failed MFA attempts or credential lockouts.
- Add lightweight SQL views/exporters for BI tools (DuckDB, Arrow, etc.) as workflows evolve.

## Changelog
See `docs/CHANGELOG.md` for a living summary of the SQLite migration, new helper scripts, and other unreleased work.

## References
- Patchright Python docs (`/Kaliiiiiiiiii-Vinyzu/patchright-python`) for drop-in Playwright compatibility.
- Playwright Python docs on browser contexts & storage state (`/microsoft/playwright-python`)—still applicable to the Patchright API surface.
- Stealth patterns from `playwright-stealth` (`/mattwmaster58/playwright_stealth`).
