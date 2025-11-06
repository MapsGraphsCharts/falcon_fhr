# Secure Scraper

A modular Playwright-powered scraper that supports both headed and headless execution against
fingerprint-aware targets. The project is structured to keep authentication, workflows, storage,
and browser orchestration cleanly separated while making it easy to toggle stealth capabilities.

## Key Features
- Async-first Patchright (Playwright-compatible) workflows for headed or headless sessions
- Stealth layer built around [`playwright-stealth`](https://github.com/mattwmaster58/playwright_stealth)
- Config management through environment variables with type validation
- Hooks for login/search/download orchestration and JSON persistence
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
│       ├── tasks/          # High-level scraping workflows (search, download)
│       └── utils/          # Shared utilities (throttling, retries, metrics)
├── tests/                  # Async unit/integration tests
└── data/                   # Local persistence (logs, downloads)
```

## Getting Started
1. Create and populate `.env` from `.env.example`.
   ```
   SCRAPER_USERNAME=<your amex username>
   SCRAPER_PASSWORD=<your password>
   SCRAPER_MFA_SECRET=<optional base32 TOTP secret>
   SCRAPER_STORAGE_STATE_PATH=data/logs/network/storage_state_latest.json
   SCRAPER_HEADLESS=false  # set to true for unattended runs
   SCRAPER_SEARCH_LOCATION_ID=ZMETRO-EXPEDIA-179899
   SCRAPER_SEARCH_LOCATION_NAME=Rome (and vicinity), Lazio, Italy
   SCRAPER_SEARCH_CHECK_IN=2025-11-11
   SCRAPER_SEARCH_NIGHTS=3
   SCRAPER_SEARCH_ADULTS=2
   SCRAPER_SEARCH_LATITUDE=41.903755
   SCRAPER_SEARCH_LONGITUDE=12.479556
   SCRAPER_DESTINATION_CATALOG_PATH=data/destinations/catalog.json
   SCRAPER_SEARCH_DESTINATION_KEYS=["us-texas","ca-quebec"]  # optional list from the catalog
   ```
   - If `SCRAPER_MFA_SECRET` is not supplied, the login flow will prompt for the SMS/email code.
   - Once a session is established, the storage-state file can be reused to skip fresh logins.
2. Install project dependencies:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e .[dev]
   ```
3. Install Patchright's patched Chromium build (and optional Linux deps) via the helper script:
   ```bash
   python scripts/bootstrap.py
   ```
4. Capture network artefacts (optional but recommended for new targets):
   ```bash
   PYTHONPATH=src python scripts/capture_network.py --interactive
   ```
5. Run the automated login + placeholder workflow:
   ```bash
   PYTHONPATH=src python scripts/run_scraper.py
   ```
   After login the script warms up the search results page and calls the `hotel/properties` API directly, saving both the raw response (`data/downloads/hotels_raw.json`) and a simplified list (`data/downloads/hotels.json`).

### Inspecting capture artefacts
Use the analyzer to summarise tokens and cookies stored during a capture run:
```bash
PYTHONPATH=src python -m secure_scraper.analysis.analyze_capture \\
    --capture data/logs/network/network_capture_<timestamp>.json \\
    --storage data/logs/network/storage_state_<timestamp>.json
```
The summary lists high-value endpoints, extracted tokens (e.g. `publicGuid`, `assessmentToken`), and the complete cookie inventory grouped by domain.

### API-based search
After login the scraper opens the travel search-results route to satisfy Amex's anti-bot checks before POSTing to `…/hotel/properties`. Payloads are generated via `SearchParams`. For each destination a subdirectory is created under `data/downloads/<destination_key>/` containing:
- the raw API payload (`hotels_raw.json`)
- a DB-ready hotel metadata snapshot (`hotels_normalized.json`)
- a stay-specific rate snapshot (`rates_normalized.json`)

When multiple destinations are processed the normalised aggregates are deduplicated into `master_hotels_normalized.json` and `master_rates_normalized.json`. Override the default destination/dates/adults with the `SCRAPER_SEARCH_*` settings or construct custom payloads in code.

### Destination catalog
- The maintained catalog lives at `data/destinations/catalog.json`. Each entry contains a unique key, the Amex display name, and placeholders for the required metadata (`location_id`, `latitude`, `longitude`).
- Set `SCRAPER_SEARCH_DESTINATION_KEYS` (comma separated) to drive searches from the catalog. Use `*` to run every ready destination or prefix with `group:` (e.g. `group:United States`) to select by region. When present, defaults from `SCRAPER_SEARCH_LOCATION_*` are ignored.
- Run the helper to inspect or hydrate catalog entries:
  ```bash
  PYTHONPATH=src python scripts/manage_destinations.py --missing
  PYTHONPATH=src python scripts/manage_destinations.py --hydrate-missing   # fill location_id/lat/lon
  ```
  This prints entries that still need metadata before they can be searched. Populate the missing fields manually or extend the script to harvest them automatically from captured traffic.
- Per-destination results now live in `data/downloads/<destination_key>/`.

### Filtering & automation tips
- Provide the JSON list `SCRAPER_SEARCH_PROGRAM_FILTER` to restrict results to Fine Hotels + Resorts® or The Hotel Collection, for example:
  ```bash
  SCRAPER_SEARCH_PROGRAM_FILTER='["FHR"]' PYTHONPATH=src python scripts/run_scraper.py
  ```
  Multiple programs are allowed: `["FHR","THC"]`.
- Pagination is automatic: when program filters are present the scraper walks every results page until Amex stops returning hotels.
- Set `SCRAPER_HEADLESS=true` to run the browser in headless mode for unattended jobs.
- All environment variables can live in `.env` or be supplied inline on the command line.

## Next Steps
- Expand `SearchParams` to cover additional filters (price ranges, loyalty tiers, amenities).
- Improve `LoginFlow` resilience with analytics around failed MFA attempts or credential lockouts.
- Extend `storage.json_writer.JsonStore` with streaming writes or cloud transports as needed.

## References
- Patchright Python docs (`/Kaliiiiiiiiii-Vinyzu/patchright-python`) for drop-in Playwright compatibility.
- Playwright Python docs on browser contexts & storage state (`/microsoft/playwright-python`)—still applicable to the Patchright API surface.
- Stealth patterns from `playwright-stealth` (`/mattwmaster58/playwright_stealth`).
