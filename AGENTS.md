# Repository Guidelines

## Project Structure & Module Organization
Secure Scraper keeps runtime code under `src/secure_scraper`, split into auth, config, core, selectors, storage, tasks, and utils packages. CLI entrypoints live in `scripts/` (`bootstrap.py` handles Patchright installs; `run_scraper.py` orchestrates workflows). Configuration lives in `config/`, runtime artefacts in `data/` (logs, Chrome profile, SQLite), docs under `docs/`, and async tests in `tests/`. Keep new modules co-located with their domain folder and prefer composing existing utils over duplicating logic.

## Build, Test, and Development Commands
`python -m venv .venv && source .venv/bin/activate` creates an isolated env; install deps with `pip install -e .[dev]`. Run the scraper via `PYTHONPATH=src python scripts/run_scraper.py --config config/run_config.toml` (override flags with `--override key=value`). Install/verify browsers using `python scripts/bootstrap.py`. Lint and format with `ruff check src tests` and `ruff format src tests`. Execute the suite using `python -m pytest tests -ra`; target scenarios quickly with `pytest tests/test_fastmail.py -vv`.

## Coding Style & Naming Conventions
Ruff enforces 4-space indentation, 100-char lines, double quotes, and lint rules `E,F,I,UP,B`. Keep modules and functions snake_case, classes PascalCase, and constants ALL_CAPS. New config keys should mirror existing `SCRAPER_*` environment prefixes. Prefer dataclass/pydantic models for structured settings, and annotate every public function. Place async helpers in `utils/` or the nearest domain package instead of spawning flat modules.

## Testing Guidelines
Use `pytest` with `pytest-asyncio`; mark async functions with `@pytest.mark.asyncio` and name tests `test_*`. Stub network calls using httpx dummy clients or monkeypatch fixtures as in `tests/test_fastmail.py`. Add regression tests whenever adding selectors, storage writers, or MFA integrations, and keep coverage over critical flows (auth, Fastmail OTP, SQLite writers). Run `pytest --maxfail=1 --disable-warnings -q` before opening a PR.

## Commit & Pull Request Guidelines
Follow the Conventional Commits style seen in history (`feat(storage): …`, `chore: …`). Keep subjects imperative and under ~72 chars; explain config changes or migrations in the body. PRs should describe the scenario, include reproduction/validation notes (commands run, config profiles used), link related issues, and attach screenshots or log excerpts when UI or stealth toggles are involved.

## Security & Configuration Tips
Never commit `.env`, secrets, or items under `data/chrome-profile/`. Reference `.env.example` when adding new `SCRAPER_*` variables and document default overrides in `config/*.toml`. Mask credentials in logs and scrub captured network artefacts before sharing. Prefer `PYTHONPATH=src` over editable installs when running ad hoc scripts to avoid leaking uncommitted code into other projects.
