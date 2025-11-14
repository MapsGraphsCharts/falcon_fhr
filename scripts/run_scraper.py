"""Entry point for manual runs."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from patchright._impl._errors import Error as PatchrightError
from patchright._impl._errors import TargetClosedError
from playwright.async_api import BrowserContext

from secure_scraper.auth.login_flow import LoginFlow
from secure_scraper.config.run_config import DateSweep, RunConfig
from secure_scraper.config.settings import Settings
from secure_scraper.core.browser import BrowserSession, ensure_close_context
from secure_scraper.core.logging import configure_logging
from secure_scraper.destinations.catalog import Destination, DestinationCatalog
from secure_scraper.hotels import (
    HotelRateRecord,
    HotelRecord,
    build_hotel_and_rate_records,
)
from secure_scraper.services.search_client import (
    BackendUnavailableError,
    SearchClient,
    SessionRefreshError,
)
from secure_scraper.storage import SqliteStore

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from secure_scraper.storage.sqlite_store import SearchRunRecord
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams


def _resolve_destinations(settings: Settings) -> list[Destination]:
    keys = [key.strip() for key in settings.search_destination_keys if key.strip()]

    if not keys:
        destinations = [
            Destination(
                key="custom",
                group="Manual",
                name=settings.search_location_name,
                location_id=settings.search_location_id,
                latitude=settings.search_latitude,
                longitude=settings.search_longitude,
            )
        ]
        return destinations

    catalog = DestinationCatalog.load(settings.destination_catalog_path)
    all_destinations = list(catalog.values())

    selected: list[Destination] = []
    include_all = False
    group_filters: list[str] = []

    for key in keys:
        lowered = key.lower()
        if lowered in {"*", "all"}:
            include_all = True
            continue
        if lowered.startswith("group:"):
            group_filters.append(key.split(":", 1)[1].strip())
            continue
        try:
            selected.append(catalog.get(key))
            continue
        except KeyError:
            match = next((dest for dest in all_destinations if dest.key.lower() == lowered), None)
            if match:
                selected.append(match)
                continue
            logging.warning(
                "Destination key '%s' not found in catalog %s", key, catalog.source
            )

    if group_filters:
        for group in group_filters:
            matches = [
                dest for dest in all_destinations if dest.group.lower() == group.lower()
            ]
            if not matches:
                logging.warning("No destinations matched group '%s'", group)
            else:
                selected.extend(matches)

    if include_all:
        selected.extend(all_destinations)

    deduped: list[Destination] = []
    seen: set[str] = set()
    for destination in selected:
        if destination.key in seen:
            continue
        seen.add(destination.key)
        deduped.append(destination)
    destinations = deduped

    ready: list[Destination] = []
    for destination in destinations:
        missing = destination.missing_fields()
        if missing:
            logging.warning(
                "Skipping destination %s (%s); missing metadata fields: %s",
                destination.key,
                destination.name,
                ", ".join(missing),
            )
            continue
        ready.append(destination)

    if not ready:
        requested = ", ".join(keys)
        raise RuntimeError(
            f"No destinations are ready for search for requested keys/groups: {requested}"
        )

    return ready


@dataclass(frozen=True)
class _DestinationRun:
    destination: Destination
    params: SearchParams
    label: str | None


@dataclass
class _SweepBatch:
    sweep: DateSweep
    runs: list[_DestinationRun]
    runs_by_destination: dict[str, _DestinationRun]

    @property
    def label(self) -> str | None:
        return self.sweep.label

    @property
    def label_text(self) -> str:
        return self.sweep.label or self.sweep.check_in.isoformat()


def _build_sweep_batches(
    destinations: list[Destination],
    sweeps: list[DateSweep],
    settings: Settings,
) -> list[_SweepBatch]:
    batches: list[_SweepBatch] = []
    program_filter = list(settings.search_program_filter)
    for sweep in sweeps:
        check_in = sweep.check_in
        if check_in is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Date sweep missing check-in date")
        nights = sweep.nights if sweep.nights is not None else settings.search_nights
        check_out = check_in + timedelta(days=nights)
        runs: list[_DestinationRun] = []
        indexed: dict[str, _DestinationRun] = {}
        for destination in destinations:
            location_id = destination.location_id
            latitude = destination.latitude
            longitude = destination.longitude
            if location_id is None or latitude is None or longitude is None:
                raise RuntimeError(
                    f"Destination {destination.key} missing metadata despite readiness check"
                )
            params = SearchParams(
                location_id=location_id,
                location_label=destination.name,
                latitude=latitude,
                longitude=longitude,
                check_in=check_in,
                check_out=check_out,
                rooms=[RoomRequest(adults=settings.search_adults)],
                program_filter=list(program_filter) if program_filter else None,
            )
            run = _DestinationRun(destination=destination, params=params, label=sweep.label)
            runs.append(run)
            indexed[destination.key] = run
        batches.append(_SweepBatch(sweep=sweep, runs=runs, runs_by_destination=indexed))
    return batches


async def _load_existing_runs_map(
    db_store: SqliteStore,
    batches: list[_SweepBatch],
) -> dict[tuple[str, str | None], SearchRunRecord]:
    mapping: dict[tuple[str, str | None], SearchRunRecord] = {}
    for batch in batches:
        if not batch.runs:
            continue
        run_pairs = [(run.destination, run.params) for run in batch.runs]
        records = await db_store.fetch_latest_runs_bulk(run_pairs, label=batch.label)
        for destination_key, record in records.items():
            mapping[(destination_key, batch.label)] = record
    return mapping


def _batch_complete(
    batch: _SweepBatch,
    existing_runs: dict[tuple[str, str | None], SearchRunRecord],
) -> bool:
    if not batch.runs:
        return True
    for run in batch.runs:
        record = existing_runs.get((run.destination.key, run.label))
        if record is None or record.status != "complete":
            return False
    return True


def _pending_runs_exist(
    batches: list[_SweepBatch],
    existing_runs: dict[tuple[str, str | None], SearchRunRecord],
) -> bool:
    if not existing_runs:
        return True
    for batch in batches:
        for run in batch.runs:
            record = existing_runs.get((run.destination.key, run.label))
            if record is None or record.status != "complete":
                return True
    return False


def _build_destination_first_queue(
    destinations: list[Destination],
    batches: list[_SweepBatch],
) -> list[_DestinationRun]:
    queue: list[_DestinationRun] = []
    for destination in destinations:
        for batch in batches:
            run = batch.runs_by_destination.get(destination.key)
            if run:
                queue.append(run)
    return queue


async def run(settings: Settings, sweeps: list[DateSweep]) -> None:
    db_store: SqliteStore | None = None
    if settings.sqlite_storage_enabled:
        db_store = SqliteStore(
            settings.sqlite_storage_path,
            busy_timeout_ms=settings.sqlite_busy_timeout_ms,
            journal_mode=settings.sqlite_journal_mode,
            synchronous=settings.sqlite_synchronous,
        )
        await db_store.initialize()
    try:
        async with BrowserSession(settings) as session:
            destinations = _resolve_destinations(settings)
            sweep_batches = _build_sweep_batches(destinations, sweeps, settings)
            existing_runs: dict[tuple[str, str | None], SearchRunRecord] = {}
            if db_store and settings.resume_completed_runs:
                existing_runs = await _load_existing_runs_map(db_store, sweep_batches)

            context: BrowserContext | None = None
            try:
                context = await session.new_context()
                if settings.sweep_priority == "destination-first":
                    pending_exists = True
                    if db_store and settings.resume_completed_runs:
                        pending_exists = _pending_runs_exist(sweep_batches, existing_runs)
                    if not pending_exists:
                        logging.info(
                            "Skipping destination-first sweep; "
                            "all %s destinations already complete for %s sweeps",
                            len(destinations),
                            len(sweep_batches),
                        )
                    else:
                        total_runs = sum(len(batch.runs) for batch in sweep_batches)
                        logging.info(
                            "Destination-first priority enabled "
                            "(%s destinations x %s sweeps => %s runs)",
                            len(destinations),
                            len(sweep_batches),
                            total_runs,
                        )
                        run_queue = _build_destination_first_queue(destinations, sweep_batches)
                        context = await _execute_destination_runs(
                            session,
                            context,
                            settings,
                            run_queue,
                            db_store=db_store,
                            existing_runs=existing_runs,
                        )
                else:
                    for batch in sweep_batches:
                        if not batch.runs:
                            continue
                        first_run = batch.runs[0]
                        nights = (first_run.params.check_out - first_run.params.check_in).days
                        if (
                            db_store
                            and settings.resume_completed_runs
                            and _batch_complete(batch, existing_runs)
                        ):
                            logging.info(
                                "Skipping sweep %s; all %s destinations already complete",
                                batch.label_text,
                                len(batch.runs),
                            )
                            continue
                        logging.info("Starting sweep %s (%s nights)", batch.label_text, nights)
                        context = await _execute_destination_runs(
                            session,
                            context,
                            settings,
                            batch.runs,
                            db_store=db_store,
                            existing_runs=existing_runs,
                        )
            finally:
                if context:
                    await ensure_close_context(context)
    finally:
        if db_store:
            await db_store.close()


async def _execute_destination_runs(
    session: BrowserSession,
    context: BrowserContext,
    settings: Settings,
    runs: list[_DestinationRun],
    *,
    db_store: SqliteStore | None,
    existing_runs: dict[tuple[str, str | None], SearchRunRecord],
) -> BrowserContext:
    if not runs:
        return context

    warmup_page = settings.search_warmup_enabled
    login_flow = LoginFlow(settings)
    page = await login_flow.run(context)
    try:
        await page.close()
    except Exception:
        pass

    client = SearchClient(context)
    consecutive_backend_failures = 0

    for scheduled in runs:
        destination = scheduled.destination
        params = scheduled.params
        label = scheduled.label
        nights = (params.check_out - params.check_in).days

        settings.search_check_in = params.check_in
        settings.search_nights = nights

        logging.info(
            "Starting search for destination %s (%s, %s)",
            destination.key,
            destination.group,
            destination.name,
        )

        existing_run = existing_runs.get((destination.key, label)) if existing_runs else None
        if existing_run:
            if existing_run.status == "complete":
                timestamp = existing_run.completed_at or existing_run.updated_at
                logging.info(
                    "Skipping %s; latest run (id=%s) finished at %s",
                    destination.key,
                    existing_run.id,
                    timestamp,
                )
                continue
            if existing_run.status == "failed":
                logging.info(
                    "Re-running %s; previous attempt (id=%s) failed%s",
                    destination.key,
                    existing_run.id,
                    f" ({existing_run.failure_reason})" if existing_run.failure_reason else "",
                )

        last_session_error: SessionRefreshError | None = None
        backend_failure: BackendUnavailableError | None = None
        results: dict[str, object] | None = None
        run_id: int | None = None
        try:
            for rebuild_attempt in range(2):
                try:
                    if db_store and run_id is None:
                        run_id = await db_store.begin_run(
                            destination=destination,
                            params=params,
                            label=label,
                        )
                    results = await client.fetch_properties(params, warmup_page=warmup_page)
                    last_session_error = None
                    backend_failure = None
                    break
                except TargetClosedError:
                    logging.warning(
                        "Browser session closed while fetching %s; restarting session",
                        destination.key,
                    )
                    await ensure_close_context(context)
                    context = await session.new_context()
                    login_flow = LoginFlow(settings)
                    page = await login_flow.run(context)
                    try:
                        await page.close()
                    except Exception:
                        pass
                    client = SearchClient(context)
                    continue
                except PatchrightError as exc:
                    message = str(exc).lower()
                    context_disposed = (
                        "context disposed" in message
                        or "browser has been closed" in message
                        or "target page" in message
                    )
                    if context_disposed:
                        logging.warning(
                            "Request context disposed while fetching %s; rebuilding session",
                            destination.key,
                        )
                        await ensure_close_context(context)
                        context = await session.new_context()
                        login_flow = LoginFlow(settings)
                        page = await login_flow.run(context)
                        try:
                            await page.close()
                        except Exception:
                            pass
                        client = SearchClient(context)
                        continue
                    raise
                except SessionRefreshError as exc:
                    last_session_error = exc
                    logging.warning(
                        "Session refresh failed for %s; rebuilding authentication (attempt %s)",
                        destination.key,
                        rebuild_attempt + 1,
                    )
                    await ensure_close_context(context)
                    context = await session.new_context()
                    login_flow = LoginFlow(settings)
                    page = await login_flow.run(context)
                    try:
                        await page.close()
                    except Exception:
                        pass
                    client = SearchClient(context)
                    continue
                except BackendUnavailableError as exc:
                    backend_failure = exc
                    logging.warning(
                        "Hotel properties API unavailable for %s (HTTP %s); skipping destination",
                        destination.key,
                        exc.status,
                    )
                    break
            if last_session_error is not None:
                raise RuntimeError(
                    f"Unable to recover session while fetching {destination.key}"
                ) from last_session_error

            if backend_failure is not None:
                reason = (
                    f"Properties API returned HTTP {backend_failure.status}: "
                    f"{backend_failure.body}"
                )
                if db_store and run_id is not None:
                    await db_store.mark_run_failed(run_id, reason)
                    run_id = None
                consecutive_backend_failures += 1
                if consecutive_backend_failures >= settings.max_consecutive_backend_failures:
                    raise RuntimeError(
                        "Aborting sweep after "
                        f"{consecutive_backend_failures} back-to-back API failures"
                    ) from backend_failure
                continue
            else:
                consecutive_backend_failures = 0

            assert results is not None
            hotels_payload = results.get("hotels", [])

            hotel_records, rate_records = build_hotel_and_rate_records(
                results, destination=destination, params=params
            )
            hotel_dicts = HotelRecord.from_iterable(hotel_records)
            rate_dicts = HotelRateRecord.from_iterable(rate_records)

            logging.info("Fetched %s hotels for %s", len(hotels_payload), destination.key)

            pause_s = max(0.0, settings.destination_pause_s)
            if pause_s:
                # configurable pause between destinations to mimic human pacing /
                # avoid burst traffic
                await asyncio.sleep(pause_s)

            if db_store and run_id is not None:
                context_obj = results.get("context")
                context_payload = context_obj if isinstance(context_obj, dict) else None
                request_id = context_payload.get("requestId") if context_payload else None
                await db_store.store_run_payload(run_id, results)
                await db_store.save_hotels(run_id, hotel_dicts)
                await db_store.save_rates(run_id, rate_dicts)
                await db_store.finalize_run(
                    run_id,
                    total_hotels=len(hotel_dicts),
                    total_rates=len(rate_dicts),
                    request_id=request_id,
                    context=context_payload,
                )
                run_id = None
        except Exception as exc:
            if db_store and run_id is not None:
                await db_store.mark_run_failed(run_id, str(exc))
            raise

    return context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Secure Scraper workflow")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Path to a TOML run configuration file "
            "(defaults to config/run_config.toml when present)"
        ),
    )
    parser.add_argument(
        "--no-config",
        action="store_true",
        help="Ignore config/run_config.toml even if it exists",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--headed",
        action="store_true",
        help="Force headed browser mode (overrides config/env)",
    )
    mode_group.add_argument(
        "--headless",
        action="store_true",
        help="Force headless browser mode (overrides config/env)",
    )
    parser.add_argument(
        "--override",
        action="append",
        metavar="KEY=VALUE",
        help="Override a Settings attribute (repeatable). Values accept JSON literals.",
    )
    return parser


def _decode_override(value: str) -> object:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        pass
    lower = value.strip().lower()
    if lower in {"true", "false"}:
        return lower == "true"
    try:
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _apply_overrides(settings: Settings, overrides: dict[str, object]) -> None:
    for key, raw in overrides.items():
        if not hasattr(settings, key):
            logging.getLogger(__name__).warning("Ignoring unknown override '%s'", key)
            continue
        setattr(settings, key, raw)
        logging.getLogger(__name__).info("Override: set %s=%r", key, raw)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings()
    config_path: Optional[Path] = None
    run_config: Optional[RunConfig] = None
    overrides: dict[str, object] = {}

    if not args.no_config:
        if args.config:
            config_path = args.config
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
        else:
            default_path = Path("config/run_config.toml")
            if default_path.exists():
                config_path = default_path

    if config_path:
        run_config = RunConfig.load(config_path)
        run_config.apply_to(settings, base_dir=config_path.parent)

    if args.override:
        for entry in args.override:
            if "=" not in entry:
                parser.error(f"Override must be in KEY=VALUE form (got '{entry}')")
            key, value = entry.split("=", 1)
            overrides[key.strip()] = _decode_override(value.strip())

    if args.headed:
        settings.headless = False
    elif args.headless:
        settings.headless = True

    if overrides:
        _apply_overrides(settings, overrides)

    if run_config and run_config.date_range:
        sweeps = run_config.date_sweeps()
    else:
        sweeps = []
    if not sweeps:
        default_check_in = settings.search_check_in or (date.today() + timedelta(days=14))
        settings.search_check_in = default_check_in
        sweeps = [DateSweep(check_in=default_check_in)]

    configure_logging(settings.log_level, Path("data/logs"))
    settings.ensure_directories()

    if run_config:
        suffix = f" ({run_config.title})" if run_config.title else ""
        logging.getLogger(__name__).info(
            "Loaded run profile '%s'%s from %s", run_config.profile, suffix, config_path
        )
        if run_config.notes:
            logging.getLogger(__name__).info("Profile notes: %s", run_config.notes)
    elif config_path is None:
        logging.getLogger(__name__).info(
            "Running with environment-based settings (no run_config applied)"
        )

    if args.headed:
        logging.getLogger(__name__).info("CLI override: running in headed mode")
    elif args.headless:
        logging.getLogger(__name__).info("CLI override: running in headless mode")

    if run_config and run_config.date_range:
        logging.getLogger(__name__).info(
            "Date range configured with %s iterations (start %s)",
            len(sweeps),
            sweeps[0].check_in.isoformat(),
        )

    asyncio.run(run(settings, sweeps))


if __name__ == "__main__":
    main()
