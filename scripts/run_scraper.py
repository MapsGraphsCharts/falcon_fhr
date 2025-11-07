"""Entry point for manual runs."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
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
from secure_scraper.services.search_client import SearchClient, SessionRefreshError
from secure_scraper.tasks.download import DownloadTask
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams


def _compute_dates(settings: Settings) -> tuple[date, date]:
    check_in = settings.search_check_in or (date.today() + timedelta(days=14))
    check_out = check_in + timedelta(days=settings.search_nights)
    return check_in, check_out


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


async def run(settings: Settings, sweeps: list[DateSweep]) -> None:
    async with BrowserSession(settings) as session:
        context = await session.new_context()
        try:
            for sweep in sweeps:
                context = await _run_sweep(session, context, settings, sweep)
        finally:
            await ensure_close_context(context)


async def _run_sweep(
    session: BrowserSession,
    context: BrowserContext,
    settings: Settings,
    sweep: DateSweep,
) -> BrowserContext:
    if sweep.check_in:
        settings.search_check_in = sweep.check_in
    if sweep.nights is not None:
        settings.search_nights = sweep.nights
    label = sweep.label
    label_text = label or (settings.search_check_in.isoformat() if settings.search_check_in else "auto")
    logging.info("Starting sweep %s (%s nights)", label_text, settings.search_nights)

    login_flow = LoginFlow(settings)
    page = await login_flow.run(context)
    try:
        await page.close()
    except Exception:
        pass

    check_in, check_out = _compute_dates(settings)
    destinations = _resolve_destinations(settings)

    client = SearchClient(context)
    downloader = DownloadTask(settings.download_dir)

    aggregated_hotels: list[dict[str, object]] = []
    aggregated_rates: list[dict[str, object]] = []

    for destination in destinations:
        location_id = destination.location_id
        latitude = destination.latitude
        longitude = destination.longitude
        if location_id is None or latitude is None or longitude is None:  # pragma: no cover - defensive
            raise RuntimeError(f"Destination {destination.key} missing metadata despite readiness check")

        params = SearchParams(
            location_id=location_id,
            location_label=destination.name,
            latitude=latitude,
            longitude=longitude,
            check_in=check_in,
            check_out=check_out,
            rooms=[RoomRequest(adults=settings.search_adults)],
            program_filter=list(settings.search_program_filter) or None,
        )
        warmup_page = settings.search_warmup_enabled

        logging.info(
            "Starting search for destination %s (%s, %s)",
            destination.key,
            destination.group,
            destination.name,
        )

        last_error: Exception | None = None
        results: dict[str, object] | None = None
        for rebuild_attempt in range(2):
            try:
                results = await client.fetch_properties(params, warmup_page=warmup_page)
                last_error = None
                break
            except SessionRefreshError as exc:
                last_error = exc
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
        if last_error is not None:
            raise RuntimeError(
                f"Unable to recover session while fetching {destination.key}"
            ) from last_error

        assert results is not None
        hotels_payload = results.get("hotels", [])

        hotel_records, rate_records = build_hotel_and_rate_records(
            results, destination=destination, params=params
        )
        hotel_dicts = HotelRecord.from_iterable(hotel_records)
        rate_dicts = HotelRateRecord.from_iterable(rate_records)

        destination_subdir = destination.key
        if label:
            destination_subdir = f"{destination_subdir}/{label}"
        destination_dir = settings.download_dir / destination_subdir
        destination_dir.mkdir(parents=True, exist_ok=True)

        normalized_hotels_path = await downloader.run(
            hotel_dicts,
            filename="hotels_normalized.json",
            subdir=destination_subdir,
        )
        normalized_rates_path = await downloader.run(
            rate_dicts,
            filename="rates_normalized.json",
            subdir=destination_subdir,
        )
        raw_path = destination_dir / "hotels_raw.json"
        raw_path.write_text(json.dumps(results, indent=2))

        logging.info("Fetched %s hotels for %s", len(hotels_payload), destination.key)
        logging.info("Normalized hotels saved to %s", normalized_hotels_path)
        logging.info("Normalized rates saved to %s", normalized_rates_path)
        logging.info("Full payload saved to %s", raw_path)

        aggregated_hotels.extend(hotel_dicts)
        aggregated_rates.extend(rate_dicts)

        master_suffix = f"_{label}" if label else ""
        if aggregated_hotels:
            unique_records: dict[str, dict[str, object]] = {}
            for record in aggregated_hotels:
                property_id = record.get("property_id")
                if not property_id:
                    continue
                unique_records[property_id] = record
            master_path = await downloader.run(
                list(unique_records.values()), filename=f"master_hotels_normalized{master_suffix}.json"
            )
            logging.info(
                "Aggregated master list (%s properties) saved to %s",
                len(unique_records),
                master_path,
            )

        if aggregated_rates:
            unique_rates: dict[str, dict[str, object]] = {}
            for record in aggregated_rates:
                search = record.get("search") or {}
                property_id = record.get("property_id")
                if not property_id:
                    continue
                key = "|".join(
                    str(part) if part is not None else ""
                    for part in (
                        property_id,
                        record.get("rate_id"),
                        record.get("room_type_id"),
                        search.get("check_in"),
                        search.get("check_out"),
                    )
                )
                if key not in unique_rates:
                    unique_rates[key] = record
            master_rates_path = await downloader.run(
                list(unique_rates.values()),
                filename=f"master_rates_normalized{master_suffix}.json",
            )
            logging.info(
                "Aggregated master rates list (%s entries) saved to %s",
                len(unique_rates),
                master_rates_path,
            )
    return context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Secure Scraper workflow")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a TOML run configuration file (defaults to config/run_config.toml when present)",
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
        logging.getLogger(__name__).info("Running with environment-based settings (no run_config applied)")

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
