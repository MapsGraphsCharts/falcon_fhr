"""Entry point for manual runs."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, timedelta
from pathlib import Path

from secure_scraper.auth.login_flow import LoginFlow
from secure_scraper.config.settings import Settings
from secure_scraper.core.browser import BrowserSession, ensure_close_context
from secure_scraper.core.logging import configure_logging
from secure_scraper.hotels import (
    HotelRateRecord,
    HotelRecord,
    build_hotel_and_rate_records,
)
from secure_scraper.services.search_client import SearchClient
from secure_scraper.destinations.catalog import Destination, DestinationCatalog
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


async def run() -> None:
    settings = Settings()
    configure_logging(settings.log_level, Path("data/logs"))
    settings.ensure_directories()

    async with BrowserSession(settings) as session:
        context = await session.new_context()
        try:
            login_flow = LoginFlow(settings)
            page = await login_flow.run(context)

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
                )

                logging.info(
                    "Starting search for destination %s (%s, %s)",
                    destination.key,
                    destination.group,
                    destination.name,
                )

                results = await client.fetch_properties(params)
                hotels_payload = results.get("hotels", [])

                hotel_records, rate_records = build_hotel_and_rate_records(
                    results, destination=destination, params=params
                )
                hotel_dicts = HotelRecord.from_iterable(hotel_records)
                rate_dicts = HotelRateRecord.from_iterable(rate_records)

                destination_subdir = destination.key
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

            if aggregated_hotels:
                unique_records: dict[str, dict[str, object]] = {}
                for record in aggregated_hotels:
                    property_id = record.get("property_id")
                    if not property_id:
                        continue
                    unique_records[property_id] = record
                master_path = await downloader.run(
                    list(unique_records.values()), filename="master_hotels_normalized.json"
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
                    filename="master_rates_normalized.json",
                )
                logging.info(
                    "Aggregated master rates list (%s entries) saved to %s",
                    len(unique_rates),
                    master_rates_path,
                )

            await page.close()
        finally:
            await ensure_close_context(context)


if __name__ == "__main__":
    asyncio.run(run())
