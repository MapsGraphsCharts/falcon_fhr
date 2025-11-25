"""Utility CLI for inspecting and hydrating the destination catalog."""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from typing import Iterable, Sequence

from secure_scraper.config.settings import Settings
from secure_scraper.destinations.catalog import Destination, DestinationCatalog
from secure_scraper.services import LocationClient


def _normalise(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _format_destination(destination: Destination) -> str:
    missing = destination.missing_fields()
    status = "ready" if not missing else f"missing: {', '.join(missing)}"
    return f"{destination.key:35} | {destination.group:25} | {destination.name:40} | {status}"


def _print_table(destinations: Sequence[Destination]) -> None:
    for destination in destinations:
        print(_format_destination(destination))


def _destination_country_hint(destination: Destination) -> str:
    prefix = destination.key.split("-", 1)[0]
    return prefix.upper()


def _score_candidate(destination_entry: dict[str, object], candidate: dict[str, object]) -> int:
    name = str(destination_entry["name"])
    group = str(destination_entry.get("group", ""))
    key = str(destination_entry.get("key", ""))

    name_norm = _normalise(name)
    group_norm = _normalise(group)
    key_norm = _normalise(key.replace("-", " "))
    candidate_name = str(candidate.get("name", ""))
    candidate_norm = _normalise(candidate_name)

    score = 0
    if not candidate_norm:
        return score

    if candidate_norm == name_norm:
        score += 120
    elif candidate_norm.startswith(name_norm) and name_norm:
        score += 100
    elif name_norm and name_norm in candidate_norm:
        score += 80

    if group_norm and group_norm in candidate_norm:
        score += 15
    if key_norm and key_norm in candidate_norm:
        score += 10

    category = candidate.get("_category")
    if category == "regions":
        score += 30
    elif category == "cities":
        if "and vicinity" in candidate_name.lower():
            score += 15
        else:
            score += 5

    country_hint = destination_entry.get("_country_hint")
    country_code = str(candidate.get("countryCode") or candidate.get("country"))
    if country_hint and country_code.upper() == country_hint.upper():
        score += 10

    return score


def _choose_candidate(destination_entry: dict[str, object], payload: dict[str, object]) -> dict[str, object] | None:
    candidates: list[dict[str, object]] = []
    for candidate in LocationClient.iter_candidates(payload):
        if not candidate.get("geoLocation") or not candidate.get("id"):
            continue
        candidates.append(candidate)

    if not candidates:
        return None

    scored = sorted(
        ((candidate, _score_candidate(destination_entry, candidate)) for candidate in candidates),
        key=lambda item: item[1],
        reverse=True,
    )
    best_candidate, score = scored[0]
    return best_candidate if score > 0 else None


def _hydrate_destinations(
    entries: Iterable[dict[str, object]],
    *,
    limit: int | None,
    overwrite: bool,
    dry_run: bool,
) -> tuple[list[str], list[str]]:
    updates: list[str] = []
    misses: list[str] = []

    with LocationClient() as client:
        processed = 0
        for entry in entries:
            if limit is not None and processed >= limit:
                break

            location_id = entry.get("location_id")
            latitude = entry.get("latitude")
            longitude = entry.get("longitude")
            needs_update = (
                overwrite
                or not location_id
                or latitude in (None, "")
                or longitude in (None, "")
            )

            if not needs_update:
                continue

            query = str(entry["name"])
            payload = client.lookup_best(query)
            if not payload:
                misses.append(f"{entry['key']}: no response for query '{query}'")
                continue

            entry["_country_hint"] = _destination_country_hint(
                Destination(
                    key=str(entry["key"]),
                    group=str(entry.get("group", "")),
                    name=str(entry["name"]),
                )
            )

            candidate = _choose_candidate(entry, payload)
            if not candidate:
                misses.append(f"{entry['key']}: no candidate matched query '{query}'")
                continue

            geo = candidate.get("geoLocation") or {}
            new_location_id = candidate.get("id")
            new_latitude = geo.get("latitude")
            new_longitude = geo.get("longitude")

            if not (new_location_id and new_latitude is not None and new_longitude is not None):
                misses.append(f"{entry['key']}: candidate missing geo/location data")
                continue

            processed += 1
            updates.append(
                f"{entry['key']}: {location_id or '∅'} -> {new_location_id} "
                f"({new_latitude}, {new_longitude}) [{candidate.get('_category')}]"
            )

            if dry_run:
                continue

            entry["location_id"] = new_location_id
            entry["latitude"] = float(new_latitude)
            entry["longitude"] = float(new_longitude)

        for entry in entries:
            entry.pop("_country_hint", None)

    return updates, misses


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect configured Amex Travel destinations.")
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Only show destinations that are missing metadata required for searches.",
    )
    parser.add_argument(
        "--group-summary",
        action="store_true",
        help="Print a grouped summary (counts per region) instead of individual entries.",
    )
    parser.add_argument(
        "--hydrate-missing",
        action="store_true",
        help="Look up location IDs and coordinates for destinations missing metadata.",
    )
    parser.add_argument(
        "--hydrate-limit",
        type=int,
        default=None,
        help="Maximum number of destinations to hydrate (useful for spot checks).",
    )
    parser.add_argument(
        "--hydrate-overwrite",
        action="store_true",
        help="Overwrite existing metadata instead of only filling empty fields.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run hydration without writing updates to disk.",
    )
    args = parser.parse_args()

    settings = Settings()
    catalog_path = settings.destination_catalog_path
    catalog_data = json.loads(catalog_path.read_text())
    entries: list[dict[str, object]] = catalog_data.get("destinations", [])

    if args.hydrate_missing:
        updates, misses = _hydrate_destinations(
            entries,
            limit=args.hydrate_limit,
            overwrite=args.hydrate_overwrite,
            dry_run=args.dry_run,
        )
        if updates:
            print("Hydration updates:")
            for item in updates:
                print(f"  {item}")
        if misses:
            print("Hydration misses:")
            for item in misses:
                print(f"  {item}")
        if updates and not args.dry_run:
            catalog_path.write_text(json.dumps(catalog_data, indent=2))
            print(f"Catalog updated at {catalog_path}")

    catalog = DestinationCatalog.load(catalog_path)
    destinations = list(catalog.values())

    if args.missing:
        destinations = [destination for destination in destinations if not destination.is_ready()]

    if args.group_summary:
        counts: dict[str, int] = defaultdict(int)
        ready_counts: dict[str, int] = defaultdict(int)
        for destination in destinations:
            counts[destination.group] += 1
            if destination.is_ready():
                ready_counts[destination.group] += 1
        print(f"Catalog source: {catalog.source}")
        for group in sorted(counts):
            ready = ready_counts.get(group, 0)
            total = counts[group]
            print(f"{group:25} {ready:3}/{total:3} ready")
        return

    print(f"Catalog source: {catalog.source}")
    _print_table(sorted(destinations, key=lambda d: (d.group, d.name)))


if __name__ == "__main__":
    main()
