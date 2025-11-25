"""Room-type price volatility analysis for Japan FHR sweeps."""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Iterable

DEFAULT_DB = Path("data/storage/hotels.sqlite3")


def fetch_values(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    query = """
        SELECT h.name AS hotel,
               rt.name AS room_name,
               rs.pricing_total_inclusive AS total
        FROM rate_snapshots rs
        JOIN search_runs sr ON sr.id = rs.run_id
        JOIN hotels h ON h.property_id = rs.property_id
        JOIN room_types rt ON rt.property_id = rs.property_id
                           AND rt.room_type_id = rs.room_type_id
        WHERE sr.destination_name = 'Japan'
          AND sr.nights = 3
          AND sr.check_in BETWEEN '2025-12-01' AND '2026-02-28'
          AND rs.pricing_total_inclusive IS NOT NULL
          AND rs.hotel_collection = 'FHR'
    """
    return conn.execute(query).fetchall()


def summarize(rows: Iterable[sqlite3.Row]) -> list[tuple[float, float, float, float, int, str, str]]:
    values: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        key = (row["hotel"], row["room_name"])
        values[key].append(float(row["total"]))

    result: list[tuple[float, float, float, float, int, str, str]] = []
    for (hotel, room), totals in values.items():
        if len(totals) < 2:
            continue
        mn = min(totals)
        mx = max(totals)
        spread = mx - mn
        avg = sum(totals) / len(totals)
        result.append((spread, mn, mx, avg, len(totals), hotel, room))
    result.sort(reverse=True)
    return result


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def main(db_path: Path = DEFAULT_DB, limit: int = 50) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = fetch_values(conn)
    entries = summarize(rows)

    print("Hotel | Room Type | Spread | Min | Max | Avg | Samples")
    for spread, mn, mx, avg, freq, hotel, room in entries[:limit]:
        print(
            f"{hotel} | {room} | {format_currency(spread)} | "
            f"{format_currency(mn)} | {format_currency(mx)} | {format_currency(avg)} | {freq}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize 3-night price volatility per room type")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to hotels.sqlite3")
    parser.add_argument("--limit", type=int, default=50, help="How many rows to display")
    args = parser.parse_args()
    main(args.db, args.limit)
