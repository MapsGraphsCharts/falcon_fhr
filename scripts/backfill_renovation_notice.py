#!/usr/bin/env python
"""Backfill renovation/closure notices from stored hotel raw_json blobs."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from secure_scraper.hotels.normalizer import _normalize_notice


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/storage/hotels.sqlite3"),
        help="Path to the SQLite database (default: data/storage/hotels.sqlite3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the rows that would change without modifying the database.",
    )
    return parser.parse_args()


def backfill(db_path: Path, dry_run: bool = False) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT property_id, raw_json, renovation_closure_notice FROM hotels"
        )
        updates: list[tuple[str | None, str]] = []
        examined = 0
        changed = 0
        for property_id, raw_blob, existing in cursor:
            examined += 1
            if not raw_blob:
                continue
            notice = _normalize_notice(json.loads(raw_blob).get("renovationAndClosures"))
            normalized_existing = existing.strip() if isinstance(existing, str) else existing
            if notice == normalized_existing:
                continue
            changed += 1
            updates.append((notice, property_id))

        if not updates:
            print("No renovation/closure updates required.")
            return

        print(f"Prepared {changed} updates out of {examined} hotels.")
        if dry_run:
            for notice, property_id in updates[:10]:
                print(f"DRY-RUN {property_id}: {notice!r}")
            if len(updates) > 10:
                print(f"…and {len(updates) - 10} more")
            return

        with conn:
            conn.executemany(
                "UPDATE hotels SET renovation_closure_notice=? WHERE property_id=?",
                updates,
            )
        print(f"Applied {changed} updates.")
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    backfill(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
