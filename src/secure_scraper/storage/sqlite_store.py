"""SQLite-backed persistence for hotel metadata and rate snapshots."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from dataclasses import dataclass

from secure_scraper.destinations.catalog import Destination
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SCHEMA_VERSION = 5

VALID_JOURNAL_MODES = frozenset({"delete", "truncate", "persist", "memory", "wal", "off"})
VALID_SYNCHRONOUS_MODES = frozenset({"off", "normal", "full", "extra"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FORMAT)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def _maybe_json(value: Any) -> str | None:
    return _json_dumps(value) if value is not None else None


def _bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _sum_adults(rooms: Sequence[RoomRequest]) -> int:
    return sum(getattr(room, "adults", 0) for room in rooms)


def _sum_children(rooms: Sequence[RoomRequest]) -> int:
    total = 0
    for room in rooms:
        children = getattr(room, "children", [])
        total += len(children or [])
    return total


def _extract_primary_description(entries: Sequence[dict[str, Any]] | None) -> tuple[str | None, str | None]:
    if not entries:
        return None, None
    preferred = None
    for entry in entries:
        locale = (entry.get("locale") or "").lower()
        if locale.startswith("en"):
            preferred = entry
            break
    if preferred is None:
        preferred = entries[0]
    return preferred.get("title"), preferred.get("description")


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


@dataclass(frozen=True)
class SearchRunRecord:
    """Lightweight view of a search run row."""

    id: int
    destination_key: str
    destination_name: str | None
    destination_group: str | None
    label: str | None
    status: str
    started_at: str
    updated_at: str
    completed_at: str | None
    failure_reason: str | None
    total_hotels: int
    total_rates: int
    search_signature: str


logger = logging.getLogger(__name__)


class SqliteStore:
    """Thin async wrapper over sqlite3 for structured persistence."""

    _SQLITE_PARAMETER_LIMIT = 900

    def __init__(
        self,
        db_path: Path,
        *,
        busy_timeout_ms: int = 2000,
        journal_mode: str | None = "wal",
        synchronous: str | None = "normal",
    ) -> None:
        self._path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._journal_mode = self._normalize_journal_mode(journal_mode)
        self._synchronous = self._normalize_synchronous(synchronous)
        self._connection: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # lifecycle

    async def initialize(self) -> None:
        if self._connection is not None:
            return
        async with self._lock:
            if self._connection is None:
                conn = await asyncio.to_thread(self._open_connection)
                self._connection = conn

    async def close(self) -> None:
        if self._connection is None:
            return
        conn = self._connection
        self._connection = None
        await asyncio.to_thread(conn.close)

    def _open_connection(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute(f"PRAGMA busy_timeout = {int(max(self._busy_timeout_ms, 0))};")
        if self._journal_mode:
            conn.execute(f"PRAGMA journal_mode = {self._journal_mode.upper()};")
        if self._synchronous:
            conn.execute(f"PRAGMA synchronous = {self._synchronous.upper()};")
        try:
            self._apply_migrations(conn)
        except sqlite3.OperationalError as exc:
            conn.close()
            logger.error(
                "SQLite migration failed (path=%s, timeout_ms=%s): %s",
                self._path,
                self._busy_timeout_ms,
                exc,
            )
            raise
        return conn

    @staticmethod
    def _normalize_journal_mode(value: str | None) -> str | None:
        if value is None:
            return None
        mode = value.strip().lower()
        if not mode:
            return None
        if mode not in VALID_JOURNAL_MODES:
            raise ValueError(
                f"Unsupported SQLite journal_mode '{value}'. Expected one of: {sorted(VALID_JOURNAL_MODES)}"
            )
        return mode

    @staticmethod
    def _normalize_synchronous(value: str | None) -> str | None:
        if value is None:
            return None
        mode = value.strip().lower()
        if not mode:
            return None
        if mode not in VALID_SYNCHRONOUS_MODES:
            raise ValueError(
                f"Unsupported SQLite synchronous mode '{value}'. Expected one of: {sorted(VALID_SYNCHRONOUS_MODES)}"
            )
        return mode

    def _apply_migrations(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        current = self._get_schema_version(conn)
        if current >= SCHEMA_VERSION:
            return
        for version in range(current + 1, SCHEMA_VERSION + 1):
            script = MIGRATIONS.get(version)
            if not script:
                raise RuntimeError(f"Missing migration script for version {version}")
            conn.executescript(script)
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)\n"
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(version),),
            )
        conn.commit()

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        cursor = conn.execute("SELECT value FROM meta WHERE key='schema_version'")
        row = cursor.fetchone()
        if not row:
            return 0
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return 0

    def _require_connection(self) -> sqlite3.Connection:
        if not self._connection:
            raise RuntimeError("SQLite store has not been initialised")
        return self._connection

    def _row_to_search_run(self, row: Sequence[Any]) -> SearchRunRecord:
        return SearchRunRecord(
            id=int(row[0]),
            destination_key=row[1],
            destination_name=row[2],
            destination_group=row[3],
            label=row[4],
            status=row[5],
            started_at=row[6],
            updated_at=row[7],
            completed_at=row[8],
            failure_reason=row[9],
            total_hotels=int(row[10] or 0),
            total_rates=int(row[11] or 0),
            search_signature=row[12],
        )

    # ------------------------------------------------------------------
    # run orchestration

    async def begin_run(
        self,
        *,
        destination: Destination,
        params: SearchParams,
        label: str | None,
    ) -> int:
        """Record a new destination/date run and return its identifier."""

        def _op() -> int:
            conn = self._require_connection()
            now = _utc_now()
            programs = list((params.program_filter or []))
            signature = self._signature(destination.key, label, params, programs)
            with conn:
                conn.execute(
                    """
                    INSERT INTO destinations(key, group_name, name, location_id, latitude, longitude, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        group_name=excluded.group_name,
                        name=excluded.name,
                        location_id=excluded.location_id,
                        latitude=excluded.latitude,
                        longitude=excluded.longitude,
                        updated_at=excluded.updated_at
                    """,
                    (
                        destination.key,
                        destination.group,
                        destination.name,
                        destination.location_id,
                        destination.latitude,
                        destination.longitude,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    UPDATE search_runs
                    SET status='failed', failure_reason='Superseded by new run', updated_at=?
                    WHERE search_signature=? AND status='running'
                    """,
                    (now, signature),
                )
                cursor = conn.execute(
                    """
                    INSERT INTO search_runs(
                        destination_key,
                        destination_group,
                        destination_name,
                        label,
                        check_in,
                        check_out,
                        nights,
                        adults,
                        children,
                        rooms,
                        program_filter,
                        status,
                        started_at,
                        created_at,
                        updated_at,
                        search_signature
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)
                    """,
                    (
                        destination.key,
                        destination.group,
                        destination.name,
                        label,
                        params.check_in.isoformat(),
                        params.check_out.isoformat(),
                        (params.check_out - params.check_in).days,
                        _sum_adults(params.rooms),
                        _sum_children(params.rooms),
                        len(params.rooms),
                        _json_dumps(programs),
                        now,
                        now,
                        now,
                        signature,
                    ),
                )
                return int(cursor.lastrowid)

        async with self._lock:
            return await asyncio.to_thread(_op)

    async def finalize_run(
        self,
        run_id: int,
        *,
        total_hotels: int,
        total_rates: int,
        request_id: str | None,
        context: dict[str, Any] | None,
    ) -> None:
        def _op() -> None:
            conn = self._require_connection()
            now = _utc_now()
            context_json = _json_dumps(context) if context else None
            with conn:
                conn.execute(
                    """
                    UPDATE search_runs
                    SET status='complete',
                        completed_at=?,
                        updated_at=?,
                        total_hotels=?,
                        total_rates=?,
                        request_id=?,
                        raw_context=?
                    WHERE id=?
                    """,
                    (now, now, total_hotels, total_rates, request_id, context_json, run_id),
                )

        async with self._lock:
            await asyncio.to_thread(_op)

    async def fetch_latest_run(
        self,
        *,
        destination: Destination,
        params: SearchParams,
        label: str | None,
    ) -> SearchRunRecord | None:
        signature = self._signature(destination.key, label, params, list(params.program_filter or []))

        def _op() -> SearchRunRecord | None:
            conn = self._require_connection()
            cursor = conn.execute(
                """
                SELECT id, destination_key, destination_name, destination_group, label, status,
                       started_at, updated_at, completed_at, failure_reason, total_hotels,
                       total_rates, search_signature
                FROM search_runs
                WHERE search_signature=?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (signature,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_search_run(row)

        async with self._lock:
            return await asyncio.to_thread(_op)

    async def fetch_latest_runs_bulk(
        self,
        runs: Sequence[tuple[Destination, SearchParams]],
        *,
        label: str | None,
    ) -> dict[str, SearchRunRecord]:
        """Fetch the latest run record for multiple destinations in one query."""

        if not runs:
            return {}

        signatures: list[str] = []
        signature_to_key: dict[str, str] = {}
        for destination, params in runs:
            programs = list(params.program_filter or [])
            signature = self._signature(destination.key, label, params, programs)
            signatures.append(signature)
            signature_to_key.setdefault(signature, destination.key)

        # Deduplicate while preserving order to keep parameter counts low.
        ordered_unique_signatures = list(dict.fromkeys(signatures))

        def _op() -> dict[str, SearchRunRecord]:
            conn = self._require_connection()
            records: dict[str, SearchRunRecord] = {}

            def _query_chunk(chunk: Sequence[str]) -> None:
                placeholders = ",".join("?" for _ in chunk)
                cursor = conn.execute(
                    f"""
                    SELECT sr.id, sr.destination_key, sr.destination_name, sr.destination_group,
                           sr.label, sr.status, sr.started_at, sr.updated_at, sr.completed_at,
                           sr.failure_reason, sr.total_hotels, sr.total_rates, sr.search_signature
                    FROM search_runs sr
                    JOIN (
                        SELECT search_signature, MAX(started_at) AS max_started_at
                        FROM search_runs
                        WHERE search_signature IN ({placeholders})
                        GROUP BY search_signature
                    ) latest ON sr.search_signature = latest.search_signature
                            AND sr.started_at = latest.max_started_at
                    """,
                    chunk,
                )
                for row in cursor.fetchall():
                    record = self._row_to_search_run(row)
                    key = signature_to_key.get(record.search_signature)
                    if key and key not in records:
                        records[key] = record

            chunk: list[str] = []
            for signature in ordered_unique_signatures:
                chunk.append(signature)
                if len(chunk) >= self._SQLITE_PARAMETER_LIMIT:
                    _query_chunk(tuple(chunk))
                    chunk.clear()
            if chunk:
                _query_chunk(tuple(chunk))
            return records

        async with self._lock:
            return await asyncio.to_thread(_op)

    async def mark_run_failed(self, run_id: int, reason: str) -> None:
        def _op() -> None:
            conn = self._require_connection()
            now = _utc_now()
            with conn:
                conn.execute(
                    """
                    UPDATE search_runs
                    SET status='failed', completed_at=?, updated_at=?, failure_reason=?
                    WHERE id=?
                    """,
                    (now, now, reason[:512], run_id),
                )

        async with self._lock:
            await asyncio.to_thread(_op)

    # ------------------------------------------------------------------
    # payload storage

    async def store_run_payload(self, run_id: int, payload: dict[str, Any]) -> None:
        def _op() -> None:
            conn = self._require_connection()
            if not payload:
                return
            context = payload.get("context") if isinstance(payload.get("context"), dict) else None
            request_id = context.get("requestId") if context else None
            context_blob = _json_dumps(context) if context else None
            now = _utc_now()
            with conn:
                conn.execute(
                    """
                    UPDATE search_runs SET request_id=?, raw_context=?, updated_at=? WHERE id=?
                    """,
                    (_safe_str(request_id), context_blob, now, run_id),
                )

        async with self._lock:
            await asyncio.to_thread(_op)

    # ------------------------------------------------------------------
    # hotel persistence

    async def save_hotels(self, run_id: int, records: Iterable[dict[str, Any]]) -> None:
        def _op() -> None:
            conn = self._require_connection()
            now = _utc_now()
            with conn:
                for record in records:
                    property_id = record.get("property_id")
                    if not property_id:
                        continue
                    summary: dict[str, Any] = record.get("summary") or {}
                    search_ctx = record.get("search")
                    raw = record.get("raw")
                    conn.execute(
                        """
                        INSERT INTO hotels(
                            property_id,
                            supplier_id,
                            name,
                            type,
                            brand_name,
                            chain_name,
                            star_rating,
                            phone,
                            address_line1,
                            address_city,
                            address_state,
                            address_postal_code,
                            address_country_code,
                            address_country_name,
                            latitude,
                            longitude,
                            distance_miles,
                            distance_unit,
                            loyalty_valid,
                            user_rating,
                            user_rating_count,
                            hero_image,
                            marketing_insider_tip,
                            marketing_video,
                            location_teaser,
                            renovation_closure_notice,
                            check_in_start,
                            check_in_end,
                            check_out_time,
                            summary_json,
                            search_context_json,
                            raw_json,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(property_id) DO UPDATE SET
                            supplier_id=excluded.supplier_id,
                            name=excluded.name,
                            type=excluded.type,
                            brand_name=excluded.brand_name,
                            chain_name=excluded.chain_name,
                            star_rating=excluded.star_rating,
                            phone=excluded.phone,
                            address_line1=excluded.address_line1,
                            address_city=excluded.address_city,
                            address_state=excluded.address_state,
                            address_postal_code=excluded.address_postal_code,
                            address_country_code=excluded.address_country_code,
                            address_country_name=excluded.address_country_name,
                            latitude=excluded.latitude,
                            longitude=excluded.longitude,
                            distance_miles=excluded.distance_miles,
                            distance_unit=excluded.distance_unit,
                            loyalty_valid=excluded.loyalty_valid,
                            user_rating=excluded.user_rating,
                            user_rating_count=excluded.user_rating_count,
                            hero_image=excluded.hero_image,
                            marketing_insider_tip=excluded.marketing_insider_tip,
                            marketing_video=excluded.marketing_video,
                            location_teaser=excluded.location_teaser,
                            renovation_closure_notice=excluded.renovation_closure_notice,
                            check_in_start=excluded.check_in_start,
                            check_in_end=excluded.check_in_end,
                            check_out_time=excluded.check_out_time,
                            summary_json=excluded.summary_json,
                            search_context_json=excluded.search_context_json,
                            raw_json=excluded.raw_json,
                            updated_at=excluded.updated_at
                        """,
                        (
                            property_id,
                            record.get("supplier_id"),
                            summary.get("name"),
                            summary.get("type"),
                            summary.get("brand_name"),
                            summary.get("chain_name"),
                            summary.get("star_rating"),
                            summary.get("phone"),
                            summary.get("address_line1"),
                            summary.get("address_city"),
                            summary.get("address_state"),
                            summary.get("address_postal_code"),
                            summary.get("address_country_code"),
                            summary.get("address_country_name"),
                            summary.get("latitude"),
                            summary.get("longitude"),
                            summary.get("distance_miles"),
                            summary.get("distance_unit"),
                            _bool(summary.get("loyalty_valid")),
                            summary.get("user_rating"),
                            summary.get("user_rating_count"),
                            summary.get("hero_image"),
                            summary.get("marketing_insider_tip"),
                            summary.get("marketing_video"),
                            summary.get("location_teaser"),
                            summary.get("renovation_closure_notice"),
                            summary.get("check_in_start"),
                            summary.get("check_in_end"),
                            summary.get("check_out_time"),
                            _json_dumps(summary),
                            _maybe_json(search_ctx),
                            _maybe_json(raw),
                            now,
                            now,
                        ),
                    )
                    self._upsert_hotel_features(conn, property_id, summary)
                    self._upsert_program_benefits(conn, property_id, summary.get("program_benefits") or [])
        async with self._lock:
            await asyncio.to_thread(_op)

    def _upsert_hotel_features(self, conn: sqlite3.Connection, property_id: str, summary: dict[str, Any]) -> None:
        buckets = {
            "interest": summary.get("interests") or [],
            "amenity": summary.get("amenities") or [],
            "program": summary.get("program_codes") or [],
            "marketing_tag": summary.get("marketing_tags") or [],
            "host_language": summary.get("host_languages") or [],
            "payment_option": summary.get("payment_options") or [],
            "policy": summary.get("policies") or [],
            "supplier_fee": summary.get("supplier_fees") or [],
        }
        conn.execute("DELETE FROM hotel_features WHERE property_id=?", (property_id,))
        rows = []
        for feature_type, values in buckets.items():
            for value in values:
                if not value:
                    continue
                rows.append((property_id, feature_type, str(value)))
        if rows:
            conn.executemany(
                "INSERT OR IGNORE INTO hotel_features(property_id, feature_type, value) VALUES(?, ?, ?)",
                rows,
            )

    def _upsert_program_benefits(
        self, conn: sqlite3.Connection, property_id: str, benefits: list[dict[str, Any]]
    ) -> None:
        conn.execute("DELETE FROM hotel_program_benefits WHERE property_id=?", (property_id,))
        rows = []
        for benefit in benefits:
            rows.append(
                (
                    property_id,
                    benefit.get("program_code"),
                    benefit.get("program_name"),
                    benefit.get("benefit_type"),
                    benefit.get("description"),
                    benefit.get("note"),
                    benefit.get("start_date"),
                    benefit.get("end_date"),
                    _bool(benefit.get("exceptional_value")),
                )
            )
        if rows:
            conn.executemany(
                """
                INSERT INTO hotel_program_benefits(
                    property_id,
                    program_code,
                    program_name,
                    benefit_type,
                    description,
                    note,
                    start_date,
                    end_date,
                    exceptional_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    # ------------------------------------------------------------------
    # rates persistence

    async def save_rates(self, run_id: int, records: Iterable[dict[str, Any]]) -> None:
        def _op() -> None:
            conn = self._require_connection()
            now = _utc_now()
            room_types: dict[tuple[str, str], dict[str, Any]] = {}
            rate_entries: list[tuple[tuple[Any, ...], list[tuple[Any, ...]], list[tuple[Any, ...]]]] = []
            promotions: dict[tuple[str, str], dict[str, Any]] = {}
            seen_snapshots: set[tuple[str, str, str]] = set()

            for record in records:
                property_id = record.get("property_id")
                if not property_id:
                    continue
                summary: dict[str, Any] = record.get("summary") or {}
                pricing: dict[str, Any] = summary.get("pricing") or {}
                room_type_id = self._resolve_room_type_id(record)
                key = (property_id, room_type_id)
                entry = room_types.setdefault(
                    key,
                    {
                        "name": record.get("room_type_name"),
                        "amenities": set(),
                        "bed_groups": summary.get("bed_groups") or [],
                        "raw": record.get("raw"),
                    },
                )
                entry["amenities"].update(summary.get("amenities") or [])
                if summary.get("bed_groups"):
                    entry["bed_groups"] = summary.get("bed_groups")
                if record.get("raw") and not entry.get("raw"):
                    entry["raw"] = record.get("raw")

                search_ctx = record.get("search") or {}
                rate_identifier = self._resolve_rate_id(record, room_type_id)
                rate_identifier = self._resolve_rate_id(record, room_type_id)
                rate_key = (property_id, room_type_id, rate_identifier)
                if rate_key in seen_snapshots:
                    continue
                seen_snapshots.add(rate_key)

                values = (
                    run_id,
                    property_id,
                    room_type_id,
                    rate_identifier,
                    summary.get("hotel_collection"),
                    summary.get("available"),
                    _bool(summary.get("is_breakfast_included")),
                    _bool(summary.get("is_food_beverage_credit")),
                    _bool(summary.get("is_free_cancellation")),
                    _bool(summary.get("is_parking_included")),
                    _bool(summary.get("is_shuttle_included")),
                    record.get("occupancy_adults"),
                    record.get("occupancy_children"),
                    record.get("room_count"),
                    pricing.get("currency"),
                    pricing.get("base"),
                    pricing.get("total"),
                    pricing.get("total_inclusive"),
                    pricing.get("total_fees"),
                    pricing.get("total_taxes"),
                    pricing.get("average_nightly_rate"),
                    pricing.get("average_nightly_rate_points_burn"),
                    pricing.get("payment_model"),
                    pricing.get("points_burn"),
                    _maybe_json(pricing.get("points_burn_calculation")),
                    _maybe_json(summary.get("room_allocations")),
                    _maybe_json(record.get("special_offer")),
                    _maybe_json(record.get("supplier_rate_promotion")),
                    _maybe_json(record.get("comparison_amenity")),
                    _json_dumps(search_ctx),
                    now,
                )

                nightly_rows = self._build_nightly_rows(
                    pricing.get("nightly_actual_rates") or [],
                    pricing.get("nightly_inclusive_rates") or [],
                    search_ctx,
                )
                component_rows = []
                component_rows.extend(self._build_component_rows(pricing.get("fees") or [], "fee"))
                component_rows.extend(self._build_component_rows(pricing.get("taxes") or [], "tax"))
                rate_entries.append((values, nightly_rows, component_rows))

                special_offer = record.get("special_offer")
                if isinstance(special_offer, dict):
                    promotion_code = special_offer.get("promotionCode")
                    if promotion_code:
                        key = (property_id, promotion_code)
                        if key not in promotions:
                            title_text, description_text = _extract_primary_description(
                                special_offer.get("descriptions")
                            )
                            promotions[key] = {
                                "property_id": property_id,
                                "promotion_code": promotion_code,
                                "promotion_type": special_offer.get("type"),
                                "title": special_offer.get("title") or title_text,
                                "description": description_text,
                                "min_nights": special_offer.get("minNights"),
                                "max_nights": special_offer.get("maxNights"),
                                "booking_start": special_offer.get("bookingStartDate"),
                                "booking_end": special_offer.get("bookingEndDate"),
                                "stay_start": special_offer.get("stayStartDate"),
                                "stay_end": special_offer.get("stayEndDate"),
                                "blackout_dates": special_offer.get("blackoutDates"),
                                "card_types": special_offer.get("cardTypes"),
                                "raw": special_offer,
                            }

            with conn:
                for (property_id, room_type_id), meta in room_types.items():
                    conn.execute(
                        """
                        INSERT INTO room_types(
                            property_id,
                            room_type_id,
                            name,
                            amenities_json,
                            bed_groups_json,
                            raw_json,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(property_id, room_type_id) DO UPDATE SET
                            name=excluded.name,
                            amenities_json=excluded.amenities_json,
                            bed_groups_json=excluded.bed_groups_json,
                            raw_json=excluded.raw_json,
                            updated_at=excluded.updated_at
                        """,
                        (
                            property_id,
                            room_type_id,
                            meta.get("name"),
                            _json_dumps(sorted(meta.get("amenities") or [])),
                            _json_dumps(meta.get("bed_groups")),
                            _json_dumps(meta.get("raw")),
                            now,
                            now,
                        ),
                    )

                # Replace snapshots for this execution before inserting fresh rows.
                conn.execute("DELETE FROM rate_snapshots WHERE run_id=?", (run_id,))
                for values, nightly_rows, component_rows in rate_entries:
                    placeholder_clause = ", ".join(["?"] * len(values))
                    cursor = conn.execute(
                        """
                        INSERT INTO rate_snapshots(
                            run_id,
                            property_id,
                            room_type_id,
                            rate_id,
                            hotel_collection,
                            available,
                            is_breakfast_included,
                            is_food_beverage_credit,
                            is_free_cancellation,
                            is_parking_included,
                            is_shuttle_included,
                            occupancy_adults,
                            occupancy_children,
                            room_count,
                            pricing_currency,
                            pricing_base,
                            pricing_total,
                            pricing_total_inclusive,
                            pricing_total_fees,
                            pricing_total_taxes,
                            average_nightly_rate,
                            average_nightly_rate_points_burn,
                            payment_model,
                            points_burn,
                            points_burn_calculation_json,
                            room_allocations_json,
                            special_offer_json,
                            supplier_rate_promotion_json,
                            comparison_amenity_json,
                            search_context_json,
                            created_at
                        ) VALUES (PLACEHOLDERS)
                        """.replace("PLACEHOLDERS", placeholder_clause),
                        values,
                    )
                    snapshot_id = cursor.lastrowid
                    if nightly_rows:
                        conn.executemany(
                            """
                            INSERT INTO rate_nightly_prices(
                                rate_snapshot_id,
                                night_index,
                                night_date,
                                actual_rate,
                                inclusive_rate
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            [(snapshot_id, *row) for row in nightly_rows],
                        )
                    if component_rows:
                        conn.executemany(
                            """
                            INSERT INTO rate_components(
                                rate_snapshot_id,
                                component_type,
                                code,
                                label,
                                amount,
                                currency,
                                is_included,
                                pay_locally,
                                details_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [(snapshot_id, *row) for row in component_rows],
                        )

                self._upsert_promotions(conn, promotions.values(), now)

        async with self._lock:
            await asyncio.to_thread(_op)

    def _build_nightly_rows(
        self,
        nightly_actual: Sequence[float],
        nightly_inclusive: Sequence[float],
        search_ctx: dict[str, Any],
    ) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        check_in = _parse_date(search_ctx.get("check_in"))
        if nightly_actual or nightly_inclusive:
            for idx, (actual, inclusive) in enumerate(_zip_longest(nightly_actual, nightly_inclusive)):
                night_date = (check_in + timedelta(days=idx)).isoformat() if check_in else None
                rows.append((idx, night_date, actual, inclusive))
        return rows

    def _build_component_rows(self, components: Sequence[dict[str, Any]], kind: str) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        for component in components:
            included = component.get("isIncluded")
            if included is None:
                included = component.get("is_included")
            pay_locally = component.get("payLocally")
            if pay_locally is None:
                pay_locally = component.get("pay_locally")
            rows.append(
                (
                    kind,
                    component.get("type"),
                    component.get("description")
                    or component.get("label")
                    or component.get("name"),
                    component.get("value") or component.get("amount"),
                    component.get("currency"),
                    _bool(included),
                    _bool(pay_locally),
                    _json_dumps(component),
                )
                )
        return rows

    def _upsert_promotions(
        self,
        conn: sqlite3.Connection,
        promotions: Iterable[dict[str, Any]],
        timestamp: str,
    ) -> None:
        promo_rows: list[tuple[Any, ...]] = []
        for promo in promotions:
            promo_rows.append(
                (
                    promo.get("property_id"),
                    promo.get("promotion_code"),
                    promo.get("promotion_type"),
                    promo.get("title"),
                    promo.get("description"),
                    promo.get("min_nights"),
                    promo.get("max_nights"),
                    promo.get("booking_start"),
                    promo.get("booking_end"),
                    promo.get("stay_start"),
                    promo.get("stay_end"),
                    _maybe_json(promo.get("blackout_dates")),
                    _maybe_json(promo.get("card_types")),
                    _json_dumps(promo.get("raw")),
                    timestamp,
                    timestamp,
                )
            )
        if not promo_rows:
            return
        conn.executemany(
            """
            INSERT INTO hotel_promotions(
                property_id,
                promotion_code,
                promotion_type,
                title,
                description,
                min_nights,
                max_nights,
                booking_start,
                booking_end,
                stay_start,
                stay_end,
                blackout_dates_json,
                card_types_json,
                raw_json,
                first_seen,
                last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(property_id, promotion_code) DO UPDATE SET
                promotion_type=excluded.promotion_type,
                title=COALESCE(excluded.title, hotel_promotions.title),
                description=COALESCE(excluded.description, hotel_promotions.description),
                min_nights=excluded.min_nights,
                max_nights=excluded.max_nights,
                booking_start=excluded.booking_start,
                booking_end=excluded.booking_end,
                stay_start=excluded.stay_start,
                stay_end=excluded.stay_end,
                blackout_dates_json=excluded.blackout_dates_json,
                card_types_json=excluded.card_types_json,
                raw_json=excluded.raw_json,
                last_seen=excluded.last_seen
            """,
            promo_rows,
        )


    def _resolve_room_type_id(self, record: dict[str, Any]) -> str:
        room_type_id = record.get("room_type_id")
        if room_type_id:
            return str(room_type_id)
        payload = f"{record.get('property_id','')}|{record.get('room_type_name','')}|{json.dumps(record.get('summary') or {}, sort_keys=True)}"
        return f"anon_{hashlib.sha1(payload.encode()).hexdigest()[:12]}"

    def _resolve_rate_id(self, record: dict[str, Any], room_type_id: str) -> str:
        rate_id = record.get("rate_id")
        if rate_id:
            return str(rate_id)
        payload = json.dumps(
            {
                "property_id": record.get("property_id"),
                "room_type_id": room_type_id,
                "summary": record.get("summary") or {},
            },
            sort_keys=True,
            default=str,
        )
        digest = hashlib.sha1(payload.encode()).hexdigest()[:12]
        return f"rate_{digest}"

    def _signature(self, destination_key: str, label: str | None, params: SearchParams, programs: list[str]) -> str:
        payload = "|".join(
            (
                destination_key,
                label or "",
                params.check_in.isoformat(),
                params.check_out.isoformat(),
                str(len(params.rooms)),
                str(_sum_adults(params.rooms)),
                ",".join(sorted(programs)),
            )
        )
        return hashlib.sha1(payload.encode()).hexdigest()


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _zip_longest(left: Sequence[Any], right: Sequence[Any]) -> list[tuple[Any, Any]]:
    length = max(len(left), len(right))
    rows: list[tuple[Any, Any]] = []
    for idx in range(length):
        a = left[idx] if idx < len(left) else None
        b = right[idx] if idx < len(right) else None
        rows.append((a, b))
    return rows


MIGRATIONS: dict[int, str] = {
    1: """
        CREATE TABLE IF NOT EXISTS destinations (
            key TEXT PRIMARY KEY,
            group_name TEXT,
            name TEXT NOT NULL,
            location_id TEXT,
            latitude REAL,
            longitude REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS search_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            destination_key TEXT NOT NULL REFERENCES destinations(key),
            destination_group TEXT,
            destination_name TEXT,
            label TEXT,
            check_in TEXT NOT NULL,
            check_out TEXT NOT NULL,
            nights INTEGER NOT NULL,
            adults INTEGER NOT NULL,
            children INTEGER NOT NULL,
            rooms INTEGER NOT NULL,
            program_filter TEXT,
            request_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            failure_reason TEXT,
            total_hotels INTEGER DEFAULT 0,
            total_rates INTEGER DEFAULT 0,
            search_signature TEXT NOT NULL,
            raw_context TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_search_runs_signature ON search_runs(search_signature);
        CREATE INDEX IF NOT EXISTS idx_search_runs_status ON search_runs(status);

        CREATE TABLE IF NOT EXISTS search_payloads (
            run_id INTEGER PRIMARY KEY REFERENCES search_runs(id) ON DELETE CASCADE,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hotels (
            property_id TEXT PRIMARY KEY,
            supplier_id TEXT,
            name TEXT,
            type TEXT,
            brand_name TEXT,
            chain_name TEXT,
            star_rating REAL,
            phone TEXT,
            address_line1 TEXT,
            address_city TEXT,
            address_state TEXT,
            address_postal_code TEXT,
            address_country_code TEXT,
            address_country_name TEXT,
            latitude REAL,
            longitude REAL,
            distance_miles REAL,
            distance_unit TEXT,
            loyalty_valid INTEGER,
            user_rating REAL,
            user_rating_count INTEGER,
            hero_image TEXT,
            marketing_insider_tip TEXT,
            marketing_video TEXT,
            location_teaser TEXT,
            check_in_start TEXT,
            check_in_end TEXT,
            check_out_time TEXT,
            summary_json TEXT,
            search_context_json TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hotel_features (
            property_id TEXT NOT NULL REFERENCES hotels(property_id) ON DELETE CASCADE,
            feature_type TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (property_id, feature_type, value)
        );
        CREATE INDEX IF NOT EXISTS idx_hotel_features_value ON hotel_features(feature_type, value);

        CREATE TABLE IF NOT EXISTS hotel_program_benefits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id TEXT NOT NULL REFERENCES hotels(property_id) ON DELETE CASCADE,
            program_code TEXT,
            program_name TEXT,
            benefit_type TEXT,
            description TEXT,
            note TEXT,
            start_date TEXT,
            end_date TEXT,
            exceptional_value INTEGER
        );

        CREATE TABLE IF NOT EXISTS room_types (
            property_id TEXT NOT NULL REFERENCES hotels(property_id) ON DELETE CASCADE,
            room_type_id TEXT NOT NULL,
            name TEXT,
            amenities_json TEXT,
            bed_groups_json TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (property_id, room_type_id)
        );

        CREATE TABLE IF NOT EXISTS rate_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES search_runs(id) ON DELETE CASCADE,
            property_id TEXT NOT NULL REFERENCES hotels(property_id) ON DELETE CASCADE,
            room_type_id TEXT NOT NULL,
            rate_id TEXT,
            hotel_collection TEXT,
            available INTEGER,
            is_breakfast_included INTEGER,
            is_food_beverage_credit INTEGER,
            is_free_cancellation INTEGER,
            is_parking_included INTEGER,
            is_shuttle_included INTEGER,
            occupancy_adults INTEGER,
            occupancy_children INTEGER,
            room_count INTEGER,
            pricing_currency TEXT,
            pricing_base REAL,
            pricing_total REAL,
            pricing_total_inclusive REAL,
            pricing_total_fees REAL,
            pricing_total_taxes REAL,
            average_nightly_rate REAL,
            average_nightly_rate_points_burn REAL,
            payment_model TEXT,
            points_burn INTEGER,
            points_burn_calculation_json TEXT,
            room_allocations_json TEXT,
            special_offer_json TEXT,
            supplier_rate_promotion_json TEXT,
            comparison_amenity_json TEXT,
            search_context_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, property_id, room_type_id, rate_id)
        );
        CREATE INDEX IF NOT EXISTS idx_rate_snapshots_run ON rate_snapshots(run_id);
        CREATE INDEX IF NOT EXISTS idx_rate_snapshots_property ON rate_snapshots(property_id);

        CREATE TABLE IF NOT EXISTS rate_nightly_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_snapshot_id INTEGER NOT NULL REFERENCES rate_snapshots(id) ON DELETE CASCADE,
            night_index INTEGER NOT NULL,
            night_date TEXT,
            actual_rate REAL,
            inclusive_rate REAL
        );

        CREATE TABLE IF NOT EXISTS rate_components (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_snapshot_id INTEGER NOT NULL REFERENCES rate_snapshots(id) ON DELETE CASCADE,
            component_type TEXT NOT NULL,
            code TEXT,
            label TEXT,
            amount REAL,
            currency TEXT,
            is_included INTEGER,
            pay_locally INTEGER,
            details_json TEXT
        );
    """,
    2: """
        CREATE TABLE IF NOT EXISTS hotel_promotions (
            property_id TEXT NOT NULL REFERENCES hotels(property_id) ON DELETE CASCADE,
            promotion_code TEXT NOT NULL,
            promotion_type TEXT,
            title TEXT,
            description TEXT,
            min_nights INTEGER,
            max_nights INTEGER,
            booking_start TEXT,
            booking_end TEXT,
            stay_start TEXT,
            stay_end TEXT,
            blackout_dates_json TEXT,
            card_types_json TEXT,
            raw_json TEXT NOT NULL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY (property_id, promotion_code)
        );
        CREATE INDEX IF NOT EXISTS idx_hotel_promotions_type ON hotel_promotions(promotion_type);
    """,
    3: """
        ALTER TABLE hotels ADD COLUMN renovation_closure_notice TEXT;
        DROP TABLE IF EXISTS hotel_payloads;
    """,
    4: """
        DROP TABLE IF EXISTS search_payloads;
    """,
    5: """
        ALTER TABLE rate_snapshots DROP COLUMN raw_json;
    """,
}
