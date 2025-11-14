from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from secure_scraper.destinations.catalog import Destination
from secure_scraper.storage import SqliteStore
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams

_SYNCHRONOUS_MAP = {0: "off", 1: "normal", 2: "full", 3: "extra"}


@pytest.mark.asyncio
async def test_sqlite_store_persists_hotels_and_rates(tmp_path) -> None:
    db_path = tmp_path / "store.sqlite"
    store = SqliteStore(db_path)
    await store.initialize()

    destination = Destination(
        key="test-dest",
        group="Group",
        name="Test City",
        location_id="LOC-1",
        latitude=1.23,
        longitude=4.56,
    )
    params = SearchParams(
        location_id="LOC-1",
        location_label="Test City",
        latitude=1.23,
        longitude=4.56,
        check_in=date(2025, 1, 1),
        check_out=date(2025, 1, 2),
        rooms=[RoomRequest(adults=2)],
        program_filter=["FHR"],
    )

    run_id = await store.begin_run(destination=destination, params=params, label="test-run")

    hotel_record = {
        "property_id": "hotel-1",
        "supplier_id": "supplier-1",
        "summary": {
            "name": "Hotel Test",
            "type": "Hotel",
            "amenities": ["Free WiFi"],
            "program_codes": ["FHR"],
            "interests": [],
            "marketing_tags": [],
            "host_languages": [],
            "payment_options": [],
            "policies": [],
            "supplier_fees": [],
            "program_benefits": [],
            "renovation_closure_notice": "Pool closed Jan 19-23 for maintenance",
        },
        "search": {
            "destination_key": destination.key,
            "destination_group": destination.group,
            "destination_name": destination.name,
            "search_location_id": params.location_id,
            "search_location_label": params.location_label,
            "check_in": params.check_in.isoformat(),
            "check_out": params.check_out.isoformat(),
            "rooms": 1,
            "total_adults": 2,
            "total_children": 0,
            "nights": 1,
            "request_id": "req-123",
        },
        "raw": {"id": "hotel-1"},
    }

    special_offer = {
        "promotionCode": "FHR123",
        "type": "SPECIAL_OFFER",
        "minNights": 3,
        "bookingStartDate": "2024-01-01",
        "bookingEndDate": "2024-12-31",
        "stayStartDate": "2024-02-01",
        "stayEndDate": "2024-11-30",
        "blackoutDates": ["2024-05-01"],
        "cardTypes": ["Consumer Platinum"],
        "descriptions": [
            {
                "locale": "en-us",
                "title": "Stay Longer",
                "description": "Enjoy a complimentary third night.",
            }
        ],
    }

    rate_record = {
        "property_id": "hotel-1",
        "room_type_id": "room-1",
        "room_type_name": "Suite",
        "rate_id": "rate-1",
        "summary": {
            "hotel_collection": "FHR",
            "available": 1,
            "is_breakfast_included": True,
            "is_food_beverage_credit": False,
            "is_free_cancellation": True,
            "is_parking_included": False,
            "is_shuttle_included": False,
            "amenities": ["WiFi"],
            "bed_groups": [],
            "cancel_penalties": [],
            "room_allocations": [],
            "pricing": {
                "currency": "USD",
                "base": 100.0,
                "total": 120.0,
                "total_inclusive": 120.0,
                "total_fees": 10.0,
                "total_taxes": 10.0,
                "average_nightly_rate": 100.0,
                "average_nightly_rate_points_burn": None,
                "payment_model": "PAY_NOW",
                "points_burn": None,
                "nightly_actual_rates": [100.0],
                "nightly_inclusive_rates": [120.0],
                "fees": [
                    {
                        "type": "PROPERTY_FEE",
                        "currency": "USD",
                        "value": 10.0,
                        "isIncluded": True,
                        "payLocally": False,
                    }
                ],
                "taxes": [],
            },
        },
        "occupancy_adults": 2,
        "occupancy_children": 0,
        "room_count": 1,
        "search": hotel_record["search"],
        "raw": {"id": "rate-1"},
        "special_offer": special_offer,
        "supplier_rate_promotion": None,
        "comparison_amenity": None,
    }

    payload = {"context": {"requestId": "req-123"}, "hotels": [hotel_record]}

    await store.store_run_payload(run_id, payload)
    await store.save_hotels(run_id, [hotel_record])
    await store.save_rates(run_id, [rate_record])
    await store.finalize_run(
        run_id,
        total_hotels=1,
        total_rates=1,
        request_id="req-123",
        context=payload["context"],
    )

    writer_sync_mode = store._require_connection().execute("PRAGMA synchronous").fetchone()[0]
    assert _SYNCHRONOUS_MAP[int(writer_sync_mode)] == "normal"

    await store.close()

    conn = sqlite3.connect(db_path)
    try:
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert journal_mode.lower() == "wal"
        cur = conn.execute("SELECT status, total_hotels, total_rates, request_id FROM search_runs")
        status, total_hotels, total_rates, request_id = cur.fetchone()
        assert status == "complete"
        assert total_hotels == 1
        assert total_rates == 1
        assert request_id == "req-123"

        cur = conn.execute("SELECT COUNT(*) FROM hotels WHERE property_id='hotel-1'")
        assert cur.fetchone()[0] == 1

        cur = conn.execute(
            "SELECT renovation_closure_notice FROM hotels WHERE property_id='hotel-1'"
        )
        assert cur.fetchone()[0] == "Pool closed Jan 19-23 for maintenance"

        cur = conn.execute("SELECT COUNT(*) FROM rate_snapshots WHERE property_id='hotel-1'")
        assert cur.fetchone()[0] == 1

        cur = conn.execute("SELECT COUNT(*) FROM rate_nightly_prices")
        assert cur.fetchone()[0] == 1

        cur = conn.execute("SELECT COUNT(*) FROM rate_components")
        assert cur.fetchone()[0] == 1

        cur = conn.execute(
            """
            SELECT promotion_code, promotion_type, min_nights, booking_start, stay_start
            FROM hotel_promotions
            WHERE property_id='hotel-1'
            """
        )
        promo_row = cur.fetchone()
        assert promo_row == ("FHR123", "SPECIAL_OFFER", 3, "2024-01-01", "2024-02-01")
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_sqlite_store_custom_pragmas(tmp_path) -> None:
    db_path = tmp_path / "custom.sqlite"
    store = SqliteStore(db_path, journal_mode="delete", synchronous="full")
    await store.initialize()

    writer_sync_mode = store._require_connection().execute("PRAGMA synchronous").fetchone()[0]
    assert _SYNCHRONOUS_MAP[int(writer_sync_mode)] == "full"

    await store.close()

    conn = sqlite3.connect(db_path)
    try:
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous_mode = conn.execute("PRAGMA synchronous").fetchone()[0]
        assert journal_mode.lower() == "delete"
        assert _SYNCHRONOUS_MAP[int(synchronous_mode)] == "full"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_fetch_latest_runs_bulk(tmp_path) -> None:
    db_path = tmp_path / "bulk.sqlite"
    store = SqliteStore(db_path)
    await store.initialize()

    dest_a = Destination(
        key="dest-a",
        group="Group",
        name="City A",
        location_id="LOC-A",
        latitude=1.0,
        longitude=2.0,
    )
    dest_b = Destination(
        key="dest-b",
        group="Group",
        name="City B",
        location_id="LOC-B",
        latitude=3.0,
        longitude=4.0,
    )
    dest_c = Destination(
        key="dest-c",
        group="Group",
        name="City C",
        location_id="LOC-C",
        latitude=5.0,
        longitude=6.0,
    )

    params_a = SearchParams(
        location_id="LOC-A",
        location_label="City A",
        latitude=1.0,
        longitude=2.0,
        check_in=date(2025, 2, 1),
        check_out=date(2025, 2, 2),
        rooms=[RoomRequest(adults=2)],
        program_filter=["FHR"],
    )
    params_b = SearchParams(
        location_id="LOC-B",
        location_label="City B",
        latitude=3.0,
        longitude=4.0,
        check_in=date(2025, 2, 1),
        check_out=date(2025, 2, 2),
        rooms=[RoomRequest(adults=2)],
        program_filter=None,
    )
    params_c = SearchParams(
        location_id="LOC-C",
        location_label="City C",
        latitude=5.0,
        longitude=6.0,
        check_in=date(2025, 2, 1),
        check_out=date(2025, 2, 2),
        rooms=[RoomRequest(adults=2)],
        program_filter=None,
    )

    run_a = await store.begin_run(destination=dest_a, params=params_a, label="bulk")
    await store.finalize_run(
        run_a,
        total_hotels=1,
        total_rates=1,
        request_id="req-a",
        context={"requestId": "req-a"},
    )

    run_b = await store.begin_run(destination=dest_b, params=params_b, label="bulk")
    await store.mark_run_failed(run_b, "boom")

    runs = [(dest_a, params_a), (dest_b, params_b), (dest_c, params_c)]
    results = await store.fetch_latest_runs_bulk(runs, label="bulk")

    assert set(results.keys()) == {"dest-a", "dest-b"}
    assert results["dest-a"].status == "complete"
    assert results["dest-b"].status == "failed"

    await store.close()
