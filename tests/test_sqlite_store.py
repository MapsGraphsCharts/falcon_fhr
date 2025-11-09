from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from secure_scraper.destinations.catalog import Destination
from secure_scraper.storage import SqliteStore
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams


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
    await store.close()

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT status, total_hotels, total_rates, request_id FROM search_runs")
        status, total_hotels, total_rates, request_id = cur.fetchone()
        assert status == "complete"
        assert total_hotels == 1
        assert total_rates == 1
        assert request_id == "req-123"

        cur = conn.execute("SELECT COUNT(*) FROM hotels WHERE property_id='hotel-1'")
        assert cur.fetchone()[0] == 1

        cur = conn.execute("SELECT COUNT(*) FROM hotel_payloads WHERE run_id=?", (run_id,))
        assert cur.fetchone()[0] == 1

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
