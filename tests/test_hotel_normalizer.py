from __future__ import annotations

from datetime import date

from secure_scraper.destinations.catalog import Destination
from secure_scraper.hotels import build_hotel_and_rate_records
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams


def test_build_hotel_records_creates_db_ready_payload():
    hotel_payload = {
        "context": {"requestId": "REQ-123"},
        "hotels": [
            {
                "id": "ZHOT-EXPEDIA-123456",
                "supplierId": 987,
                "name": "Test Hotel",
                "type": "Hotel",
                "brand": {"name": "Test Brand"},
                "chain": {"name": "Test Chain", "validForLoyaltyProgram": True},
                "starRating": 5.0,
                "phone": "+1-555-0100",
                "address": {
                    "addressLine1": "1 Plaza Way",
                    "cityName": "Metropolis",
                    "provinceName": "State",
                    "postalCode": "12345",
                    "countryCode": "US",
                    "countryName": "United States of America",
                },
                "geoLocation": {"latitude": 40.0, "longitude": -74.0},
                "distanceFromSearchLocation": {"distance": 1.2, "unit": "mi"},
                "amenities": [{"description": "Pool"}, {"description": "Gym"}],
                "interests": ["Luxury", "Family property"],
                "paymentOptions": ["American Express", "Visa"],
                "checkIn": {"beginTime": "15:00-05:00", "endTime": "23:00-05:00", "instructions": "Bring ID"},
                "checkOut": {"time": "11:00-05:00"},
                "clientHotelDecoration": {
                    "programs": ["FHR"],
                    "programBenefits": [
                        {
                            "programCode": "FHR",
                            "programName": "Fine Hotels + Resorts",
                            "exceptionalValue": True,
                            "benefits": [
                                {
                                    "type": "Breakfast",
                                    "descriptions": [{"locale": "en-us", "description": "Daily breakfast"}],
                                    "startDate": "2024-01-01",
                                    "endDate": "2024-12-31",
                                },
                                {
                                    "type": "Late Check-Out",
                                    "descriptions": [{"locale": "en-us", "description": "Guaranteed 4pm late checkout"}],
                                },
                            ],
                        }
                    ],
                    "clientHotelInfo": {
                        "marketingInfo": {
                            "shortDescription": "Headline",
                            "description": "Full marketing description",
                            "diningDescription": "Dining details",
                            "amenitiesDescription": "Amenity overview",
                            "activitiesDescription": "Activities overview",
                            "accomodationDescription": "Accommodation overview",
                            "insiderTip": "Insider tip content",
                            "marketingVideo": "https://example.com/video",
                            "featuresTags": ["TAG1", "TAG2"],
                        }
                    },
                },
                "userReviews": {"rating": 4.7, "reviewCount": 128},
                "propertyImages": [
                    {"isHero": True, "large": "https://example.com/hero.jpg"},
                    {"large": "https://example.com/alt.jpg"},
                ],
                "caption": "Marketing caption",
                "description": "Fallback property description",
                "images": ["https://example.com/image1.jpg"],
                "hostLanguages": ["English", "Spanish"],
                "policies": [{"description": "No pets"}],
                "noShowPolicy": "Cancellation penalties may apply.",
                "supplierFeesDescriptions": [{"text": "Resort fee USD 20"}],
                "roomTypes": [
                    {
                        "id": "ROOM-1",
                        "name": "Deluxe Room",
                        "rates": [
                            {
                                "id": "RATE-1",
                                "available": 2,
                                "hotelCollection": "FHR",
                                "isBreakfastIncluded": True,
                                "isFoodBeverageCredit": False,
                                "isFreeCancellation": True,
                                "isParkingIncluded": False,
                                "isShuttleIncluded": False,
                                "amenities": [{"description": "Free WiFi"}],
                                "bedGroups": [
                                    {
                                        "beds": [
                                            {"count": 1, "size": "King", "type": "KingBed"},
                                        ],
                                        "description": "1 King Bed",
                                    }
                                ],
                                "cancelPenalties": [
                                    {
                                        "start": "2024-01-08T23:59:00-05:00",
                                        "end": "2024-01-10T23:59:00-05:00",
                                        "nights": 1,
                                        "currency": "USD",
                                    }
                                ],
                                "pricingInfo": {
                                    "currency": "USD",
                                    "base": 1000,
                                    "total": 1150,
                                    "totalInclusive": 1150,
                                    "totalFees": 150,
                                    "totalTaxes": 0,
                                    "averageNightlyRate": 500,
                                    "averageNightlyRatePointsBurn": 70000,
                                    "nightlyActualRates": [500, 500],
                                    "nightlyInclusiveRates": [575, 575],
                                    "paymentModel": "PAY_NOW",
                                    "pointsBurn": 140000,
                                    "pointsBurnCalculation": {"type": "RATE", "value": 0.007},
                                    "fees": [
                                        {
                                            "type": "PROPERTY_FEE",
                                            "value": 50,
                                            "currency": "USD",
                                            "isIncluded": True,
                                            "payLocally": False,
                                        }
                                    ],
                                    "taxes": [
                                        {
                                            "type": "SALES_TAX",
                                            "value": 100,
                                            "currency": "USD",
                                            "isIncluded": True,
                                            "payLocally": False,
                                        }
                                    ],
                                },
                                "specialOffer": {
                                    "promotionCode": "FHR123",
                                    "type": "SPECIAL_OFFER",
                                    "minNights": 3,
                                    "bookingStartDate": "2024-01-01",
                                    "bookingEndDate": "2024-02-01",
                                    "stayStartDate": "2024-02-10",
                                    "stayEndDate": "2024-03-31",
                                    "descriptions": [
                                        {
                                            "locale": "en-us",
                                            "description": "Receive a complimentary third night when booking a stay of at least 3 nights.",
                                        }
                                    ],
                                },
                                "supplierRatePromotion": {
                                    "title": "Exclusive Nightly Offer",
                                    "description": "Save more when you stay longer." },
                                "comparisonAmenity": {
                                    "header": "Exclusive Benefits",
                                    "description": "Includes resort credit and welcome amenity.",
                                },
                                "rooms": [
                                    {
                                        "adults": 2,
                                        "children": 0,
                                        "pricingInfo": {
                                            "currency": "USD",
                                            "base": 1000,
                                            "total": 1150,
                                            "totalInclusive": 1150,
                                            "totalFees": 150,
                                            "totalTaxes": 0,
                                        },
                                        "cancellationPolicies": [
                                            {"text": "24h free cancellation"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    destination = Destination(
        key="us-test",
        group="United States",
        name="Test Destination",
        location_id="LOC-1",
        latitude=40.0,
        longitude=-74.0,
    )
    params = SearchParams(
        location_id="LOC-1",
        location_label="Test Destination",
        latitude=40.0,
        longitude=-74.0,
        check_in=date(2024, 1, 10),
        check_out=date(2024, 1, 12),
        rooms=[
            RoomRequest(adults=2, children=[6]),
            RoomRequest(adults=1),
        ],
    )

    hotel_records, rate_records = build_hotel_and_rate_records(
        hotel_payload, destination=destination, params=params
    )
    assert len(hotel_records) == 1
    assert len(rate_records) == 1

    record = hotel_records[0]
    assert record.property_id == "ZHOT-EXPEDIA-123456"
    assert record.brand_name == "Test Brand"
    assert record.chain_name == "Test Chain"
    assert record.star_rating == 5.0
    assert record.latitude == 40.0
    assert record.longitude == -74.0
    assert record.distance_miles == 1.2
    assert record.amenities == ["Pool", "Gym"]
    assert record.program_codes == ["FHR"]
    assert record.program_benefits[0].benefit_type == "Breakfast"
    assert record.program_benefits[0].description == "Daily breakfast"
    assert record.program_benefits[0].exceptional_value is True
    assert record.hero_image == "https://example.com/hero.jpg"
    assert record.image_gallery == ["https://example.com/hero.jpg", "https://example.com/alt.jpg"]
    assert record.images == ["https://example.com/image1.jpg"]
    assert record.loyalty_valid is True
    assert record.description_short == "Headline"
    assert record.description_long == "Full marketing description"
    assert record.description_dining == "Dining details"
    assert record.description_amenities == "Amenity overview"
    assert record.description_activities == "Activities overview"
    assert record.description_accommodation == "Accommodation overview"
    assert record.marketing_insider_tip == "Insider tip content"
    assert record.marketing_tags == ["TAG1", "TAG2"]
    assert record.user_rating == 4.7
    assert record.user_rating_count == 128
    assert record.check_in_start == "15:00-05:00"
    assert record.check_out_time == "11:00-05:00"
    assert record.host_languages == ["English", "Spanish"]
    assert record.location_teaser is None
    assert record.policies == ["No pets"]
    assert record.no_show_policy == "Cancellation penalties may apply."
    assert record.supplier_fees == ["Resort fee USD 20"]

    record_dict = record.to_dict()
    assert record_dict["search"]["destination_key"] == "us-test"
    assert record_dict["search"]["nights"] == 2
    assert record_dict["search"]["total_adults"] == 3
    assert record_dict["search"]["total_children"] == 1
    summary = record_dict["summary"]
    assert summary["program_benefits"][1]["benefit_type"] == "Late Check-Out"
    assert summary["program_benefits"][1]["description"] == "Guaranteed 4pm late checkout"
    assert record_dict["raw"]["id"] == "ZHOT-EXPEDIA-123456"

    rate = rate_records[0]
    assert rate.property_id == "ZHOT-EXPEDIA-123456"
    assert rate.room_type_id == "ROOM-1"
    assert rate.room_type_name == "Deluxe Room"
    assert rate.rate_id == "RATE-1"
    assert rate.hotel_collection == "FHR"
    assert rate.available == 2
    assert rate.is_breakfast_included is True
    assert rate.amenities == ["Free WiFi"]
    assert rate.occupancy_adults == 2
    assert rate.room_count == 1
    assert rate.cancellation_policy_text == "24h free cancellation"
    assert rate.pricing is not None
    assert rate.pricing.total == 1150
    pricing_dict = rate.pricing.to_dict()
    assert pricing_dict["fees"][0]["type"] == "PROPERTY_FEE"
    assert pricing_dict["taxes"][0]["type"] == "SALES_TAX"
    assert rate.special_offer["promotionCode"] == "FHR123"
    rate_dict = rate.to_dict()
    assert rate_dict["search"]["destination_key"] == "us-test"
    assert rate_dict["summary"]["special_offer"]["promotionCode"] == "FHR123"
    assert rate_dict["raw"]["specialOffer"]["promotionCode"] == "FHR123"
