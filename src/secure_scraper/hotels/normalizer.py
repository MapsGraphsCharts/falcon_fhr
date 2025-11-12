"""Utilities to transform raw Amex property payloads into normalised records."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from secure_scraper.destinations.catalog import Destination
from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams

from .models import (
    HotelProgramBenefit,
    HotelRateRecord,
    HotelRecord,
    RatePricing,
    SearchContext,
)


def _sum_adults(rooms: Iterable[RoomRequest]) -> int:
    return sum(room.adults for room in rooms)


def _sum_children(rooms: Iterable[RoomRequest]) -> int:
    return sum(len(room.children) for room in rooms)


def _extract_description(entries: Optional[Iterable[dict[str, Any]]]) -> tuple[Optional[str], Optional[str]]:
    if not entries:
        return None, None
    preferred = None
    for entry in entries:
        locale = (entry.get("locale") or "").lower()
        if locale.startswith("en"):
            preferred = entry
            break
    if preferred is None:
        preferred = next(iter(entries), None)
    if preferred is None:
        return None, None
    return preferred.get("description"), preferred.get("note")


def _extract_images(property_images: Optional[Iterable[dict[str, Any]]]) -> tuple[Optional[str], List[str]]:
    gallery: List[str] = []
    hero: Optional[str] = None
    if not property_images:
        return hero, gallery
    for image in property_images:
        large = image.get("large")
        if not large:
            continue
        if hero is None and image.get("isHero"):
            hero = large
        gallery.append(large)
    if hero is None and gallery:
        hero = gallery[0]
    return hero, gallery


def _normalize_notice(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, Iterable):
        parts = []
        for item in value:
            if not item:
                continue
            parts.append(str(item).strip())
        notice = "\n".join(part for part in parts if part)
        return notice or None
    return str(value).strip() or None


def _flatten_program_benefits(program_entries: Iterable[dict[str, Any]]) -> List[HotelProgramBenefit]:
    benefits: List[HotelProgramBenefit] = []
    for program in program_entries:
        program_code = program.get("programCode") or ""
        program_name = program.get("programName") or ""
        exceptional_value = program.get("exceptionalValue")
        for benefit in program.get("benefits", []):
            description, note = _extract_description(benefit.get("descriptions"))
            benefits.append(
                HotelProgramBenefit(
                    program_code=program_code,
                    program_name=program_name,
                    benefit_type=benefit.get("type") or "",
                    description=description,
                    note=note,
                    start_date=benefit.get("startDate"),
                    end_date=benefit.get("EndDate") or benefit.get("endDate"),
                    exceptional_value=exceptional_value,
                )
            )
    return benefits


def _build_search_context(
    destination: Destination,
    params: SearchParams,
    context: dict[str, Any],
) -> SearchContext:
    return SearchContext(
        destination_key=destination.key,
        destination_group=destination.group,
        destination_name=destination.name,
        search_location_id=params.location_id,
        search_location_label=params.location_label,
        check_in=params.check_in,
        check_out=params.check_out,
        rooms=len(params.rooms),
        total_adults=_sum_adults(params.rooms),
        total_children=_sum_children(params.rooms),
        request_id=context.get("requestId"),
    )


def build_hotel_record(
    hotel: dict[str, Any],
    *,
    destination: Destination,
    params: SearchParams,
    context: Optional[dict[str, Any]] = None,
) -> HotelRecord:
    context = context or {}
    address: Dict[str, Any] = hotel.get("address") or {}
    check_in: Dict[str, Any] = hotel.get("checkIn") or {}
    check_out: Dict[str, Any] = hotel.get("checkOut") or {}
    geo: Dict[str, Any] = hotel.get("geoLocation") or {}
    distance_info: Dict[str, Any] = hotel.get("distanceFromSearchLocation") or {}
    chain: Dict[str, Any] = hotel.get("chain") or {}
    brand: Dict[str, Any] = hotel.get("brand") or {}
    decoration: Dict[str, Any] = hotel.get("clientHotelDecoration") or {}
    hotel_info: Dict[str, Any] = decoration.get("clientHotelInfo") or {}
    marketing: Dict[str, Any] = hotel_info.get("marketingInfo") or {}
    reviews: Dict[str, Any] = hotel.get("userReviews") or {}
    renovation_notice = _normalize_notice(hotel.get("renovationAndClosures"))

    hero_image, gallery = _extract_images(hotel.get("propertyImages"))
    program_benefits = _flatten_program_benefits(decoration.get("programBenefits", []))

    description_field = hotel.get("description")
    if isinstance(description_field, dict):
        description_text = description_field.get("text")
    else:
        description_text = description_field

    search_context = _build_search_context(destination, params, context)

    return HotelRecord(
        property_id=hotel.get("id") or "",
        supplier_id=str(hotel.get("supplierId")) if hotel.get("supplierId") is not None else None,
        name=hotel.get("name") or "",
        type=hotel.get("type"),
        brand_name=brand.get("name"),
        chain_name=chain.get("name"),
        star_rating=hotel.get("starRating"),
        phone=hotel.get("phone"),
        address_line1=address.get("addressLine1"),
        address_city=address.get("cityName"),
        address_state=address.get("provinceName") or address.get("provinceCode"),
        address_postal_code=address.get("postalCode"),
        address_country_code=address.get("countryCode"),
        address_country_name=address.get("countryName"),
        latitude=geo.get("latitude"),
        longitude=geo.get("longitude"),
        distance_miles=distance_info.get("distance"),
        distance_unit=distance_info.get("unit"),
        interests=list(hotel.get("interests") or []),
        amenities=[item.get("description") for item in hotel.get("amenities", []) if item.get("description")],
        program_codes=list(decoration.get("programs") or []),
        program_benefits=program_benefits,
        check_in_start=check_in.get("beginTime"),
        check_in_end=check_in.get("endTime"),
        check_out_time=check_out.get("time"),
        check_in_instructions=check_in.get("instructions"),
        check_out_instructions=check_out.get("instructions"),
        description_short=marketing.get("shortDescription") or hotel.get("caption"),
        description_long=marketing.get("description") or description_text,
        description_accommodation=marketing.get("accomodationDescription"),
        description_dining=marketing.get("diningDescription"),
        description_amenities=marketing.get("amenitiesDescription"),
        description_activities=marketing.get("activitiesDescription"),
        marketing_tags=list(marketing.get("featuresTags") or []),
        marketing_insider_tip=marketing.get("insiderTip"),
        marketing_video=marketing.get("marketingVideo"),
        hero_image=hero_image,
        image_gallery=gallery,
        images=list(hotel.get("images") or []),
        payment_options=list(hotel.get("paymentOptions") or []),
        loyalty_valid=chain.get("validForLoyaltyProgram"),
        user_rating=reviews.get("rating"),
        user_rating_count=reviews.get("reviewCount"),
        host_languages=list(hotel.get("hostLanguages") or []),
        location_teaser=hotel.get("locationTeaser"),
        policies=[policy.get("description") for policy in hotel.get("policies", []) if policy.get("description")],
        no_show_policy=hotel.get("noShowPolicy"),
        supplier_fees=[fee.get("text") for fee in hotel.get("supplierFeesDescriptions", []) if fee.get("text")],
        renovation_closure_notice=renovation_notice,
        search=search_context,
        raw=hotel,
    )


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_pricing(info: Optional[dict[str, Any]]) -> Optional[RatePricing]:
    if not info:
        return None
    nightly_actual_rates: List[float] = []
    nightly_inclusive_rates: List[float] = []
    for value in info.get("nightlyActualRates", []):
        converted = _to_float(value)
        if converted is not None:
            nightly_actual_rates.append(converted)
    for value in info.get("nightlyInclusiveRates", []):
        converted = _to_float(value)
        if converted is not None:
            nightly_inclusive_rates.append(converted)

    return RatePricing(
        currency=info.get("currency"),
        base=_to_float(info.get("base")),
        total=_to_float(info.get("total")),
        total_inclusive=_to_float(info.get("totalInclusive")),
        total_fees=_to_float(info.get("totalFees")),
        total_taxes=_to_float(info.get("totalTaxes")),
        average_nightly_rate=_to_float(info.get("averageNightlyRate")),
        average_nightly_rate_points_burn=_to_float(info.get("averageNightlyRatePointsBurn")),
        nightly_actual_rates=nightly_actual_rates,
        nightly_inclusive_rates=nightly_inclusive_rates,
        payment_model=info.get("paymentModel"),
        points_burn=info.get("pointsBurn"),
        points_burn_calculation=info.get("pointsBurnCalculation"),
        fees=[
            {
                "type": fee.get("type"),
                "value": _to_float(fee.get("value")),
                "currency": fee.get("currency"),
                "is_included": fee.get("isIncluded"),
                "pay_locally": fee.get("payLocally"),
            }
            for fee in info.get("fees", [])
        ],
        taxes=[
            {
                "type": tax.get("type"),
                "value": _to_float(tax.get("value")),
                "currency": tax.get("currency"),
                "is_included": tax.get("isIncluded"),
                "pay_locally": tax.get("payLocally"),
            }
            for tax in info.get("taxes", [])
        ],
    )


def _extract_room_allocations(rooms: Iterable[dict[str, Any]]) -> tuple[List[dict[str, Any]], Optional[int], Optional[int]]:
    allocations: List[dict[str, Any]] = []
    total_adults = 0
    total_children = 0
    for allocation in rooms:
        adults = allocation.get("adults")
        children = allocation.get("children")
        total_adults += adults or 0
        if isinstance(children, list):
            total_children += len(children)
        elif isinstance(children, int):
            total_children += children
        pricing = _parse_pricing(allocation.get("pricingInfo"))
        allocations.append(
            {
                "adults": adults,
                "children": children,
                "pricing": pricing.to_dict() if pricing else None,
                "cancellation_policies": allocation.get("cancellationPolicies", []),
            }
        )
    return allocations, total_adults or None, total_children or None


def _extract_rate_records(
    hotel: dict[str, Any],
    *,
    destination: Destination,
    params: SearchParams,
    context: dict[str, Any],
    search_context: SearchContext,
) -> List[HotelRateRecord]:
    records: List[HotelRateRecord] = []
    property_id = hotel.get("id") or ""
    for room_type in hotel.get("roomTypes", []):
        room_type_id = room_type.get("id")
        room_type_name = room_type.get("name")
        for rate in room_type.get("rates", []):
            pricing = _parse_pricing(rate.get("pricingInfo"))
            allocations, occ_adults, occ_children = _extract_room_allocations(rate.get("rooms", []))
            cancel_penalties = [
                {
                    "start": penalty.get("start"),
                    "end": penalty.get("end"),
                    "nights": penalty.get("nights"),
                    "currency": penalty.get("currency"),
                    "amount": _to_float(penalty.get("amount")),
                }
                for penalty in rate.get("cancelPenalties", [])
            ]
            cancellation_policy_text = None
            if allocations:
                policies = allocations[0].get("cancellation_policies") or []
                if policies:
                    cancellation_policy_text = policies[0].get("text")

            records.append(
                HotelRateRecord(
                    property_id=property_id,
                    location_id=params.location_id,
                    room_type_id=room_type_id,
                    room_type_name=room_type_name,
                    rate_id=rate.get("id"),
                    hotel_collection=rate.get("hotelCollection"),
                    available=rate.get("available"),
                    is_breakfast_included=rate.get("isBreakfastIncluded"),
                    is_food_beverage_credit=rate.get("isFoodBeverageCredit"),
                    is_free_cancellation=rate.get("isFreeCancellation"),
                    is_parking_included=rate.get("isParkingIncluded"),
                    is_shuttle_included=rate.get("isShuttleIncluded"),
                    amenities=[amenity.get("description") for amenity in rate.get("amenities", []) if amenity.get("description")],
                    bed_groups=rate.get("bedGroups", []),
                    cancel_penalties=cancel_penalties,
                    cancellation_policy_text=cancellation_policy_text,
                    occupancy_adults=occ_adults,
                    occupancy_children=occ_children,
                    room_count=len(allocations) or None,
                    pricing=pricing,
                    room_allocations=allocations,
                    search=search_context,
                    special_offer=rate.get("specialOffer"),
                    supplier_rate_promotion=rate.get("supplierRatePromotion"),
                    comparison_amenity=rate.get("comparisonAmenity"),
                    raw=rate,
                )
            )
    return records


def build_hotel_and_rate_records(
    payload: dict[str, Any],
    *,
    destination: Destination,
    params: SearchParams,
) -> Tuple[List[HotelRecord], List[HotelRateRecord]]:
    context = payload.get("context") or {}
    hotels_payload = payload.get("hotels") or []

    hotel_records: List[HotelRecord] = []
    rate_records: List[HotelRateRecord] = []

    for hotel in hotels_payload:
        search_context = _build_search_context(destination, params, context)
        hotel_record = build_hotel_record(
            hotel,
            destination=destination,
            params=params,
            context=context,
        )
        hotel_records.append(hotel_record)
        rate_records.extend(
            _extract_rate_records(
                hotel,
                destination=destination,
                params=params,
                context=context,
                search_context=search_context,
            )
        )
    return hotel_records, rate_records


def build_hotel_records(
    payload: dict[str, Any],
    *,
    destination: Destination,
    params: SearchParams,
) -> List[HotelRecord]:
    hotels, _ = build_hotel_and_rate_records(payload, destination=destination, params=params)
    return hotels


def build_rate_records(
    payload: dict[str, Any],
    *,
    destination: Destination,
    params: SearchParams,
) -> List[HotelRateRecord]:
    _, rates = build_hotel_and_rate_records(payload, destination=destination, params=params)
    return rates
