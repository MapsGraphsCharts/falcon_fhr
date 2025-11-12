"""Dataclasses for normalised hotel metadata and rate records."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Optional


@dataclass(slots=True)
class SearchContext:
    """Metadata describing the originating search request."""

    destination_key: str
    destination_group: str
    destination_name: str
    search_location_id: str
    search_location_label: str
    check_in: date
    check_out: date
    rooms: int
    total_adults: int
    total_children: int
    request_id: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        return {
            "destination_key": self.destination_key,
            "destination_group": self.destination_group,
            "destination_name": self.destination_name,
            "search_location_id": self.search_location_id,
            "search_location_label": self.search_location_label,
            "check_in": self.check_in.isoformat(),
            "check_out": self.check_out.isoformat(),
            "rooms": self.rooms,
            "total_adults": self.total_adults,
            "total_children": self.total_children,
            "nights": (self.check_out - self.check_in).days,
            "request_id": self.request_id,
        }


@dataclass(slots=True)
class HotelProgramBenefit:
    """Represents a loyalty/program benefit associated with a property."""

    program_code: str
    program_name: str
    benefit_type: str
    description: Optional[str] = None
    note: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    exceptional_value: Optional[bool] = None

    def to_dict(self) -> dict[str, object]:
        return {
            "program_code": self.program_code,
            "program_name": self.program_name,
            "benefit_type": self.benefit_type,
            "description": self.description,
            "note": self.note,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "exceptional_value": self.exceptional_value,
        }


@dataclass(slots=True)
class HotelRecord:
    """Flattened hotel metadata appropriate for persistent storage."""

    property_id: str
    supplier_id: Optional[str]
    name: str
    type: Optional[str]
    brand_name: Optional[str]
    chain_name: Optional[str]
    star_rating: Optional[float]
    phone: Optional[str]
    address_line1: Optional[str]
    address_city: Optional[str]
    address_state: Optional[str]
    address_postal_code: Optional[str]
    address_country_code: Optional[str]
    address_country_name: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    distance_miles: Optional[float]
    distance_unit: Optional[str]
    interests: List[str] = field(default_factory=list)
    amenities: List[str] = field(default_factory=list)
    program_codes: List[str] = field(default_factory=list)
    program_benefits: List[HotelProgramBenefit] = field(default_factory=list)
    check_in_start: Optional[str] = None
    check_in_end: Optional[str] = None
    check_out_time: Optional[str] = None
    check_in_instructions: Optional[str] = None
    check_out_instructions: Optional[str] = None
    description_short: Optional[str] = None
    description_long: Optional[str] = None
    description_accommodation: Optional[str] = None
    description_dining: Optional[str] = None
    description_amenities: Optional[str] = None
    description_activities: Optional[str] = None
    marketing_tags: List[str] = field(default_factory=list)
    marketing_insider_tip: Optional[str] = None
    marketing_video: Optional[str] = None
    hero_image: Optional[str] = None
    image_gallery: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    payment_options: List[str] = field(default_factory=list)
    loyalty_valid: Optional[bool] = None
    user_rating: Optional[float] = None
    user_rating_count: Optional[int] = None
    host_languages: List[str] = field(default_factory=list)
    location_teaser: Optional[str] = None
    policies: List[str] = field(default_factory=list)
    no_show_policy: Optional[str] = None
    supplier_fees: List[str] = field(default_factory=list)
    renovation_closure_notice: Optional[str] = None
    search: Optional[SearchContext] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        summary: dict[str, object] = {
            "name": self.name,
            "type": self.type,
            "brand_name": self.brand_name,
            "chain_name": self.chain_name,
            "star_rating": self.star_rating,
            "phone": self.phone,
            "address_line1": self.address_line1,
            "address_city": self.address_city,
            "address_state": self.address_state,
            "address_postal_code": self.address_postal_code,
            "address_country_code": self.address_country_code,
            "address_country_name": self.address_country_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "distance_miles": self.distance_miles,
            "distance_unit": self.distance_unit,
            "interests": list(self.interests),
            "amenities": list(self.amenities),
            "program_codes": list(self.program_codes),
            "program_benefits": [benefit.to_dict() for benefit in self.program_benefits],
            "check_in_start": self.check_in_start,
            "check_in_end": self.check_in_end,
            "check_out_time": self.check_out_time,
            "check_in_instructions": self.check_in_instructions,
            "check_out_instructions": self.check_out_instructions,
            "description_short": self.description_short,
            "description_long": self.description_long,
            "description_dining": self.description_dining,
            "description_amenities": self.description_amenities,
            "description_activities": self.description_activities,
            "description_accommodation": self.description_accommodation,
            "marketing_tags": list(self.marketing_tags),
            "marketing_insider_tip": self.marketing_insider_tip,
            "marketing_video": self.marketing_video,
            "hero_image": self.hero_image,
            "image_gallery": list(self.image_gallery),
            "images": list(self.images),
            "payment_options": list(self.payment_options),
            "loyalty_valid": self.loyalty_valid,
            "user_rating": self.user_rating,
            "user_rating_count": self.user_rating_count,
            "host_languages": list(self.host_languages),
            "location_teaser": self.location_teaser,
            "policies": list(self.policies),
            "no_show_policy": self.no_show_policy,
            "supplier_fees": list(self.supplier_fees),
            "renovation_closure_notice": self.renovation_closure_notice,
        }
        return {
            "property_id": self.property_id,
            "supplier_id": self.supplier_id,
            "summary": summary,
            "search": self.search.to_dict() if self.search else None,
            "raw": self.raw,
        }

    @classmethod
    def from_iterable(cls, records: Iterable["HotelRecord"]) -> List[dict[str, object]]:
        return [record.to_dict() for record in records]


@dataclass(slots=True)
class RatePricing:
    """Aggregate pricing information for a room rate."""

    currency: Optional[str]
    base: Optional[float]
    total: Optional[float]
    total_inclusive: Optional[float]
    total_fees: Optional[float]
    total_taxes: Optional[float]
    average_nightly_rate: Optional[float]
    average_nightly_rate_points_burn: Optional[float]
    nightly_actual_rates: List[float] = field(default_factory=list)
    nightly_inclusive_rates: List[float] = field(default_factory=list)
    payment_model: Optional[str] = None
    points_burn: Optional[int] = None
    points_burn_calculation: Optional[Dict[str, object]] = None
    fees: List[Dict[str, object]] = field(default_factory=list)
    taxes: List[Dict[str, object]] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "currency": self.currency,
            "base": self.base,
            "total": self.total,
            "total_inclusive": self.total_inclusive,
            "total_fees": self.total_fees,
            "total_taxes": self.total_taxes,
            "average_nightly_rate": self.average_nightly_rate,
            "average_nightly_rate_points_burn": self.average_nightly_rate_points_burn,
            "nightly_actual_rates": list(self.nightly_actual_rates),
            "nightly_inclusive_rates": list(self.nightly_inclusive_rates),
            "payment_model": self.payment_model,
            "points_burn": self.points_burn,
            "points_burn_calculation": self.points_burn_calculation,
            "fees": list(self.fees),
            "taxes": list(self.taxes),
        }


@dataclass(slots=True)
class HotelRateRecord:
    """Normalised rate data associated with a property and stay."""

    property_id: str
    location_id: str
    room_type_id: Optional[str]
    room_type_name: Optional[str]
    rate_id: Optional[str]
    hotel_collection: Optional[str]
    available: Optional[int]
    is_breakfast_included: Optional[bool]
    is_food_beverage_credit: Optional[bool]
    is_free_cancellation: Optional[bool]
    is_parking_included: Optional[bool]
    is_shuttle_included: Optional[bool]
    amenities: List[str] = field(default_factory=list)
    bed_groups: List[Dict[str, object]] = field(default_factory=list)
    cancel_penalties: List[Dict[str, object]] = field(default_factory=list)
    cancellation_policy_text: Optional[str] = None
    occupancy_adults: Optional[int] = None
    occupancy_children: Optional[int] = None
    room_count: Optional[int] = None
    pricing: Optional[RatePricing] = None
    room_allocations: List[Dict[str, object]] = field(default_factory=list)
    search: SearchContext = field(default=None)
    special_offer: Optional[Dict[str, Any]] = None
    supplier_rate_promotion: Optional[Dict[str, Any]] = None
    comparison_amenity: Optional[Dict[str, Any]] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        summary: dict[str, object] = {
            "hotel_collection": self.hotel_collection,
            "available": self.available,
            "is_breakfast_included": self.is_breakfast_included,
            "is_food_beverage_credit": self.is_food_beverage_credit,
            "is_free_cancellation": self.is_free_cancellation,
            "is_parking_included": self.is_parking_included,
            "is_shuttle_included": self.is_shuttle_included,
            "amenities": list(self.amenities),
            "bed_groups": list(self.bed_groups),
            "cancel_penalties": list(self.cancel_penalties),
            "cancellation_policy_text": self.cancellation_policy_text,
            "occupancy_adults": self.occupancy_adults,
            "occupancy_children": self.occupancy_children,
            "room_count": self.room_count,
            "pricing": self.pricing.to_dict() if self.pricing else None,
            "room_allocations": list(self.room_allocations),
            "special_offer": self.special_offer,
            "supplier_rate_promotion": self.supplier_rate_promotion,
            "comparison_amenity": self.comparison_amenity,
        }
        return {
            "property_id": self.property_id,
            "location_id": self.location_id,
            "room_type_id": self.room_type_id,
            "room_type_name": self.room_type_name,
            "rate_id": self.rate_id,
            "summary": summary,
            "search": self.search.to_dict(),
            # Persist promo metadata at the top level so downstream stores can access it directly.
            "special_offer": self.special_offer,
            "supplier_rate_promotion": self.supplier_rate_promotion,
            "comparison_amenity": self.comparison_amenity,
            "raw": self.raw,
        }

    @classmethod
    def from_iterable(cls, records: Iterable["HotelRateRecord"]) -> List[dict[str, object]]:
        return [record.to_dict() for record in records]
