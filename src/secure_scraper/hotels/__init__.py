"""Hotel domain models and normalization helpers."""

from .models import (
    HotelProgramBenefit,
    HotelRateRecord,
    HotelRecord,
    RatePricing,
    SearchContext,
)
from .normalizer import (
    build_hotel_and_rate_records,
    build_hotel_record,
    build_hotel_records,
    build_rate_records,
)

__all__ = [
    "HotelProgramBenefit",
    "HotelRateRecord",
    "HotelRecord",
    "RatePricing",
    "SearchContext",
    "build_hotel_and_rate_records",
    "build_hotel_record",
    "build_hotel_records",
    "build_rate_records",
]
