"""Utilities for building Amex Travel search payloads."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass
class RoomRequest:
    adults: int
    children: List[int] = field(default_factory=list)


@dataclass
class SearchParams:
    location_id: str
    location_label: str
    latitude: float
    longitude: float
    check_in: date
    check_out: date
    rooms: List[RoomRequest]
    page: int = 1
    page_size: int = 50
    sort_option: str = "RECOMMENDED"
    sort_direction: str = "DESC"
    program_filter: Optional[List[str]] = None

    def to_payload(self) -> dict:
        return {
            "pagination": {"page": self.page, "pageSize": self.page_size},
            "sortOptions": [{"direction": self.sort_direction, "option": self.sort_option}],
            "checkIn": self.check_in.isoformat(),
            "checkOut": self.check_out.isoformat(),
            "location": self.location_id,
            "locationType": "LOCATION_ID",
            "rooms": [
                {"adults": room.adults, "children": room.children}
                if room.children
                else {"adults": room.adults}
                for room in self.rooms
            ],
            **(
                {
                    "filters": {
                        "clientProgramFilter": list(self.program_filter),
                    }
                }
                if self.program_filter
                else {}
            ),
        }
