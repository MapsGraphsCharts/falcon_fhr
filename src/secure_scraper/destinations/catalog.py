"""Destination catalog helpers."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class Destination:
    """Represents a logical search destination."""

    key: str
    group: str
    name: str
    location_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def missing_fields(self) -> List[str]:
        missing: List[str] = []
        if not self.location_id:
            missing.append("location_id")
        if self.latitude is None:
            missing.append("latitude")
        if self.longitude is None:
            missing.append("longitude")
        return missing

    def is_ready(self) -> bool:
        return not self.missing_fields()


class DestinationCatalog:
    """Loads destination metadata from disk."""

    def __init__(self, destinations: Mapping[str, Destination], *, source: Path) -> None:
        self._destinations = destinations
        self._source = source

    @property
    def source(self) -> Path:
        return self._source

    def get(self, key: str) -> Destination:
        try:
            return self._destinations[key]
        except KeyError as exc:  # pragma: no cover - defensive
            known = ", ".join(sorted(self._destinations))
            raise KeyError(f"Destination '{key}' not found in catalog {self._source}. Known keys: {known}") from exc

    def values(self) -> Iterable[Destination]:
        return self._destinations.values()

    @classmethod
    def load(cls, path: Path) -> "DestinationCatalog":
        if not path.exists():
            raise FileNotFoundError(f"Destination catalog not found at {path}")
        data = json.loads(path.read_text())
        entries = data.get("destinations", [])
        destinations: dict[str, Destination] = {}
        for entry in entries:
            destination = Destination(
                key=entry["key"],
                group=entry.get("group", ""),
                name=entry.get("name", entry["key"]),
                location_id=entry.get("location_id"),
                latitude=entry.get("latitude"),
                longitude=entry.get("longitude"),
            )
            destinations[destination.key] = destination
        return cls(destinations, source=path)

