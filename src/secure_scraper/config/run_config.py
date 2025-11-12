"""User-friendly run configuration loader for manual runs."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator, model_validator

try:  # pragma: no cover - Python 3.11+ ships tomllib
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - fallback for <3.11
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "Run configuration loading requires 'tomllib' (Python >=3.11) or the 'tomli' package."
        ) from exc

if TYPE_CHECKING:  # pragma: no cover
    from secure_scraper.config.settings import Settings

_RELATIVE_CHECK_IN = re.compile(r"^(?P<count>\d+)\s*(?P<unit>[dDwWmM])$")


def _coerce_string_list(value: object) -> list[str]:
    if value in (None, "", ()):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result
    raise TypeError("Expected string or list of strings")


class SearchSection(BaseModel):
    """Search-specific overrides decoded from the run config."""

    check_in: Optional[str] = Field(
        default=None, description="ISO 8601 date or relative offset such as '+14d'"
    )
    check_in_offset_days: Optional[int] = Field(
        default=None,
        description="Relative offset (in days) applied when `check_in` is not provided",
    )
    nights: Optional[int] = Field(default=None, ge=1)
    adults: Optional[int] = Field(default=None, ge=1)
    destinations: list[str] = Field(default_factory=list)
    program_filter: list[str] = Field(default_factory=list)

    @field_validator("destinations", mode="before")
    @classmethod
    def _coerce_destinations(cls, value: object) -> list[str]:
        return _coerce_string_list(value)

    @field_validator("program_filter", mode="before")
    @classmethod
    def _coerce_program_filter(cls, value: object) -> list[str]:
        return _coerce_string_list(value)


class BrowserSection(BaseModel):
    """Browser/runtime overrides decoded from the run config."""

    headless: Optional[bool] = None
    slow_mo_ms: Optional[int] = Field(default=None, ge=0)
    viewport_width: Optional[int] = Field(default=None, ge=0)
    viewport_height: Optional[int] = Field(default=None, ge=0)
    device_scale_factor: Optional[float] = Field(default=None, ge=0)
    log_level: Optional[str] = None


class StorageSection(BaseModel):
    """Structured storage overrides (e.g. SQLite persistence)."""

    sqlite_enabled: Optional[bool] = Field(
        default=None, description="Toggle SQLite storage for hotel/rate snapshots"
    )
    sqlite_path: Optional[str] = Field(
        default=None, description="Override the SQLite file path"
    )
    sqlite_busy_timeout_ms: Optional[int] = Field(
        default=None, description="Override SQLite busy timeout (ms) for locks"
    )
    sqlite_journal_mode: Optional[str] = Field(
        default=None,
        description="Override SQLite journal_mode (e.g., 'wal', 'delete')",
    )
    sqlite_synchronous: Optional[str] = Field(
        default=None,
        description="Override SQLite synchronous PRAGMA (e.g., 'normal', 'full')",
    )


class ManualDestinationSection(BaseModel):
    """Optional manual destination override."""

    name: Optional[str] = None
    location_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @field_validator("name", "location_id", mode="before")
    @classmethod
    def _blank_to_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value


@dataclass(frozen=True)
class DateSweep:
    """Represents a single check-in iteration when using date ranges."""

    check_in: date
    nights: Optional[int] = None
    label: Optional[str] = None


class DateRangeSection(BaseModel):
    """Defines a series of check-in dates to iterate."""

    start: str = Field(description="ISO date or relative offset for the first check-in")
    end: Optional[str] = Field(
        default=None,
        description="ISO date or relative offset for the final check-in (inclusive)",
    )
    occurrences: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of iterations when end is not provided",
    )
    step_days: int = Field(default=1, ge=1, description="Days between each check-in")
    nights: Optional[int] = Field(
        default=None,
        ge=1,
        description="Override stay length (nights) for each iteration",
    )

    @model_validator(mode="after")
    def _validate_bounds(cls, values: "DateRangeSection") -> "DateRangeSection":
        if values.end is None and values.occurrences is None:
            raise ValueError("date_range requires either 'end' or 'occurrences'")
        return values

    def generate(self) -> list[DateSweep]:
        start_date = _parse_check_in(self.start)
        end_date = _parse_check_in(self.end) if self.end else None
        occurrences = self.occurrences
        current = start_date
        generated = 0
        sweeps: list[DateSweep] = []
        while True:
            if end_date and current > end_date:
                break
            if occurrences and generated >= occurrences:
                break
            sweeps.append(
                DateSweep(
                    check_in=current,
                    nights=self.nights,
                    label=current.isoformat(),
                )
            )
            generated += 1
            current = current + timedelta(days=self.step_days)
        return sweeps


class RunConfig(BaseModel):
    """Top-level configuration decoded from TOML."""

    profile: str = Field(default="default", description="Human label used for logging")
    title: Optional[str] = None
    notes: Optional[str] = None
    search: SearchSection = Field(default_factory=SearchSection)
    browser: BrowserSection = Field(default_factory=BrowserSection)
    manual_destination: Optional[ManualDestinationSection] = None
    storage: Optional[StorageSection] = None
    destination_catalog_path: Optional[str] = None
    storage_state_path: Optional[str] = None
    date_range: Optional[DateRangeSection] = None

    @classmethod
    def load(cls, path: Path) -> "RunConfig":
        """Load a config from a TOML file."""
        data = tomllib.loads(path.read_text())
        return cls.model_validate(data)

    # Public API -----------------------------------------------------------------

    def apply_to(self, settings: "Settings", *, base_dir: Optional[Path] = None) -> None:
        """Apply overrides to an existing Settings instance."""
        self._apply_search(settings)
        self._apply_browser(settings)
        self._apply_storage(settings, base_dir)
        self._apply_manual_destination(settings)
        self._apply_paths(settings, base_dir)

    # Internal helpers -----------------------------------------------------------

    def _apply_search(self, settings: "Settings") -> None:
        search = self.search
        if search.check_in:
            settings.search_check_in = _parse_check_in(search.check_in)
        elif search.check_in_offset_days is not None:
            settings.search_check_in = date.today() + timedelta(days=search.check_in_offset_days)

        if search.nights is not None:
            settings.search_nights = search.nights

        if search.adults is not None:
            settings.search_adults = search.adults

        if search.destinations:
            settings.search_destination_keys = tuple(search.destinations)

        if search.program_filter:
            settings.search_program_filter = tuple(search.program_filter)
        elif search.program_filter == []:
            settings.search_program_filter = ()

    def _apply_browser(self, settings: "Settings") -> None:
        browser = self.browser
        if browser.headless is not None:
            settings.headless = browser.headless
        if browser.slow_mo_ms is not None:
            settings.slow_mo_ms = browser.slow_mo_ms
        if browser.viewport_width is not None:
            settings.viewport_width = browser.viewport_width
        if browser.viewport_height is not None:
            settings.viewport_height = browser.viewport_height
        if browser.device_scale_factor is not None:
            settings.device_scale_factor = browser.device_scale_factor
        if browser.log_level:
            settings.log_level = browser.log_level

    def _apply_storage(self, settings: "Settings", base_dir: Optional[Path]) -> None:
        storage = self.storage
        if not storage:
            return
        if storage.sqlite_enabled is not None:
            settings.sqlite_storage_enabled = storage.sqlite_enabled
        if storage.sqlite_path:
            settings.sqlite_storage_path = _resolve_path(storage.sqlite_path, base_dir)
        if storage.sqlite_busy_timeout_ms is not None:
            settings.sqlite_busy_timeout_ms = storage.sqlite_busy_timeout_ms
        if storage.sqlite_journal_mode is not None:
            settings.sqlite_journal_mode = storage.sqlite_journal_mode
        if storage.sqlite_synchronous is not None:
            settings.sqlite_synchronous = storage.sqlite_synchronous

    def _apply_manual_destination(self, settings: "Settings") -> None:
        manual = self.manual_destination
        if not manual:
            return
        if manual.name is not None:
            settings.search_location_name = manual.name
        if manual.location_id is not None:
            settings.search_location_id = manual.location_id
        if manual.latitude is not None:
            settings.search_latitude = manual.latitude
        if manual.longitude is not None:
            settings.search_longitude = manual.longitude

    def _apply_paths(self, settings: "Settings", base_dir: Optional[Path]) -> None:
        if self.destination_catalog_path:
            settings.destination_catalog_path = _resolve_path(self.destination_catalog_path, base_dir)
        if self.storage_state_path:
            settings.storage_state_path = _resolve_path(self.storage_state_path, base_dir)

    def date_sweeps(self) -> list[DateSweep]:
        if not self.date_range:
            return []
        return self.date_range.generate()


def _resolve_path(raw: str, base_dir: Optional[Path]) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute() and base_dir:
        return (base_dir / path).resolve()
    return path


def _parse_check_in(value: str) -> date:
    text = value.strip()
    lowered = text.lower()
    if lowered == "today":
        return date.today()
    if lowered.startswith("today+"):
        text = f"+{text.split('+', 1)[1]}"
        lowered = text.lower()
    if lowered.startswith("+"):
        match = _RELATIVE_CHECK_IN.match(lowered[1:])
        if not match:
            raise ValueError(
                f"Unsupported check_in relative format '{value}'. Use forms like '+14d', '+2w', '+1m'."
            )
        count = int(match.group("count"))
        unit = match.group("unit").lower()
        if unit == "d":
            delta = timedelta(days=count)
        elif unit == "w":
            delta = timedelta(weeks=count)
        else:
            # Treat months as 30-day blocks to avoid external dependencies.
            delta = timedelta(days=30 * count)
        return date.today() + delta
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid check_in date '{value}'. Provide ISO format (YYYY-MM-DD) or a relative offset."
        ) from exc


__all__ = ["RunConfig"]
