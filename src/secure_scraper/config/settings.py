"""Runtime configuration for the scraper.

Relies on pydantic-settings so that environment variables (prefixed with ``SCRAPER_``)
can override defaults. See `.env.example` for common values.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

DEFAULT_CHROMIUM_PLUGINS: tuple[dict[str, object], ...] = (
    {
        "name": "Chrome PDF Viewer",
        "filename": "internal-pdf-viewer",
        "description": "Portable Document Format",
        "mime_types": (
            {"type": "application/pdf", "suffixes": "pdf", "description": "Portable Document Format"},
            {
                "type": "application/x-google-chrome-pdf",
                "suffixes": "pdf",
                "description": "Portable Document Format",
            },
        ),
    },
)

DEFAULT_FIREFOX_PLUGINS: tuple[dict[str, object], ...] = ()


class Settings(BaseSettings):
    """Captures runtime configuration for the scraper."""

    username: Optional[str] = Field(default=None, description="Primary account username")
    password: Optional[str] = Field(default=None, description="Primary account password")
    mfa_secret: Optional[str] = None
    fastmail_api_token: Optional[str] = Field(
        default=None,
        description="Fastmail JMAP API token used for OTP email retrieval",
    )
    fastmail_mailbox: Optional[str] = Field(
        default="inbox",
        description="Fastmail mailbox role or name to poll for OTP emails",
    )
    fastmail_sender_filter: Optional[str] = Field(
        default="AmericanExpress@welcome.americanexpress.com",
        description="Restrict OTP email polling to this sender address when provided",
    )
    fastmail_subject_pattern: Optional[str] = Field(
        default=r"Your American Express one-time verification code",
        description="Regex applied to the email subject when searching for OTP codes",
    )
    fastmail_code_pattern: str = Field(
        default=r"\b(\d{6})\b",
        description="Regex capture group used to extract OTP codes from email subject/body",
    )
    fastmail_poll_interval_s: float = Field(
        default=5.0, description="Seconds between Fastmail mailbox polling attempts for OTP emails"
    )
    fastmail_timeout_s: float = Field(
        default=120.0, description="Maximum seconds to wait for an OTP email before failing"
    )
    fastmail_recent_window_s: float = Field(
        default=900.0, description="Accept OTP emails received within this many seconds"
    )
    fastmail_message_limit: int = Field(
        default=10, description="Number of recent Fastmail messages to inspect per poll"
    )

    base_url: str = Field(
        default="https://www.amextravel.com",
        description="Target base URL for login/search flows",
    )
    headless: bool = True
    slow_mo_ms: int = Field(default=0, description="Slow-mo delay in milliseconds")
    viewport_width: int = 1280
    viewport_height: int = 720
    device_scale_factor: Optional[float] = None
    user_agent: Optional[str] = None
    download_dir: Path = Field(default=Path("data/downloads"))
    log_level: str = Field(default="INFO")
    default_timeout_ms: int = Field(default=15000)
    navigation_timeout_ms: int = Field(default=30000)
    destination_catalog_path: Path = Field(
        default=Path("data/destinations/catalog.json"), description="Path to destination catalog metadata"
    )

    stealth_enabled: bool = Field(default=True, description="Apply playwright-stealth evasions")
    stealth_init_scripts_only: bool = False
    stealth_languages: Optional[Tuple[str, str]] = None
    stealth_platform: Optional[str] = None
    stealth_user_agent: Optional[str] = None

    persistent_context_enabled: bool = Field(
        default=True,
        description="Use launch_persistent_context with a Chrome channel and user data dir",
    )
    persistent_user_data_dir: Path = Field(
        default=Path("data/chrome-profile"),
        description="Directory that stores the persistent Chrome profile when enabled",
    )
    chromium_channel: Optional[str] = Field(
        default=None,
        description="Browser channel passed to Patchright (e.g. 'chrome'); use None for bundled Chromium",
    )
    chromium_no_viewport: bool = Field(
        default=True,
        description="When using persistent context, let Chrome manage its native viewport",
    )
    chromium_args: Tuple[str, ...] = Field(
        default=("--disable-blink-features=AutomationControlled",),
        description="Extra Chromium args passed during launch",
    )

    storage_state_path: Optional[Path] = None

    search_location_id: str = Field(
        default="ZMETRO-EXPEDIA-179899", description="Default location identifier for searches"
    )
    search_location_name: str = Field(
        default="Rome (and vicinity), Lazio, Italy", description="Human readable label for search destination"
    )
    search_latitude: float = Field(default=41.903755, description="Destination latitude")
    search_longitude: float = Field(default=12.479556, description="Destination longitude")
    search_check_in: Optional[date] = None
    search_nights: int = Field(default=3, description="Length of stay in nights")
    search_adults: int = Field(default=2, description="Adults per room")
    search_program_filter: Tuple[str, ...] = Field(
        default=(), description="Optional program filters (e.g. 'FHR', 'THC')"
    )
    search_destination_keys: Tuple[str, ...] = Field(
        default=(), description="Destination catalog keys to run; comma-separated when provided via env"
    )
    search_warmup_enabled: bool = Field(
        default=False,
        description="If true, wait for warm-up properties payload via search redirect page",
    )
    login_monitor_markers: bool = Field(
        default=False,
        description="If true, wait for credentials-signin/auth session network markers during login",
    )

    fingerprint_enabled: bool = Field(
        default=True, description="If true apply custom fingerprint overrides in browser context"
    )
    fingerprint_user_agent: Optional[str] = Field(
        default="Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0",
        description="User agent string to present to pages",
    )
    fingerprint_platform: Optional[str] = Field(default="Linux x86_64")
    fingerprint_language: Optional[str] = Field(default="en-US")
    fingerprint_languages: Tuple[str, ...] = Field(default=("en-US", "en"))
    fingerprint_hardware_concurrency: Optional[int] = Field(default=16)
    fingerprint_max_touch_points: int = Field(default=0)
    fingerprint_vendor: Optional[str] = Field(default="")
    fingerprint_product_sub: Optional[str] = Field(default="20100101")
    fingerprint_do_not_track: Optional[str] = Field(default="unspecified")
    fingerprint_screen_width: Optional[int] = Field(default=3072)
    fingerprint_screen_height: Optional[int] = Field(default=1728)
    fingerprint_screen_avail_width: Optional[int] = Field(default=3072)
    fingerprint_screen_avail_height: Optional[int] = Field(default=1696)
    fingerprint_color_depth: Optional[int] = Field(default=24)
    fingerprint_pixel_depth: Optional[int] = Field(default=24)
    fingerprint_window_inner_width: Optional[int] = Field(default=3072)
    fingerprint_window_inner_height: Optional[int] = Field(default=568)
    fingerprint_window_outer_width: Optional[int] = Field(default=3072)
    fingerprint_window_outer_height: Optional[int] = Field(default=1696)
    fingerprint_device_pixel_ratio: Optional[float] = Field(default=2.0)
    fingerprint_device_memory: Optional[float] = Field(
        default=None, description="navigator.deviceMemory value; use None to hide the property"
    )
    fingerprint_oscpu: Optional[str] = Field(default=None, description="Value exposed via navigator.oscpu")
    fingerprint_plugins: Tuple[dict[str, object], ...] = Field(
        default=DEFAULT_CHROMIUM_PLUGINS,
        description="Plugin metadata exposed via navigator.plugins",
    )
    fingerprint_webgl_vendor: Optional[str] = Field(default="NVIDIA Corporation")
    fingerprint_webgl_renderer: Optional[str] = Field(default="NVIDIA GeForce GTX 980, or similar")
    fingerprint_canvas_fingerprint: Optional[str] = Field(
        default=None, description="Canvas fingerprint to return when toDataURL is called without arguments"
    )
    fingerprint_disable_client_hints: bool = Field(
        default=True,
        description="Strip navigator.userAgentData and Sec-CH-* headers for Firefox-style fingerprints",
    )

    model_config = SettingsConfigDict(
        env_prefix="SCRAPER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",
    )

    @field_validator("download_dir", mode="before")
    def _expand_download_dir(cls, value: str | Path) -> Path:  # noqa: D401
        if isinstance(value, Path):
            return value
        return Path(value).expanduser()

    @field_validator("storage_state_path", mode="before")
    def _expand_storage_state(cls, value: str | Path | None) -> Optional[Path]:
        if value in (None, ""):
            return None
        return Path(value).expanduser()

    @field_validator("destination_catalog_path", mode="before")
    def _expand_catalog_path(cls, value: str | Path) -> Path:
        if isinstance(value, Path):
            return value
        return Path(value).expanduser()

    @field_validator("persistent_user_data_dir", mode="before")
    def _expand_profile_dir(cls, value: str | Path) -> Path:
        if isinstance(value, Path):
            return value
        return Path(value).expanduser()

    @field_validator("search_check_in", mode="before")
    def _parse_search_check_in(cls, value: str | date | None) -> Optional[date]:
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)

    @field_validator("search_nights")
    def _validate_nights(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("search_nights must be positive")
        return value

    @field_validator("search_destination_keys", mode="before")
    def _parse_destination_keys(cls, value: object) -> Tuple[str, ...]:
        if value is None or value == "":
            return ()
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(str(item) for item in value if str(item))
        if isinstance(value, str):
            keys: Iterable[str] = (key.strip() for key in value.split(","))
            return tuple(key for key in keys if key)
        raise TypeError("search_destination_keys must be provided as a comma-separated string or list")

    @field_validator("search_program_filter", mode="before")
    def _parse_program_filter(cls, value: object) -> Tuple[str, ...]:
        if value is None or value == "":
            return ()
        if isinstance(value, tuple):
            return tuple(str(item).strip() for item in value if str(item).strip())
        if isinstance(value, list):
            return tuple(str(item).strip() for item in value if str(item).strip())
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",")]
            return tuple(part for part in parts if part)
        raise TypeError("search_program_filter must be provided as a comma-separated string or list")

    @field_validator("fingerprint_languages", mode="before")
    def _parse_fingerprint_languages(cls, value: object) -> Tuple[str, ...]:
        if value is None or value == "":
            return ()
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(str(item) for item in value if str(item))
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",")]
            return tuple(part for part in parts if part)
        raise TypeError("fingerprint_languages must be provided as a comma-separated string or list")

    @field_validator("fingerprint_plugins", mode="before")
    def _parse_fingerprint_plugins(cls, value: object) -> Tuple[dict[str, object], ...]:
        if value in (None, "", ()):
            return ()
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:  # noqa: TRY003
                raise ValueError("fingerprint_plugins must be valid JSON") from exc
            value = parsed
        if isinstance(value, dict):
            value = [value]
        if not isinstance(value, (list, tuple)):
            raise TypeError("fingerprint_plugins must be a sequence of plugin definitions")
        plugins: list[dict[str, object]] = []
        for entry in value:
            if not isinstance(entry, dict):
                raise TypeError("Each fingerprint plugin must be a mapping")
            mime_raw = entry.get("mime_types") or entry.get("mimes") or ()
            if isinstance(mime_raw, str):
                try:
                    mime_raw = json.loads(mime_raw)
                except json.JSONDecodeError:
                    mime_raw = [mime_raw]
            if isinstance(mime_raw, dict):
                mime_raw = [mime_raw]
            if not isinstance(mime_raw, (list, tuple)):
                mime_raw = []
            mime_types: list[dict[str, str]] = []
            for item in mime_raw:
                if not isinstance(item, dict):
                    continue
                mime_types.append(
                    {
                        "type": str(item.get("type", "")),
                        "suffixes": str(item.get("suffixes", "")),
                        "description": str(item.get("description", "")),
                    }
                )
            plugins.append(
                {
                    "name": str(entry.get("name", "")),
                    "filename": str(entry.get("filename", "")),
                    "description": str(entry.get("description", "")),
                    "mime_types": tuple(mime_types),
                }
            )
        return tuple(plugins)

    def ensure_directories(self) -> None:
        """Create directories that must exist at runtime."""
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def viewport(self) -> dict[str, int]:
        return {"width": self.viewport_width, "height": self.viewport_height}

    def chromium_launch_args(self) -> dict[str, object]:
        launch_args: dict[str, object] = {
            "headless": self.headless,
        }
        if self.slow_mo_ms:
            launch_args["slow_mo"] = self.slow_mo_ms
        if self.chromium_channel:
            launch_args["channel"] = self.chromium_channel
        if self.chromium_args:
            launch_args["args"] = list(self.chromium_args)
        return launch_args

    def context_options(self) -> dict[str, object]:
        options: dict[str, object] = {
            "base_url": self.base_url,
        }
        viewport_disabled = self.persistent_context_enabled and self.chromium_no_viewport
        if not viewport_disabled:
            options["viewport"] = self.viewport()
        if self.device_scale_factor is not None and not viewport_disabled:
            options["device_scale_factor"] = self.device_scale_factor
        user_agent = self.user_agent or (self.fingerprint_user_agent if self.fingerprint_enabled else None)
        if user_agent:
            options["user_agent"] = user_agent
        if self.fingerprint_language:
            options["locale"] = self.fingerprint_language
        if (
            self.fingerprint_screen_width is not None
            and self.fingerprint_screen_height is not None
            and self.fingerprint_enabled
        ):
            options["screen"] = {
                "width": self.fingerprint_screen_width,
                "height": self.fingerprint_screen_height,
            }
        options["is_mobile"] = False
        options["has_touch"] = bool(self.fingerprint_max_touch_points)
        if self.storage_state_path:
            if self.storage_state_path.exists():
                if self.persistent_context_enabled:
                    logger.info(
                        "Persistent context enabled; ignoring storage state %s (profile will manage cookies)",
                        self.storage_state_path,
                    )
                else:
                    logger.info("Loading storage state from %s", self.storage_state_path)
                    options["storage_state"] = str(self.storage_state_path)
            else:
                logger.debug(
                    "Storage state path %s not found; proceeding without preloaded session",
                    self.storage_state_path,
                )
        return options

    def stealth_kwargs(self) -> dict[str, object]:
        if not self.stealth_enabled:
            return {}
        kwargs: dict[str, object] = {
            "init_scripts_only": self.stealth_init_scripts_only,
        }
        if self.stealth_languages:
            kwargs["navigator_languages_override"] = self.stealth_languages
        if self.stealth_platform:
            kwargs["navigator_platform_override"] = self.stealth_platform
        if self.stealth_user_agent:
            kwargs["navigator_user_agent_override"] = self.stealth_user_agent
        return kwargs

    def fingerprint_overrides(self):  # noqa: D401
        """Return fingerprint overrides if configured."""
        if not self.fingerprint_enabled:
            return None
        from secure_scraper.core.fingerprint import FingerprintOverrides, PluginOverride

        plugin_source = self.fingerprint_plugins
        ua = self.fingerprint_user_agent or self.user_agent or ""
        if plugin_source == DEFAULT_CHROMIUM_PLUGINS and "firefox" in ua.lower():
            plugin_source = DEFAULT_FIREFOX_PLUGINS
        plugins = tuple(PluginOverride(**plugin) for plugin in plugin_source)
        languages = self.fingerprint_languages or (
            (self.fingerprint_language,) if self.fingerprint_language else ()
        )

        return FingerprintOverrides(
            user_agent=self.fingerprint_user_agent or self.user_agent,
            platform=self.fingerprint_platform,
            language=self.fingerprint_language,
            languages=languages,
            hardware_concurrency=self.fingerprint_hardware_concurrency,
            max_touch_points=self.fingerprint_max_touch_points,
            vendor=self.fingerprint_vendor,
            product_sub=self.fingerprint_product_sub,
            do_not_track=self.fingerprint_do_not_track,
            screen_width=self.fingerprint_screen_width,
            screen_height=self.fingerprint_screen_height,
            screen_avail_width=self.fingerprint_screen_avail_width,
            screen_avail_height=self.fingerprint_screen_avail_height,
            color_depth=self.fingerprint_color_depth,
            pixel_depth=self.fingerprint_pixel_depth,
            window_inner_width=self.fingerprint_window_inner_width,
            window_inner_height=self.fingerprint_window_inner_height,
            window_outer_width=self.fingerprint_window_outer_width,
            window_outer_height=self.fingerprint_window_outer_height,
            device_pixel_ratio=self.fingerprint_device_pixel_ratio,
            plugins=plugins,
            webgl_vendor=self.fingerprint_webgl_vendor,
            webgl_renderer=self.fingerprint_webgl_renderer,
            canvas_fingerprint=self.fingerprint_canvas_fingerprint,
            disable_client_hints=self.fingerprint_disable_client_hints,
            device_memory=self.fingerprint_device_memory,
            oscpu=self.fingerprint_oscpu,
        )
