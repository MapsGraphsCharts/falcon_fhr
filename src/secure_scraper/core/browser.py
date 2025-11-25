"""Browser orchestration helpers."""
from __future__ import annotations

import logging
import os
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Type

from hyperbrowser import AsyncHyperbrowser
from hyperbrowser.models import CreateSessionParams
from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright

from secure_scraper.config.settings import Settings
from secure_scraper.core.fingerprint import apply_fingerprint_overrides
from secure_scraper.core.stealth import StealthManager

logger = logging.getLogger(__name__)


@dataclass
class BrowserSession:
    """Async context manager that owns Playwright + browser lifecycle."""

    settings: Settings
    _playwright_cm: Optional[AbstractAsyncContextManager] = None
    _playwright: Optional[Playwright] = None
    _browser: Optional[Browser] = None
    _persistent_context: Optional[BrowserContext] = None
    _stealth: Optional[StealthManager] = None
    _hyper_client: Optional[AsyncHyperbrowser] = None
    _hyper_session: Optional[object] = None

    async def __aenter__(self) -> "BrowserSession":  # noqa: D401
        self.settings.ensure_directories()
        if self.settings.persistent_context_enabled and not self.settings.hyperbrowser_enabled:
            self.settings.persistent_user_data_dir.mkdir(parents=True, exist_ok=True)

        if self.settings.hyperbrowser_enabled:
            self._stealth = None
            self._playwright_cm = async_playwright()
        else:
            self._stealth = StealthManager(
                self.settings.stealth_enabled,
                init_only=self.settings.stealth_init_scripts_only,
                **self.settings.stealth_kwargs(),
            )
            self._playwright_cm = self._stealth.wrap_playwright()
        self._playwright = await self._playwright_cm.__aenter__()

        if self.settings.hyperbrowser_enabled:
            await self._connect_hyperbrowser_session()
        elif not self.settings.persistent_context_enabled:
            launch_args = self.settings.chromium_launch_args()
            logger.info("Launching Chromium with args: %s", launch_args)
            self._browser = await self._playwright.chromium.launch(**launch_args)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._persistent_context:
            await self._close_persistent_context()
        if self._browser:
            await self._browser.close()
        if self.settings.hyperbrowser_enabled:
            await self._stop_hyperbrowser_session()
            self._hyper_client = None
        if self._playwright_cm:
            await self._playwright_cm.__aexit__(exc_type, exc, tb)

    @property
    def browser(self) -> Browser:
        if not self._browser:
            raise RuntimeError("Browser not initialised")
        return self._browser

    async def new_context(self, **overrides: object) -> BrowserContext:
        """Create a new context with stealth applied."""
        if self.settings.hyperbrowser_enabled:
            await self._connect_hyperbrowser_session()
            if not self._persistent_context:
                raise RuntimeError("Hyperbrowser context not initialised")
            return self._persistent_context
        options = {**self.settings.context_options(), **overrides}
        logger.debug("Creating context with options: %s", options)
        if self.settings.persistent_context_enabled:
            context = await self._launch_persistent_context(options)
        else:
            context = await self.browser.new_context(**options)
        context = await self._prepare_context(context)
        return context

    async def _prepare_context(self, context: BrowserContext) -> BrowserContext:
        if self._stealth:
            await self._stealth.apply(context)
        if not self.settings.hyperbrowser_enabled:
            await apply_fingerprint_overrides(context, self.settings)
            if self.settings.fingerprint_disable_client_hints:
                await context.route("**/*", self._strip_client_hints_headers)
        context.set_default_timeout(self.settings.default_timeout_ms)
        context.set_default_navigation_timeout(self.settings.navigation_timeout_ms)
        return context

    async def new_page(self, **overrides: object):
        context = await self.new_context(**overrides)
        return await context.new_page()

    async def _strip_client_hints_headers(self, route, request) -> None:
        headers = {
            key: value
            for key, value in request.headers.items()
            if not key.lower().startswith("sec-ch-")
        }
        await route.continue_(headers=headers)

    async def _launch_persistent_context(self, options: dict[str, object]) -> BrowserContext:
        if not self._playwright:
            raise RuntimeError("Playwright not initialised")
        await self._close_persistent_context()
        persistent_options = {**self.settings.chromium_launch_args(), **options}
        if self.settings.chromium_no_viewport:
            persistent_options["no_viewport"] = True
            persistent_options.pop("viewport", None)
        user_data_dir = self.settings.persistent_user_data_dir
        logger.info(
            "Launching persistent Chromium profile at %s with args: %s",
            user_data_dir,
            {k: v for k, v in persistent_options.items() if k != "args"},
        )
        channel = persistent_options.get("channel")
        try:
            context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=str(user_data_dir),
                **persistent_options,
            )
        except Exception as exc:
            if channel:
                logger.warning(
                    "Failed to launch persistent context with channel '%s': %s; retrying without channel",
                    channel,
                    exc,
                )
                fallback_options = {k: v for k, v in persistent_options.items() if k != "channel"}
                context = await self._playwright.chromium.launch_persistent_context(
                    user_data_dir=str(user_data_dir),
                    **fallback_options,
                )
            else:
                raise
        self._persistent_context = context
        return context

    async def _close_persistent_context(self) -> None:
        if not self._persistent_context:
            return
        try:
            await self._persistent_context.close()
        except Exception:  # pragma: no cover - best effort cleanup
            logger.exception("Failed to close persistent context")
        finally:
            self._persistent_context = None

    async def _connect_hyperbrowser_session(self) -> None:
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._persistent_context:
            await self._close_persistent_context()
        await self._stop_hyperbrowser_session()
        api_key = self.settings.hyperbrowser_api_key or os.getenv("HYPERBROWSER_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Hyperbrowser is enabled but no API key was provided. Set HYPERBROWSER_API_KEY or"
                " configure settings.hyperbrowser_api_key."
            )
        self._hyper_client = AsyncHyperbrowser(api_key=api_key)
        params: dict[str, object] = {
            "use_stealth": self.settings.hyperbrowser_use_stealth,
            "accept_cookies": self.settings.hyperbrowser_accept_cookies,
        }
        if self.settings.hyperbrowser_region:
            params["region"] = self.settings.hyperbrowser_region
        create_params = CreateSessionParams(**params)
        self._hyper_session = await self._hyper_client.sessions.create(params=create_params)
        logger.info("Connected to Hyperbrowser session %s", self._hyper_session.id)
        self._browser = await self._playwright.chromium.connect_over_cdp(self._hyper_session.ws_endpoint)
        default_context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
        self._persistent_context = await self._prepare_context(default_context)

    async def _stop_hyperbrowser_session(self) -> None:
        if self._hyper_client and self._hyper_session:
            try:
                await self._hyper_client.sessions.stop(self._hyper_session.id)
            except Exception:  # pragma: no cover - best effort cleanup
                logger.warning(
                    "Failed to stop Hyperbrowser session %s",
                    getattr(self._hyper_session, "id", "<unknown>"),
                )
        self._hyper_session = None


async def ensure_close_context(context: BrowserContext) -> None:
    """Helper to close contexts in finally blocks."""
    try:
        await context.close()
    except Exception:  # pragma: no cover - best effort cleanup
        logger.exception("Failed to close context")
