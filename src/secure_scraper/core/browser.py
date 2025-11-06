"""Browser orchestration helpers."""
from __future__ import annotations

import logging
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Type

from playwright.async_api import Browser, BrowserContext, Playwright

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
    _stealth: Optional[StealthManager] = None

    async def __aenter__(self) -> "BrowserSession":  # noqa: D401
        self.settings.ensure_directories()

        self._stealth = StealthManager(
            self.settings.stealth_enabled,
            init_only=self.settings.stealth_init_scripts_only,
            **self.settings.stealth_kwargs(),
        )
        self._playwright_cm = self._stealth.wrap_playwright()
        self._playwright = await self._playwright_cm.__aenter__()

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
        if self._browser:
            await self._browser.close()
        if self._playwright_cm:
            await self._playwright_cm.__aexit__(exc_type, exc, tb)

    @property
    def browser(self) -> Browser:
        if not self._browser:
            raise RuntimeError("Browser not initialised")
        return self._browser

    async def new_context(self, **overrides: object) -> BrowserContext:
        """Create a new context with stealth applied."""
        options = {**self.settings.context_options(), **overrides}
        logger.debug("Creating context with options: %s", options)
        context = await self.browser.new_context(**options)
        if self._stealth:
            await self._stealth.apply(context)
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


async def ensure_close_context(context: BrowserContext) -> None:
    """Helper to close contexts in finally blocks."""
    try:
        await context.close()
    except Exception:  # pragma: no cover - best effort cleanup
        logger.exception("Failed to close context")
