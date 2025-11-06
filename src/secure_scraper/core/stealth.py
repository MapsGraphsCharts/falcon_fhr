"""Stealth helpers built on top of playwright-stealth.

See: https://github.com/mattwmaster58/playwright_stealth
"""
from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from playwright.async_api import BrowserContext
from playwright.async_api import async_playwright
from playwright_stealth import ALL_EVASIONS_DISABLED_KWARGS, Stealth


class StealthManager:
    """Wraps the Stealth helper to make usage explicit."""

    def __init__(self, enabled: bool, *, init_only: bool = False, **overrides: object) -> None:
        self.enabled = enabled
        self._overrides = overrides
        self._init_only = init_only
        self._stealth = Stealth(
            **{
                **({} if enabled else ALL_EVASIONS_DISABLED_KWARGS),
                "init_scripts_only": init_only,
                **overrides,
            }
        )

    def wrap_playwright(self) -> AbstractAsyncContextManager:
        """Return the context manager to acquire Playwright."""
        if not self.enabled:
            return async_playwright()
        return self._stealth.use_async(async_playwright())

    async def apply(self, context: BrowserContext) -> None:
        """Apply stealth evasions to a browser context."""
        if not self.enabled:
            return
        await self._stealth.apply_stealth_async(context)

    def describe(self) -> dict[str, object]:
        """Expose current stealth configuration for debugging/logging."""
        if not self.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "init_scripts_only": self._init_only,
            "overrides": self._overrides,
            "evasions": len(self._stealth.script_payload),
        }
