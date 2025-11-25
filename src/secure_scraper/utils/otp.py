"""Helpers for handling one-time passwords."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Protocol

import pyotp

logger = logging.getLogger(__name__)


def generate_totp(secret: str, *, timestamp: Optional[int] = None, interval: int = 30) -> str:
    """Generate a TOTP code for ``secret``.

    ``timestamp`` allows deterministic outputs for testing.
    """
    totp = pyotp.TOTP(secret, interval=interval)
    return totp.now() if timestamp is None else totp.at(timestamp)


class OtpCodeFetcher(Protocol):
    async def fetch_code(self) -> str:
        ...


class OtpResolver:
    """Resolves OTP challenges using TOTP or manual input."""

    def __init__(
        self,
        *,
        secret: Optional[str],
        prompt: bool = True,
        email_fetcher: Optional[OtpCodeFetcher] = None,
    ) -> None:
        self.secret = secret
        self.prompt = prompt
        self.email_fetcher = email_fetcher

    async def obtain_code(self) -> str:
        if self.secret:
            code = generate_totp(self.secret)
            logger.debug("Generated TOTP code via secret")
            return code
        if self.email_fetcher:
            try:
                code = await self.email_fetcher.fetch_code()
                logger.debug("Resolved OTP code via email fetcher")
                return code
            except Exception as exc:
                logger.exception("Email fetcher failed to obtain OTP code")
                if not self.prompt:
                    raise RuntimeError("Failed to obtain OTP via email fetcher") from exc
        if not self.prompt:
            raise RuntimeError("OTP secret not provided and prompting disabled")
        return await asyncio.to_thread(input, "Enter verification code: ")
