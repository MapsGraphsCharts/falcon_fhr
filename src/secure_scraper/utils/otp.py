"""Helpers for handling one-time passwords."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import pyotp

logger = logging.getLogger(__name__)


def generate_totp(secret: str, *, timestamp: Optional[int] = None, interval: int = 30) -> str:
    """Generate a TOTP code for ``secret``.

    ``timestamp`` allows deterministic outputs for testing.
    """
    totp = pyotp.TOTP(secret, interval=interval)
    return totp.now() if timestamp is None else totp.at(timestamp)


class OtpResolver:
    """Resolves OTP challenges using TOTP or manual input."""

    def __init__(self, *, secret: Optional[str], prompt: bool = True) -> None:
        self.secret = secret
        self.prompt = prompt

    async def obtain_code(self) -> str:
        if self.secret:
            code = generate_totp(self.secret)
            logger.debug("Generated TOTP code via secret")
            return code
        if not self.prompt:
            raise RuntimeError("OTP secret not provided and prompting disabled")
        return await asyncio.to_thread(input, "Enter verification code: ")
