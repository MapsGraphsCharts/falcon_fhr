"""Utilities to avoid robotic timing patterns."""
from __future__ import annotations

import asyncio
import random


async def human_delay(min_seconds: float = 0.2, max_seconds: float = 1.2) -> None:
    """Sleep for a random duration between ``min_seconds`` and ``max_seconds``."""
    if max_seconds < min_seconds:
        min_seconds, max_seconds = max_seconds, min_seconds
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))
