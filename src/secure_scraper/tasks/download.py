"""Download workflow scaffolding."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

from secure_scraper.storage.json_writer import JsonStore

logger = logging.getLogger(__name__)


class DownloadTask:
    """Persist scraped data to disk."""

    def __init__(self, download_dir: Path) -> None:
        self._store = JsonStore(download_dir)

    async def run(
        self,
        items: Iterable[dict[str, object]],
        *,
        filename: str = "results.json",
        subdir: str | None = None,
    ) -> Path:
        data = list(items)
        logger.info("Writing %s items to %s%s", len(data), f"{subdir}/" if subdir else "", filename)
        return await self._store.write(data, filename=filename, subdir=subdir)
