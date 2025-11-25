"""Client for Amex Travel smartfill location lookups."""
from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from typing import Any, Dict, Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

SMARTFILL_URL = "https://amex-api.lxp.iseatz.com/hotel/smartfill"


class LocationClient(AbstractContextManager["LocationClient"]):
    """Thin wrapper around the location smartfill endpoint."""

    def __init__(
        self,
        *,
        base_url: str = SMARTFILL_URL,
        timeout: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        default_headers = {
            "lxp-app-id": "amex-us",
            "Accept": "application/json",
            "User-Agent": "secure-scraper/0.1.0",
        }
        if headers:
            default_headers.update(headers)
        self._client = httpx.Client(timeout=timeout, headers=default_headers)
        self._base_url = base_url

    def close(self) -> None:
        self._client.close()

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()

    def lookup(self, query: str, *, size: int | None = None) -> Dict[str, Any]:
        params = {"query": query}
        if size is not None:
            params["size"] = str(size)
        logger.debug("Smartfill lookup query='%s' size=%s", query, size)
        response = self._client.get(self._base_url, params=params)
        response.raise_for_status()
        return response.json()

    def lookup_best(self, query: str, *, size: int | None = None) -> Optional[Dict[str, Any]]:
        try:
            return self.lookup(query, size=size)
        except httpx.HTTPError:
            logger.exception("Smartfill lookup failed for query '%s'", query)
            return None

    @staticmethod
    def iter_candidates(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        for key in (
            "cities",
            "neighborhoods",
            "airports",
            "trainStations",
            "regions",
            "areas",
            "pointsOfInterest",
        ):
            entries = payload.get(key) or []
            for entry in entries:
                entry["_category"] = key
                yield entry
