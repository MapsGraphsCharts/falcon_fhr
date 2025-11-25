"""Replay Amex Travel hotel search using stored storage state."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

DEFAULT_STORAGE = Path("data/logs/network/storage_state_20251030-171859.json")
PROPERTIES_URL = "https://www.travel.americanexpress.com/en-us/book/api/lxp/hotel/properties"


async def fetch_properties(storage: Path, payload_path: Path | None, output: Path, headless: bool) -> None:
    body = {
        "pagination": {"page": 1, "pageSize": 50},
        "sortOptions": [{"direction": "DESC", "option": "RECOMMENDED"}],
        "checkIn": "2025-11-11",
        "checkOut": "2025-11-14",
        "location": "ZMETRO-EXPEDIA-179899",
        "locationType": "LOCATION_ID",
        "rooms": [{"adults": 2}],
    }
    if payload_path:
        body = json.loads(payload_path.read_text())

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(storage))
        request = context.request
        response = await request.post(PROPERTIES_URL, data=json.dumps(body), headers={"Content-Type": "application/json"})
        if not response.ok:
            raise RuntimeError(f"Request failed: {response.status} {await response.text()}")
        data = await response.json()
        output.write_text(json.dumps(data, indent=2))
        await context.close()
        await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay captured hotel search")
    parser.add_argument("--storage", type=Path, default=DEFAULT_STORAGE)
    parser.add_argument("--payload", type=Path)
    parser.add_argument("--output", type=Path, default=Path("data/downloads/hotel_results.json"))
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()
    asyncio.run(fetch_properties(args.storage, args.payload, args.output, not args.headed))


if __name__ == "__main__":
    main()
