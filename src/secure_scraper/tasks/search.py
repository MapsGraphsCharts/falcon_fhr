"""Search workflow scaffolding."""
from __future__ import annotations

import logging
from typing import Iterable

from playwright.async_api import Page

from secure_scraper.selectors.search_page import SearchSelectors
from secure_scraper.utils.throttling import human_delay

logger = logging.getLogger(__name__)


class SearchTask:
    """Execute a search workflow and yield hits as JSON-ready dicts."""

    def __init__(self, query: str) -> None:
        self.query = query

    async def run(self, page: Page) -> Iterable[dict[str, object]]:
        logger.info("Executing search for '%s'", self.query)
        query_locator = page.locator(SearchSelectors.query_input)
        if not await query_locator.count():
            logger.warning("Search input not found at selector %s", SearchSelectors.query_input)
            return []
        await query_locator.first.fill(self.query)
        await human_delay(0.2, 0.6)
        submit_locator = page.locator(SearchSelectors.submit_button)
        if await submit_locator.count():
            await submit_locator.first.click()
        else:
            await query_locator.press("Enter")
        try:
            await page.wait_for_selector(SearchSelectors.results_container, timeout=15000)
        except Exception:
            logger.warning("Search results container not found at selector %s", SearchSelectors.results_container)
            return []

        results_locator = page.locator(SearchSelectors.result_items)
        count = await results_locator.count()
        logger.debug("Found %s search items", count)
        results = []
        for index in range(count):
            item = results_locator.nth(index)
            title = await item.locator(SearchSelectors.result_title).inner_text()
            link = await item.locator(SearchSelectors.result_link).get_attribute("href")
            results.append({"title": title.strip(), "href": link})
        return results
