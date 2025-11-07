"""Client for Amex Travel search APIs."""
from __future__ import annotations

import asyncio
from contextlib import suppress
import base64
import json
import logging
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode, quote_plus

from playwright.async_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

from secure_scraper.tasks.search_payloads import RoomRequest, SearchParams

logger = logging.getLogger(__name__)

PROPERTIES_URL = "https://www.travel.americanexpress.com/en-us/book/api/lxp/hotel/properties"
RESULTS_PAGE = (
    "https://www.travel.americanexpress.com/en-us/book/accommodations/search-results"
)
AUTH_SESSION_URL = "https://www.travel.americanexpress.com/en-us/book/api/auth/session"
SEARCH_REDIRECT_URL = (
    "https://consumer-travel.americanexpress.com/en-us/travel/search-redirect"
)
BOOK_ROOT_URL = "https://www.travel.americanexpress.com/en-us/book/"


class UnauthorizedSearchError(RuntimeError):
    """Raised when the properties API denies access due to auth/anti-bot checks."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Search request unauthorized ({status})")
        self.status = status
        self.body = body


class SessionRefreshError(RuntimeError):
    """Raised when a search cannot recover after session refresh attempts."""


class SearchClient:
    def __init__(self, context: BrowserContext) -> None:
        self.context = context
        self._account_token: Optional[str] = None

    async def fetch_properties(
        self,
        params: SearchParams,
        *,
        warmup_page: bool = True,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "Starting property fetch for %s (%s → %s)",
            params.location_id,
            params.check_in,
            params.check_out,
        )
        base_headers = {"Content-Type": "application/json"}
        if extra_headers:
            base_headers.update(extra_headers)

        account_token = await self._ensure_account_token()

        aggregated_response: Optional[Dict[str, Any]] = None
        page_number = params.page
        while True:
            page_params = replace(params, page=page_number)
            warmup_current = warmup_page and page_number == params.page
            page_response, account_token = await self._fetch_properties_page(
                page_params,
                account_token=account_token,
                headers=base_headers,
                warmup_page=warmup_current,
            )

            hotels = page_response.get("hotels", []) if page_response else []
            if aggregated_response is None:
                aggregated_response = page_response
            else:
                aggregated_response.setdefault("hotels", []).extend(hotels)
                if "context" in page_response:
                    aggregated_response.setdefault("context", {}).update(page_response["context"])

            pagination = (page_response or {}).get("context", {}).get("pagination", {}) or {}
            has_next = pagination.get("hasNext")
            if not has_next or not hotels:
                break
            page_number += 1

        if aggregated_response is None:
            aggregated_response = {
                "context": {
                    "pagination": {
                        "page": params.page,
                        "pageSize": params.page_size,
                        "hasNext": False,
                    }
                },
                "hotels": [],
            }

        return aggregated_response

    async def _ensure_account_token(self, *, force_refresh: bool = False) -> str:
        if self._account_token and not force_refresh:
            return self._account_token
        token = await self._fetch_account_token()
        self._account_token = token
        return token

    async def _fetch_properties_page(
        self,
        params: SearchParams,
        *,
        account_token: str,
        headers: Dict[str, str],
        warmup_page: bool,
    ) -> tuple[Dict[str, Any], str]:
        current_token = account_token
        payload = params.to_payload()
        use_warmup = warmup_page

        for refresh_attempt in range(3):
            page: Optional[Page] = None
            warmup_data: Optional[Dict[str, Any]] = None
            try:
                if use_warmup:
                    try:
                        page = await self._perform_search_redirect(params, current_token)
                        logger.info("Waiting for warm-up properties payload via page network response")
                        response = await page.wait_for_event(
                            "response",
                            lambda r: r.url.startswith(PROPERTIES_URL)
                            and r.request.method == "POST",
                            timeout=10_000,
                        )
                        warmup_data = await response.json()
                        logger.info("Captured properties payload via warm-up page")
                    except (asyncio.TimeoutError, PlaywrightTimeoutError, RuntimeError):
                        logger.warning("Warm-up capture failed; falling back to direct POST")
            finally:
                try:
                    if page:
                        await page.close()
                except Exception:
                    logger.debug("Failed to close search redirect page", exc_info=True)

            if warmup_data is not None:
                return warmup_data, current_token

            try:
                page_response = await self._post_properties(payload, headers)
                return page_response, current_token
            except UnauthorizedSearchError as exc:
                logger.warning(
                    "Hotel properties POST returned %s; refreshing session (attempt %s)",
                    exc.status,
                    refresh_attempt + 1,
                )
                try:
                    current_token = await self._ensure_account_token(force_refresh=True)
                except RuntimeError as token_error:
                    raise SessionRefreshError(str(token_error)) from token_error
                await self._refresh_travel_session()
                use_warmup = True

        raise SessionRefreshError("Search request failed after refreshing session")

    async def _post_properties(self, payload: dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        location = payload.get("location")
        page = payload.get("pagination", {}).get("page")
        if page:
            logger.info(
                "Fetching properties for %s via direct POST (page %s)",
                location,
                page,
            )
        else:
            logger.info("Fetching properties for %s via direct POST", location)
        response = await self.context.request.post(
            PROPERTIES_URL,
            data=json.dumps(payload),
            headers=dict(headers),
        )
        if response.ok:
            return await response.json()
        text = await response.text()
        if response.status in (401, 403):
            raise UnauthorizedSearchError(response.status, text)
        raise RuntimeError(f"Search request failed ({response.status}): {text[:512]}")

    async def _refresh_travel_session(self) -> None:
        logger.info("Refreshing travel session after failed properties request")
        page = await self.context.new_page()
        try:
            await page.goto(BOOK_ROOT_URL, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except PlaywrightTimeoutError:
                logger.debug("Travel session refresh networkidle wait timed out")
        finally:
            await page.close()

    def _build_results_url(params: SearchParams) -> str:
        if not params.rooms:
            raise ValueError("At least one room configuration is required")
        total_children = sum(len(room.children) for room in params.rooms)
        children_ages: list[int] = [
            age for room in params.rooms for age in room.children
        ]
        room = params.rooms[0]
        query_dict = {
            "adults": room.adults,
            "checkIn": params.check_in.isoformat(),
            "checkOut": params.check_out.isoformat(),
            "children": total_children,
            "childrenAges": ",".join(map(str, children_ages)) if children_ages else "",
            "locationType": "LOCATION_ID",
            "page": params.page,
            "pageSize": params.page_size,
            "placeName": params.location_label,
            "rooms": len(params.rooms),
            "sortingOption": "FEATURED",
            "placeId": params.location_id,
            "inav": "us-travel-hp-hotels-search",
        }
        return f"{RESULTS_PAGE}?{urlencode(query_dict)}"

    @staticmethod
    def from_capture(path: Path) -> SearchParams:
        body = json.loads(path.read_text())
        rooms = [
            RoomRequest(adults=item.get("adults", 1), children=item.get("children", []))
            for item in body.get("rooms", [])
        ]
        location = body.get("locationDetails") or {}
        geo = location.get("geoLocation", {})
        return SearchParams(
            location_id=body["location"],
            location_label=body.get("locationLabel", body.get("locationLabelName", body["location"])),
            latitude=float(geo.get("latitude", 0.0)),
            longitude=float(geo.get("longitude", 0.0)),
            check_in=date.fromisoformat(body["checkIn"]),
            check_out=date.fromisoformat(body["checkOut"]),
            rooms=rooms,
            page=body.get("pagination", {}).get("page", 1),
            page_size=body.get("pagination", {}).get("pageSize", 50),
            sort_option=(body.get("sortOptions", [{}])[0].get("option") or "RECOMMENDED"),
            sort_direction=(body.get("sortOptions", [{}])[0].get("direction") or "DESC"),
        )

    async def _fetch_account_token(self) -> str:
        for attempt in range(5):
            logger.info("Requesting account token (attempt %s)", attempt + 1)
            page = await self.context.new_page()
            response_task = asyncio.create_task(
                page.wait_for_event(
                    "response",
                    lambda r: r.url.startswith(AUTH_SESSION_URL),
                    timeout=10_000,
                )
            )
            try:
                await page.goto(BOOK_ROOT_URL, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=10_000)
                except PlaywrightTimeoutError:
                    logger.debug("Network idle wait timed out while preparing auth session fetch")
                token, status, preview = await self._fetch_account_token_via_request()
                if not token:
                    try:
                        awaited_response = await response_task
                    except PlaywrightTimeoutError:
                        logger.warning("auth/session not observed via page traffic on attempt %s", attempt + 1)
                    except Exception as exc:
                        logger.warning(
                            "auth/session response wait failed on attempt %s: %s",
                            attempt + 1,
                            exc,
                        )
                    else:
                        try:
                            data = await awaited_response.json()
                        except Exception:
                            text_preview = (await awaited_response.text())[:128]
                            logger.warning(
                                "auth/session page response not JSON on attempt %s: %s",
                                attempt + 1,
                                text_preview,
                            )
                        else:
                            token = data.get("clientCustomerId")
                if token:
                    logger.info("Obtained account token on attempt %s", attempt + 1)
                    return token
                if status:
                    logger.warning(
                        "auth/session HTTP %s on attempt %s (preview: %s)",
                        status,
                        attempt + 1,
                        preview,
                    )
                else:
                    logger.warning(
                        "auth/session fetch failed on attempt %s: %s",
                        attempt + 1,
                        preview,
                    )
            finally:
                if not response_task.done():
                    response_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await response_task
                try:
                    await page.close()
                except Exception as exc:
                    logger.debug("Failed to close token fetch page: %s", exc)
            await asyncio.sleep(1)
        raise RuntimeError("Unable to retrieve account token from auth session endpoint")

    async def _fetch_account_token_via_request(self) -> tuple[Optional[str], Optional[int], str]:
        cookies = await self.context.cookies([BOOK_ROOT_URL, AUTH_SESSION_URL])
        cookie_pairs = [
            f"{cookie.get('name')}={cookie.get('value')}"
            for cookie in cookies
            if cookie.get("name") and cookie.get("value")
        ]
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": BOOK_ROOT_URL,
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }
        if cookie_pairs:
            headers["Cookie"] = "; ".join(cookie_pairs)
        response = await self.context.request.get(AUTH_SESSION_URL, headers=headers)
        text = await response.text()
        token = None
        if response.ok:
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                logger.warning("auth/session response not JSON despite HTTP 200: %s", text[:128])
            else:
                token = data.get("clientCustomerId")
        return token, response.status, text[:128]

    async def _perform_search_redirect(self, params: SearchParams, account_token: str) -> Page:
        enable_program_filter = bool(params.program_filter)
        payload = {
            "request": {
                "rooms": [
                    {
                        "adults": room.adults,
                        "children": room.children,
                    }
                    for room in params.rooms
                ],
                "location": {
                    "geoLocation": {"latitude": params.latitude, "longitude": params.longitude},
                    "query": params.location_label,
                    "name": params.location_label,
                    "label": params.location_label,
                    "airportCode": "",
                    "type": "CITY",
                    "id": params.location_id,
                    "searchIdType": "LOCATION_ID",
                },
                "startDate": params.check_in.strftime("%m/%d/%Y"),
                "endDate": params.check_out.strftime("%m/%d/%Y"),
                "inavLocation": "hp-hotels",
                "horizonsConfig": {
                    "includeCenturion": True,
                    "isForcedLoginFeatureFlagEnabled": True,
                    "isCardModalEnabled": False,
                    "isFhrThcHorizonsEnabled": enable_program_filter,
                },
                "inav": "us-travel-hp-hotels-search",
                "accountToken": account_token,
            },
            "searchType": "hotels",
        }
        if enable_program_filter:
            payload["request"]["filters"] = {
                "clientProgramFilter": list(params.program_filter),
            }
        encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        url = f"{SEARCH_REDIRECT_URL}?requestBody={quote_plus(encoded)}"
        page = await self.context.new_page()
        logger.debug("Navigating to search redirect %s", url)
        await page.route("**/*", self._inject_fetch_overrides)
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            logger.debug("Networkidle not reached after search redirect; continuing")
        logger.info("Search redirect landed at %s", page.url)
        return page

    async def _inject_fetch_overrides(self, route, request) -> None:
        await route.continue_()
