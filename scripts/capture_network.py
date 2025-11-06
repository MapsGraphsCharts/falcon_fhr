"""Capture amextravel.com network activity and cookies.

This script launches Playwright (with stealth if enabled) and records high-value
network traffic while you reproduce the login/search sequence manually. It saves
network metadata, cookies, and the storage state for later replay.
"""
from __future__ import annotations

import asyncio
import json
import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import Dict

from playwright.async_api import Request, Response, TimeoutError as PlaywrightTimeoutError

from secure_scraper.config.settings import Settings
from secure_scraper.core.browser import BrowserSession, ensure_close_context
from secure_scraper.core.logging import configure_logging

CAPTURE_DIR = Path("data/logs/network")
ALLOWED_RESOURCE_TYPES = {"document", "xhr", "fetch", "script"}

logger = logging.getLogger(__name__)


async def wait_for_user(prompt: str) -> None:
    await asyncio.to_thread(input, prompt)


async def main(args: Namespace) -> None:
    settings = Settings(headless=args.headless)
    configure_logging(settings.log_level, Path("data/logs"))
    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)

    async with BrowserSession(settings) as session:
        context = await session.new_context()
        try:
            network_records: Dict[int, dict[str, object]] = {}
            responses: list[dict[str, object]] = []

            def safe_post_data(request: Request) -> str | None:
                try:
                    return request.post_data
                except UnicodeDecodeError:
                    return "<binary>"
                except Exception:
                    return None

            def on_request(request: Request) -> None:
                if request.resource_type not in ALLOWED_RESOURCE_TYPES:
                    return
                if "auth/credentials-signin" in request.url:
                    logger.warning("Captured credentials-signin request: %s", request.url)
                if "/book/api/auth/session" in request.url:
                    logger.warning("Auth session request observed")
                payload = {
                    "url": request.url,
                    "method": request.method,
                    "resource_type": request.resource_type,
                    "headers": request.headers,
                    "post_data": safe_post_data(request),
                }
                network_records[id(request)] = payload
                logger.debug("Captured request %s", request.url)

            async def on_response(response: Response) -> None:
                req = response.request
                if req.resource_type not in ALLOWED_RESOURCE_TYPES:
                    return
                if "auth/credentials-signin" in response.url:
                    logger.warning(
                        "Credentials-signin response status %s for %s",
                        response.status,
                        response.url,
                    )
                if "/book/api/auth/session" in response.url and response.status < 400:
                    logger.warning("Auth session response status %s", response.status)
                record = network_records.get(id(req))
                if not record:
                    record = {
                        "url": req.url,
                        "method": req.method,
                        "resource_type": req.resource_type,
                        "headers": req.headers,
                        "post_data": safe_post_data(req),
                    }
                body_preview = None
                try:
                    body_preview = await response.text()
                except Exception:  # pragma: no cover - binary or large payloads
                    body_preview = None
                responses.append(
                    {
                        **record,
                        "status": response.status,
                        "response_headers": response.headers,
                        "body_preview": body_preview[:2048] if body_preview else None,
                    }
                )
                logger.debug("Captured response %s", req.url)

            def attach_listeners(page) -> None:
                page.on("request", on_request)
                page.on("response", lambda resp: asyncio.create_task(on_response(resp)))
                logger.debug("Attached listeners to page %s", page)

            context.on("page", lambda page: attach_listeners(page))

            page = await context.new_page()
            attach_listeners(page)

            target_url = args.url
            logger.info("Navigating to %s", target_url)
            await page.goto(target_url, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=args.load_timeout)
            except PlaywrightTimeoutError:
                logger.warning("Network idle wait timed out after %sms; continuing.", args.load_timeout)
            logger.info("Initial load complete.")

            if args.interactive:
                await wait_for_user("Press Enter once you've finished the workflow...\n")
            else:
                logger.info("Passive capture for %.1f seconds", args.duration)
                await asyncio.sleep(args.duration)

            cookies = await context.cookies()
            timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            storage_state_path = CAPTURE_DIR / f"storage_state_{timestamp}.json"
            await context.storage_state(path=str(storage_state_path))

            capture_path = CAPTURE_DIR / f"network_capture_{timestamp}.json"
            payload = {
                "captures": responses,
                "cookies": cookies,
            }
            capture_path.write_text(json.dumps(payload, indent=2))
            logger.info("Captured %s responses", len(responses))
            logger.info("Network data saved to %s", capture_path)
            logger.info("Storage state saved to %s", storage_state_path)
        finally:
            await ensure_close_context(context)


if __name__ == "__main__":
    parser = ArgumentParser(description="Capture network traffic for amextravel.com")
    parser.add_argument("--url", default="https://www.amextravel.com", help="Target URL to capture")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (default false for interactive debugging)",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Wait for user input before finishing capture",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=20.0,
        help="Passive capture duration in seconds when not interactive",
    )
    parser.add_argument(
        "--load-timeout",
        type=int,
        default=60000,
        help="Timeout in ms for initial networkidle wait",
    )
    cli_args = parser.parse_args()
    asyncio.run(main(cli_args))
