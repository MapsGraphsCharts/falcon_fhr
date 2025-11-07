"""Two-step verification helpers for Amex flows."""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Iterable, Optional

from playwright.async_api import Locator, Page

from secure_scraper.config.settings import Settings
from secure_scraper.selectors.login_page import LoginSelectors
from secure_scraper.utils.fastmail import FastmailOtpFetcher
from secure_scraper.utils.otp import OtpResolver

logger = logging.getLogger(__name__)


_VERIFICATION_TEXT_SNIPPETS = (
    "Verification Code",
    "Two-Step Verification",
    "Security Verification",
    "Check your phone",
    "Verify your identity",
)
_CHALLENGE_POLL_INTERVAL_S = 0.5
_CHALLENGE_DETECTION_TIMEOUT_S = 12.0
_OTP_INPUT_TIMEOUT_S = 30.0
_CHALLENGE_RESOLVE_TIMEOUT_S = 40.0
_OTP_REQUEST_COOLDOWN_S = 25.0
_MAX_ATTEMPTS = 3
_ADD_DEVICE_HEADING = re.compile(r"Add This Device", re.IGNORECASE)
_ACCOUNT_LOCK_TEXTS = (
    "account is temporarily locked",
    "we could not complete your request",
    "contact us for further assistance",
)


class TwoStepVerifier:
    """Handles OTP prompts during login."""

    def __init__(self, settings: Settings, otp_resolver: Optional[OtpResolver] = None) -> None:
        self.settings = settings
        if otp_resolver:
            self.otp_resolver = otp_resolver
        else:
            email_fetcher = None
            if settings.fastmail_api_token:
                email_fetcher = FastmailOtpFetcher(
                    api_token=settings.fastmail_api_token,
                    mailbox=settings.fastmail_mailbox,
                    sender=settings.fastmail_sender_filter,
                    subject_pattern=settings.fastmail_subject_pattern,
                    code_pattern=settings.fastmail_code_pattern,
                    poll_interval=settings.fastmail_poll_interval_s,
                    timeout=settings.fastmail_timeout_s,
                    recent_window=settings.fastmail_recent_window_s,
                    message_limit=settings.fastmail_message_limit,
                )
            self.otp_resolver = OtpResolver(secret=settings.mfa_secret, email_fetcher=email_fetcher)

    async def maybe_solve(self, page: Page) -> bool:
        if not await self._challenge_present(page):
            return False

        logger.info("Two-step verification challenge detected")
        handled = False
        attempt = 0
        last_code_at: float | None = None

        while await self._challenge_present(page):
            attempt += 1
            if attempt > _MAX_ATTEMPTS:
                raise RuntimeError("Exceeded maximum attempts solving two-step verification")

            if attempt > 1:
                logger.info("Retrying two-step verification (attempt %s)", attempt)
                if last_code_at is not None:
                    elapsed = time.monotonic() - last_code_at
                    if elapsed < _OTP_REQUEST_COOLDOWN_S:
                        wait_for = _OTP_REQUEST_COOLDOWN_S - elapsed
                        logger.info(
                            "Waiting %.1fs before requesting another Amex OTP email",
                            wait_for,
                        )
                        await asyncio.sleep(wait_for)

            await self._select_email_method(page)
            input_locator = await self._wait_for_input(page)
            if not input_locator:
                raise RuntimeError("OTP input not found during challenge handling")

            code = (await self.otp_resolver.obtain_code()).strip()
            last_code_at = time.monotonic()
            await input_locator.fill(code)

            submit_locator = await self._find_submit(page)
            if submit_locator:
                await submit_locator.click()
            else:
                await input_locator.press("Enter")

            logger.info("Submitted verification code")
            handled = True

            if await self._await_challenge_resolution(page):
                logger.info("Two-step verification cleared")
                await self._dismiss_add_device_prompt(page)
                break
            if await self._is_account_locked(page):
                raise RuntimeError("Two-step verification blocked: account temporarily locked by Amex")
            logger.warning("Two-step verification still active after attempt %s", attempt)

        return handled

    async def _challenge_present(self, page: Page) -> bool:
        deadline = asyncio.get_running_loop().time() + _CHALLENGE_DETECTION_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            if await self._challenge_markers_present(page):
                return True
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
        return False

    async def _challenge_markers_present(self, page: Page) -> bool:
        if await page.get_by_role("heading", name=_ADD_DEVICE_HEADING).count():
            # Post-verification device enrollment screen; treat as resolved.
            return False
        if await page.locator("fieldset[data-testid='challenge-options-list']").count():
            return True
        input_locator = await self._find_input(page, strict=False)
        if input_locator:
            return True
        for snippet in _VERIFICATION_TEXT_SNIPPETS:
            if await page.get_by_text(snippet, exact=False).count():
                return True
        return False

    async def _select_email_method(self, page: Page) -> None:
        if await self._find_input(page, strict=False):
            logger.debug("OTP input already visible; skipping method selection")
            return

        buttons = page.locator("fieldset[data-testid='challenge-options-list'] button[data-testid='option-button']")
        count = await buttons.count()
        logger.debug("Found %s OTP option buttons", count)
        if count == 0:
            logger.warning("No OTP option buttons detected; continuing without selection")
            return

        email_button = page.get_by_role("button", name=re.compile("one-time password.*email", re.IGNORECASE))
        if await email_button.count():
            logger.info("Selecting OTP email button via accessible name")
            handle = email_button.first
            await handle.scroll_into_view_if_needed()
            await handle.click(force=True)
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
            return

        logger.info("Email button accessible name not found; defaulting to second option when available")
        if count >= 2:
            fallback = buttons.nth(1)
            await fallback.scroll_into_view_if_needed()
            await fallback.click(force=True)
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
        else:
            logger.warning("Unable to identify email OTP button; continuing without selection")

    async def _wait_for_input(self, page: Page) -> Optional[Locator]:
        deadline = asyncio.get_running_loop().time() + _OTP_INPUT_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            locator = await self._find_input(page, strict=False)
            if locator:
                return locator
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
        return None

    async def _find_input(self, page: Page, strict: bool = True) -> Optional[Locator]:
        for selector in LoginSelectors.otp_inputs:
            locator = page.locator(selector)
            if await locator.count():
                return locator.first
        if strict:
            raise RuntimeError("Could not locate OTP input field")
        return None

    async def _find_submit(self, page: Page) -> Optional[Locator]:
        buttons: Iterable[str] = LoginSelectors.otp_submit_buttons
        for selector in buttons:
            locator = page.locator(selector)
            if await locator.count():
                return locator.first
        by_role = page.get_by_role(
            "button",
            name=re.compile("(Submit|Continue|Verify|Next)", re.IGNORECASE),
        )
        if await by_role.count():
            return by_role.first
        return None

    async def _await_challenge_resolution(self, page: Page) -> bool:
        deadline = asyncio.get_running_loop().time() + _CHALLENGE_RESOLVE_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            if await page.get_by_role("heading", name=_ADD_DEVICE_HEADING).count():
                return True
            if not await self._challenge_markers_present(page):
                return True
            if await self._is_account_locked(page):
                return False
            if await self._has_error_message(page):
                return False
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
        if await self._is_account_locked(page):
            return False
        return not await self._challenge_markers_present(page)

    async def _has_error_message(self, page: Page) -> bool:
        error_texts = (
            "Try again",
            "Incorrect",
            "didn't work",
            "issue verifying",
        )
        error_locators = [
            "data-testid=challenge-error",
            "data-testid=mfa-error",
            "[role='alert']",
        ]
        for selector in error_locators:
            locator = page.locator(selector)
            if await locator.count():
                return True
        for text in error_texts:
            if await page.get_by_text(text, exact=False).count():
                return True
        return False

    async def _is_account_locked(self, page: Page) -> bool:
        for text in _ACCOUNT_LOCK_TEXTS:
            if await page.get_by_text(text, exact=False).count():
                logger.error("Detected account lock message on two-step verification page (%s)", text)
                return True
        return False

    async def _dismiss_add_device_prompt(self, page: Page) -> None:
        heading = page.get_by_role("heading", name=_ADD_DEVICE_HEADING)
        if not await heading.count():
            return

        logger.info("Add-device enrollment prompt detected; selecting 'Not Now'")
        button = page.get_by_role("button", name=re.compile(r"Not Now", re.IGNORECASE))
        if await button.count():
            await button.first.click()
            await asyncio.sleep(_CHALLENGE_POLL_INTERVAL_S)
        else:
            logger.warning("Unable to locate 'Not Now' button on add-device prompt")
