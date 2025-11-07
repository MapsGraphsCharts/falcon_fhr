"""Fastmail JMAP helpers for retrieving OTP codes from email."""
from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

_JMAP_CORE = "urn:ietf:params:jmap:core"
_JMAP_MAIL = "urn:ietf:params:jmap:mail"

_CONTEXT_KEYWORDS: tuple[str, ...] = (
    "verification code",
    "one-time verification code",
    "one time verification code",
    "one-time passcode",
    "one time passcode",
    "one-time password",
    "one time password",
    "one-time code",
    "one time code",
    "otp",
    "passcode",
    "security code",
    "use this code",
    "enter the code",
    "your code",
)
_CONTEXT_WINDOW = 80


def _snippet(value: str, start: int, end: int, *, radius: int = 40) -> str:
    window_start = max(0, start - radius)
    window_end = min(len(value), end + radius)
    snippet = value[window_start:window_end]
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet.strip()


def _parse_received_at(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        logger.debug("Unable to parse Fastmail receivedAt value %s", value)
        return None


def _strip_html(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value)
    return html.unescape(cleaned)


class FastmailOtpFetcher:
    """Polls Fastmail via JMAP until an OTP code appears."""

    session_url = "https://api.fastmail.com/jmap/session"
    default_api_url = "https://api.fastmail.com/jmap/"

    def __init__(
        self,
        *,
        api_token: str,
        mailbox: Optional[str] = "inbox",
        sender: Optional[str] = None,
        subject_pattern: Optional[str] = None,
        code_pattern: str = r"\b(\d{6})\b",
        poll_interval: float = 5.0,
        timeout: float = 120.0,
        recent_window: float = 900.0,
        message_limit: int = 10,
        http_timeout: float = 30.0,
    ) -> None:
        if not api_token:
            raise ValueError("Fastmail API token must be provided")
        if message_limit <= 0:
            raise ValueError("message_limit must be positive")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        self.api_token = api_token
        self.mailbox = mailbox.strip() if mailbox else None
        self.sender_filter = sender
        self.sender_filter_lower = sender.lower() if sender else None
        self.subject_pattern = re.compile(subject_pattern, re.IGNORECASE) if subject_pattern else None
        self.code_pattern = re.compile(code_pattern)
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.recent_window = recent_window
        self.message_limit = message_limit
        self.http_timeout = http_timeout
        self.api_url = self.default_api_url

    async def fetch_code(self) -> str:
        start_time = datetime.now(timezone.utc)
        deadline = time.monotonic() + self.timeout
        headers = {"Authorization": f"Bearer {self.api_token}"}
        lookback_cutoff = start_time - timedelta(seconds=self.recent_window)

        async with httpx.AsyncClient(timeout=self.http_timeout) as client:
            account_id = await self._resolve_account_id(client, headers)
            mailbox_id = None
            if self.mailbox:
                mailbox_id = await self._resolve_mailbox_id(client, headers, account_id, self.mailbox)

            while time.monotonic() < deadline:
                try:
                    messages = await self._fetch_recent_messages(client, headers, account_id, mailbox_id)
                except httpx.HTTPError as exc:
                    logger.warning("Fastmail query failed: %s", exc)
                    await asyncio.sleep(self.poll_interval)
                    continue

                for message in messages:
                    received_at = _parse_received_at(message.get("receivedAt"))
                    if received_at and received_at < lookback_cutoff:
                        continue
                    if self.subject_pattern and not self.subject_pattern.search(message.get("subject") or ""):
                        continue
                    if self.sender_filter_lower and not self._sender_matches(message.get("from")):
                        continue
                    code = self._extract_code(message)
                    if code:
                        logger.info(
                            "Fetched OTP code from Fastmail message with subject '%s'",
                            message.get("subject"),
                        )
                        return code
                await asyncio.sleep(self.poll_interval)

        raise RuntimeError("Timed out waiting for OTP email via Fastmail")

    async def _resolve_account_id(
        self, client: httpx.AsyncClient, headers: dict[str, str]
    ) -> str:
        response = await client.get(self.session_url, headers=headers)
        response.raise_for_status()
        payload = response.json()
        self._update_api_url(payload)
        try:
            return payload["primaryAccounts"][_JMAP_MAIL]
        except KeyError as exc:  # pragma: no cover - defensive
            raise RuntimeError("Fastmail session response missing mail account information") from exc

    async def _resolve_mailbox_id(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        account_id: str,
        mailbox: str,
    ) -> Optional[str]:
        method_calls = [
            [
                "Mailbox/get",
                {"accountId": account_id, "properties": ["id", "name", "role"]},
                "m1",
            ]
        ]
        payload = await self._post(client, headers, method_calls)
        mailboxes = self._extract_method(payload, "Mailbox/get", call_id="m1").get("list", [])
        target = mailbox.lower()

        for entry in mailboxes:
            role = (entry.get("role") or "").lower()
            if role and role == target:
                return entry.get("id")

        for entry in mailboxes:
            name = (entry.get("name") or "").lower()
            if name == target:
                return entry.get("id")

        logger.warning("Fastmail mailbox '%s' not found; polling all mailboxes", mailbox)
        return None

    async def _fetch_recent_messages(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        account_id: str,
        mailbox_id: Optional[str],
    ) -> list[dict[str, Any]]:
        filter_args: dict[str, Any] = {}
        if mailbox_id:
            filter_args["inMailbox"] = mailbox_id
        if self.sender_filter:
            filter_args["from"] = self.sender_filter

        query_args: dict[str, Any] = {
            "accountId": account_id,
            "sort": [{"property": "receivedAt", "isAscending": False}],
            "limit": self.message_limit,
        }
        if filter_args:
            query_args["filter"] = filter_args

        method_calls = [
            [
                "Email/query",
                query_args,
                "q1",
            ],
            [
                "Email/get",
                {
                    "accountId": account_id,
                    "#ids": {"resultOf": "q1", "name": "Email/query", "path": "/ids"},
                    "properties": ["id", "subject", "receivedAt", "from", "textBody", "htmlBody", "bodyValues"],
                    "fetchTextBodyValues": True,
                    "fetchHTMLBodyValues": True,
                    "bodyProperties": ["partId", "type"],
                },
                "g1",
            ],
        ]

        payload = await self._post(client, headers, method_calls)
        email_get = self._extract_method(payload, "Email/get", call_id="g1")
        return email_get.get("list", [])

    async def _post(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        method_calls: list[list[Any]],
    ) -> dict[str, Any]:
        request_payload = {
            "using": [_JMAP_CORE, _JMAP_MAIL],
            "methodCalls": method_calls,
        }
        response = await client.post(self.api_url, headers=headers, json=request_payload)
        response.raise_for_status()
        return response.json()

    def _extract_method(
        self,
        payload: dict[str, Any],
        name: str,
        *,
        call_id: Optional[str] = None,
    ) -> dict[str, Any]:
        method_responses = payload.get("methodResponses", [])
        for method_name, arguments, response_call_id in method_responses:
            if method_name == name and (call_id is None or response_call_id == call_id):
                return arguments
        if call_id is not None:
            for method_name, arguments, response_call_id in method_responses:
                if response_call_id == call_id and method_name == "error":
                    error_type = arguments.get("type", "unknown")
                    description = arguments.get("description")
                    message = f"Fastmail JMAP call {name} ({call_id}) returned error {error_type}"
                    if description:
                        message = f"{message}: {description}"
                    raise RuntimeError(message)
        available = [method_name for method_name, _arguments, _call_id in method_responses]
        raise RuntimeError(
            f"Fastmail JMAP response missing {name}; methods present: {available}"
        )  # pragma: no cover - defensive
        raise RuntimeError(f"Fastmail JMAP response missing {name}")  # pragma: no cover - defensive

    def _update_api_url(self, payload: dict[str, Any]) -> None:
        api_url = (payload.get("apiUrl") or "").strip()
        if not api_url:
            return
        if not api_url.endswith("/"):
            api_url = f"{api_url}/"
        if api_url != self.api_url:
            logger.debug("Updating Fastmail JMAP endpoint to %s", api_url)
        self.api_url = api_url

    def _sender_matches(self, sender_entries: Optional[Iterable[dict[str, Any]]]) -> bool:
        if not sender_entries:
            return False
        for entry in sender_entries:
            email = (entry.get("email") or "").lower()
            if email == self.sender_filter_lower:
                return True
        return False

    def _log_candidate(
        self,
        *,
        source: str,
        text: str,
        span: tuple[int, int],
        keyword: Optional[str] = None,
    ) -> None:
        start, end = span
        try:
            snippet = _snippet(text, start, end)
        except Exception:  # pragma: no cover - defensive
            snippet = "<unable to render snippet>"
        detail = ""
        if keyword:
            detail = f" near keyword '{keyword}'"
        logger.debug("Fastmail OTP candidate from %s%s: %s", source, detail, snippet)

    def _match_is_valid(self, text: str, start: int, end: int) -> bool:
        if start > 0 and text[start - 1] == "#":
            return False
        return True

    def _find_contextual_code(self, text: str) -> Optional[str]:
        if not text:
            return None

        lowered = text.lower()
        for keyword in _CONTEXT_KEYWORDS:
            start = 0
            while True:
                idx = lowered.find(keyword, start)
                if idx == -1:
                    break
                after_start = idx + len(keyword)
                after_end = min(len(text), after_start + _CONTEXT_WINDOW)
                after_window = text[after_start:after_end]
                for match in self.code_pattern.finditer(after_window):
                    absolute_start = after_start + match.start(1)
                    absolute_end = after_start + match.end(1)
                    if not self._match_is_valid(text, absolute_start, absolute_end):
                        continue
                    self._log_candidate(
                        source="context-after",
                        text=text,
                        span=(absolute_start, absolute_end),
                        keyword=keyword,
                    )
                    return match.group(1)

                before_start = max(0, idx - _CONTEXT_WINDOW)
                before_end = idx
                before_window = text[before_start:before_end]
                before_candidates: list[tuple[re.Match[str], int, int]] = []
                for match in self.code_pattern.finditer(before_window):
                    absolute_start = before_start + match.start(1)
                    absolute_end = before_start + match.end(1)
                    if not self._match_is_valid(text, absolute_start, absolute_end):
                        continue
                    before_candidates.append((match, absolute_start, absolute_end))

                if before_candidates:
                    selected, abs_start, abs_end = before_candidates[-1]
                    self._log_candidate(
                        source="context-before",
                        text=text,
                        span=(abs_start, abs_end),
                        keyword=keyword,
                    )
                    return selected.group(1)
                start = idx + len(keyword)
        return None

    def _extract_code(self, message: dict[str, Any]) -> Optional[str]:
        subject = message.get("subject") or ""
        contextual = self._find_contextual_code(subject)
        if contextual:
            return contextual
        match = self.code_pattern.search(subject)
        if match:
            span = match.span(1)
            if self._match_is_valid(subject, *span):
                self._log_candidate(source="subject", text=subject, span=span)
                return match.group(1)

        body_values: dict[str, Any] = message.get("bodyValues") or {}

        for part in message.get("textBody") or []:
            part_id = part.get("partId")
            if not part_id or part_id not in body_values:
                continue
            value = body_values[part_id].get("value") or ""
            contextual = self._find_contextual_code(value)
            if contextual:
                return contextual
            match = self.code_pattern.search(value)
            if match:
                span = match.span(1)
                if not self._match_is_valid(value, *span):
                    continue
                self._log_candidate(source=f"textBody[{part_id}]", text=value, span=span)
                return match.group(1)

        for part in message.get("htmlBody") or []:
            part_id = part.get("partId")
            if not part_id or part_id not in body_values:
                continue
            value = body_values[part_id].get("value") or ""
            stripped = _strip_html(value)
            contextual = self._find_contextual_code(stripped)
            if contextual:
                return contextual
            match = self.code_pattern.search(stripped)
            if match:
                span = match.span(1)
                if not self._match_is_valid(stripped, *span):
                    continue
                self._log_candidate(source=f"htmlBody[{part_id}]", text=stripped, span=span)
                return match.group(1)
        return None
