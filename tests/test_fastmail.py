from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import httpx

from secure_scraper.utils.fastmail import FastmailOtpFetcher


class _DummyResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            request = httpx.Request("GET", "https://example.test")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("error", request=request, response=response)

    def json(self) -> dict[str, Any]:
        return self._payload


class _DummyAsyncClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._post_responses = []
        self.post_urls: list[str] = []
        self._session_payload: dict[str, Any] = {}
        self.request_bodies: list[dict[str, Any]] = []

    def configure(
        self,
        *,
        session_payload: dict[str, Any],
        post_payloads: list[dict[str, Any]],
    ) -> None:
        self._session_payload = session_payload
        self._post_responses = list(post_payloads)

    async def __aenter__(self) -> "_DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, headers: dict[str, str] | None = None) -> _DummyResponse:
        return _DummyResponse(self._session_payload)

    async def post(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
    ) -> _DummyResponse:
        self.post_urls.append(url)
        if json is not None:
            self.request_bodies.append(json)
        payload = self._post_responses.pop(0)
        return _DummyResponse(payload)


@pytest.mark.asyncio
async def test_fastmail_fetcher_uses_session_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = FastmailOtpFetcher(api_token="token")

    session_payload = {
        "primaryAccounts": {"urn:ietf:params:jmap:mail": "account-1"},
        "apiUrl": "https://alt.fastmail.test/jmap/api",
    }
    mailbox_payload = {
        "methodResponses": [
            ["Mailbox/get", {"list": [{"id": "mailbox-1", "role": "inbox", "name": "Inbox"}]}, "m1"]
        ]
    }
    message_payload = {
        "methodResponses": [
            ["Email/query", {"ids": ["message-1"]}, "q1"],
            [
                "Email/get",
                {
                    "list": [
                        {
                            "id": "message-1",
                            "subject": "Your code is 246810",
                            "receivedAt": datetime.now(timezone.utc).isoformat(),
                            "bodyValues": {},
                        }
                    ]
                },
                "g1",
            ],
        ]
    }

    dummy_instances: list[_DummyAsyncClient] = []

    def _make_dummy_async_client(*args: Any, **kwargs: Any) -> _DummyAsyncClient:
        client = _DummyAsyncClient()
        client.configure(session_payload=session_payload, post_payloads=[mailbox_payload, message_payload])
        dummy_instances.append(client)
        return client

    monkeypatch.setattr("secure_scraper.utils.fastmail.httpx.AsyncClient", _make_dummy_async_client)

    code = await fetcher.fetch_code()

    assert code == "246810"
    assert fetcher.api_url == "https://alt.fastmail.test/jmap/api/"
    assert dummy_instances, "expected FastmailOtpFetcher to create an AsyncClient"
    assert dummy_instances[0].post_urls == [
        "https://alt.fastmail.test/jmap/api/",
        "https://alt.fastmail.test/jmap/api/",
    ]
    assert dummy_instances[0].request_bodies[1]["methodCalls"][1][1]["#ids"]["path"] == "/ids"


@pytest.mark.asyncio
async def test_fastmail_fetcher_surfaces_jmap_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = FastmailOtpFetcher(api_token="token")

    session_payload = {
        "primaryAccounts": {"urn:ietf:params:jmap:mail": "account-1"},
        "apiUrl": "https://alt.fastmail.test/jmap/api",
    }
    mailbox_payload = {
        "methodResponses": [
            ["Mailbox/get", {"list": [{"id": "mailbox-1", "role": "inbox"}]}, "m1"]
        ]
    }
    error_payload = {
        "methodResponses": [
            ["Email/query", {"ids": []}, "q1"],
            ["error", {"type": "accountNotFound", "description": "unknown account"}, "g1"],
        ]
    }

    def _make_dummy_async_client(*args: Any, **kwargs: Any) -> _DummyAsyncClient:
        client = _DummyAsyncClient()
        client.configure(session_payload=session_payload, post_payloads=[mailbox_payload, error_payload])
        return client

    monkeypatch.setattr("secure_scraper.utils.fastmail.httpx.AsyncClient", _make_dummy_async_client)

    with pytest.raises(RuntimeError, match="returned error accountNotFound"):
        await fetcher.fetch_code()
