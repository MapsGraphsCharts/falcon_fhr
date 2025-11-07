from __future__ import annotations

import pytest

from secure_scraper.utils.fastmail import FastmailOtpFetcher
from secure_scraper.utils.otp import OtpResolver, generate_totp


def test_generate_totp_known_values():
    secret = "JBSWY3DPEHPK3PXP"
    assert generate_totp(secret, timestamp=0) == "282760"
    assert generate_totp(secret, timestamp=59) == "996554"


@pytest.mark.asyncio
async def test_obtain_code_uses_email_fetcher(monkeypatch):
    class StubFetcher:
        async def fetch_code(self) -> str:
            return "123456"

    resolver = OtpResolver(secret=None, email_fetcher=StubFetcher(), prompt=False)
    assert await resolver.obtain_code() == "123456"


@pytest.mark.asyncio
async def test_obtain_code_prompts_when_fetcher_fails(monkeypatch):
    class FailingFetcher:
        async def fetch_code(self) -> str:
            raise RuntimeError("boom")

    async def fake_to_thread(func, *args, **kwargs):
        return "789012"

    monkeypatch.setattr("secure_scraper.utils.otp.asyncio.to_thread", fake_to_thread)

    resolver = OtpResolver(secret=None, email_fetcher=FailingFetcher(), prompt=True)
    assert await resolver.obtain_code() == "789012"


@pytest.mark.asyncio
async def test_obtain_code_raises_when_fetcher_fails_without_prompt():
    class FailingFetcher:
        async def fetch_code(self) -> str:
            raise RuntimeError("boom")

    resolver = OtpResolver(secret=None, email_fetcher=FailingFetcher(), prompt=False)
    with pytest.raises(RuntimeError, match="Failed to obtain OTP via email fetcher"):
        await resolver.obtain_code()


def test_fastmail_fetcher_extracts_code_from_subject():
    fetcher = FastmailOtpFetcher(api_token="token")
    message = {"subject": "Your code is 654321", "bodyValues": {}}
    assert fetcher._extract_code(message) == "654321"


def test_fastmail_fetcher_extracts_code_from_text_body():
    fetcher = FastmailOtpFetcher(api_token="token")
    message = {
        "subject": "No code here",
        "textBody": [{"partId": "text"}],
        "bodyValues": {"text": {"value": "Enter the code 112233 to continue."}},
    }
    assert fetcher._extract_code(message) == "112233"


def test_fastmail_fetcher_extracts_code_from_html_body():
    fetcher = FastmailOtpFetcher(api_token="token")
    message = {
        "htmlBody": [{"partId": "html"}],
        "bodyValues": {"html": {"value": "<p>One-time code: <strong>778899</strong></p>"}},
    }
    assert fetcher._extract_code(message) == "778899"


def test_fastmail_fetcher_prefers_code_near_keywords():
    fetcher = FastmailOtpFetcher(api_token="token")
    noisy_html = (
        "<p>Need help? Reference number 333333 when you call support.</p>"
        "<p style=\"margin:0\">Your verification code is <strong>076695</strong>.</p>"
    )
    message = {
        "htmlBody": [{"partId": "html"}],
        "bodyValues": {"html": {"value": noisy_html}},
    }
    assert fetcher._extract_code(message) == "076695"
