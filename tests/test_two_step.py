from __future__ import annotations

import pytest

from secure_scraper.auth.two_step import TwoStepVerifier


class _DummyLocator:
    def __init__(self, count: int = 0) -> None:
        self._count = count

    async def count(self) -> int:
        return self._count

    async def fill(self, value: str) -> None:  # pragma: no cover - tests stub
        self.value = value  # type: ignore[attr-defined]

    async def click(self) -> None:  # pragma: no cover - tests stub
        return None

    async def press(self, _key: str) -> None:  # pragma: no cover - tests stub
        return None


class _DummyPage:
    def __init__(self, texts: list[str]) -> None:
        self._texts = [text.lower() for text in texts]

    def get_by_text(self, text: str, exact: bool = False) -> _DummyLocator:
        text_lower = text.lower()
        matches = any(text_lower in candidate for candidate in self._texts)
        return _DummyLocator(1 if matches else 0)

    def get_by_role(self, *_args, **_kwargs) -> _DummyLocator:
        return _DummyLocator(0)

    def locator(self, _selector: str) -> _DummyLocator:
        return _DummyLocator(0)


class _DummySettings:
    fastmail_api_token = None
    fastmail_mailbox = None
    fastmail_sender_filter = None
    fastmail_subject_pattern = None
    fastmail_code_pattern = None
    fastmail_poll_interval_s = 1.0
    fastmail_timeout_s = 5.0
    fastmail_recent_window_s = 60.0
    fastmail_message_limit = 5
    mfa_secret = None


@pytest.mark.asyncio
async def test_account_lock_detection_stops_two_step(monkeypatch: pytest.MonkeyPatch) -> None:
    verifier = TwoStepVerifier(settings=_DummySettings())
    class _Resolver:
        @staticmethod
        async def obtain_code() -> str:
            return "123456"

    verifier.otp_resolver = _Resolver()

    page = _DummyPage(["Step something", "Your account is temporarily locked"])

    async def fake_challenge_present(_page):
        return True

    async def fake_select(_page):
        return None

    async def fake_wait_for_input(_page):
        return _DummyLocator(1)

    async def fake_find_submit(_page):
        return None

    async def fake_await_resolution(_page):
        return False

    monkeypatch.setattr(verifier, "_challenge_present", fake_challenge_present)
    monkeypatch.setattr(verifier, "_select_email_method", fake_select)
    monkeypatch.setattr(verifier, "_wait_for_input", fake_wait_for_input)
    monkeypatch.setattr(verifier, "_find_submit", fake_find_submit)
    monkeypatch.setattr(verifier, "_await_challenge_resolution", fake_await_resolution)

    async def fake_is_account_locked(_page):
        return True

    monkeypatch.setattr(verifier, "_is_account_locked", fake_is_account_locked)

    with pytest.raises(RuntimeError, match="account temporarily locked"):
        await verifier.maybe_solve(page)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_two_step_enforces_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    verifier = TwoStepVerifier(settings=_DummySettings())

    class _Resolver:
        calls = 0

        @staticmethod
        async def obtain_code() -> str:
            _Resolver.calls += 1
            return f"12345{_Resolver.calls}"

    verifier.otp_resolver = _Resolver()

    page = _DummyPage(
        ["Two-Step Verification", "Two-Step Verification still present"]  # marker text
    )

    challenge_responses = iter([True, True, True, False])
    async def fake_challenge_present(_page):
        return next(challenge_responses)

    async def fake_select(_page):
        return None

    async def fake_wait_for_input(_page):
        return _DummyLocator(1)

    async def fake_find_submit(_page):
        return None

    resolution_responses = iter([False, True])
    async def fake_await_resolution(_page):
        return next(resolution_responses)

    async def fake_is_account_locked(_page):
        return False

    sleep_calls: list[float] = []

    async def fake_sleep(duration: float):
        sleep_calls.append(duration)

    monotonic_values = iter([0.0, 5.0, 30.0])
    def fake_monotonic():
        return next(monotonic_values, 30.0)

    monkeypatch.setattr(verifier, "_challenge_present", fake_challenge_present)
    monkeypatch.setattr(verifier, "_select_email_method", fake_select)
    monkeypatch.setattr(verifier, "_wait_for_input", fake_wait_for_input)
    monkeypatch.setattr(verifier, "_find_submit", fake_find_submit)
    monkeypatch.setattr(verifier, "_await_challenge_resolution", fake_await_resolution)
    monkeypatch.setattr(verifier, "_is_account_locked", fake_is_account_locked)
    monkeypatch.setattr("secure_scraper.auth.two_step.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("secure_scraper.auth.two_step.time.monotonic", fake_monotonic)

    await verifier.maybe_solve(page)  # type: ignore[arg-type]

    assert sleep_calls, "Expected cooldown sleep between OTP submissions"
    assert pytest.approx(sleep_calls[0], rel=1e-2) == 20.0
