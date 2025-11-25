"""Login orchestration for Amex Travel."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

from playwright.async_api import BrowserContext, Page

from secure_scraper.auth.two_step import TwoStepVerifier
from secure_scraper.config.settings import Settings
from secure_scraper.selectors.login_page import LoginSelectors

logger = logging.getLogger(__name__)

_LEGACY_REQUIRED_COOKIES = {"amexsessioncookie", "aat"}
_NEXT_AUTH_COOKIE = "__Secure-next-auth.session-token"
_NEXT_AUTH_TTL_SECONDS = 6 * 3600
_LOGIN_TIMEOUT_MS = 60_000
_POST_LOGIN_TIMEOUT_S = 45
_SESSION_HANDSHAKE_TIMEOUT_S = 30
_STATE_FILTERED_COOKIES = {
    "ak_bmsc",
    "_abck",
    "bm_sv",
    "bm_sz",
    "bm_mi",
    "akaalb_www_prebookingcookie_v0",
    "akaalb_www_one_v8",
}


class LoginFlow:
    """Encapsulates login logic and storage state persistence."""

    def __init__(self, settings: Settings, verifier: Optional[TwoStepVerifier] = None) -> None:
        self.settings = settings
        self.verifier = verifier or TwoStepVerifier(settings)

    async def run(self, context: BrowserContext) -> Page:
        """Log into Amex Travel if necessary and return an authenticated page."""
        page = await context.new_page()

        storage_state_present = bool(self.settings.storage_state_path and self.settings.storage_state_path.exists())
        if storage_state_present:
            if await self._is_authenticated(context):
                logger.info("Existing authentication detected via cookies; attempting reuse")
            else:
                logger.info("Storage state detected but cookies incomplete; attempting reuse anyway")

            if await self._try_reuse_existing_session(page):
                return page

            logger.info("Stored session reuse failed; proceeding with fresh login flow")

        if not self.settings.username or not self.settings.password:
            raise RuntimeError("Username/password must be configured for login")

        await page.goto(self.settings.base_url, wait_until="domcontentloaded")
        if await self._is_authenticated(context):
            logger.info("Session validated after page load; reusing existing authentication")
            return page

        await self._navigate_to_login(page)
        await self._save_fingerprint_snapshot(page, label="prelogin")
        trace_label = time.strftime("%Y%m%d-%H%M%S")
        trace_path = Path("data/logs/login_debug") / f"login_trace_{trace_label}.zip"
        trace_started = await self._maybe_start_trace(context, trace_path)
        marker_monitoring = self.settings.login_monitor_markers
        cred_future = session_future = on_request = on_response = None
        if marker_monitoring:
            cred_future, session_future, on_request, on_response = self._install_session_watchers(context)
        try:
            await self._submit_credentials(page)
            await self.verifier.maybe_solve(page)
            cred_seen, session_seen = await self._ensure_book_session(
                page, context, cred_future, session_future
            )
        finally:
            self._remove_session_watchers(context, on_request, on_response)
            await self._maybe_stop_trace(context, trace_started, trace_path)

        handshake_ok = await self._await_authenticated(context, page)
        if not handshake_ok:
            await self._capture_debug_artifacts(page, context)
            raise RuntimeError(
                "Login handshake incomplete; captured debug artefacts for inspection"
            )
        if not (cred_seen and session_seen):
            logger.warning(
                "Login succeeded but expected auth network markers were not observed; continuing with cookie-based validation"
            )

        if handshake_ok:
            if not marker_monitoring or (cred_seen and session_seen):
                await self.save_storage_state(context)
            elif marker_monitoring:
                logger.warning(
                    "Skipping storage state persistence due to incomplete login handshake "
                    "(handshake_ok=%s, credentials_seen=%s, session_seen=%s)",
                    handshake_ok,
                    cred_seen,
                    session_seen,
                )
        else:
            logger.warning("Skipping storage state persistence because handshake failed")

        if not LoginSelectors.travel_url_pattern.search(page.url):
            await page.goto(self.settings.base_url, wait_until="domcontentloaded")
        return page

    async def save_storage_state(self, context: BrowserContext) -> Optional[str]:
        """Persist authentication cookies/tokens if configured."""
        if not self.settings.storage_state_path:
            return None
        path = self.settings.storage_state_path
        logger.info("Saving storage state to %s", path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("Failed to create storage state directory %s: %s", path.parent, exc)
        state = await context.storage_state()
        if isinstance(state, str):
            payload = json.loads(state)
        else:
            payload = state
        cookies = payload.get("cookies", [])
        filtered_cookies = [
            cookie
            for cookie in cookies
            if cookie.get("name", "").lower() not in _STATE_FILTERED_COOKIES
        ]
        if not filtered_cookies:
            logger.warning("Filtered storage state contained no cookies; skipping persistence")
            return None
        payload["cookies"] = filtered_cookies
        try:
            path.write_text(json.dumps(payload, indent=2))
        except Exception as exc:  # pragma: no cover - filesystem write
            logger.error("Failed to write storage state to %s: %s", path, exc)
            return None
        return str(path)

    async def _navigate_to_login(self, page: Page) -> None:
        logger.info("Navigating to login dialog")
        login_button = page.locator(LoginSelectors.login_button)
        if await login_button.count():
            try:
                handle = login_button.first
                await handle.scroll_into_view_if_needed()
                await handle.click(timeout=_LOGIN_TIMEOUT_MS)
                await page.wait_for_url(LoginSelectors.login_url_pattern, timeout=_LOGIN_TIMEOUT_MS)
                return
            except Exception as exc:
                logger.warning("Failed to activate login button (%s), falling back to direct URL", exc)
        # Direct navigation avoids header animation/visibility issues and lands on the same login SPA.
        await page.goto(LoginSelectors.login_entry_url, wait_until="domcontentloaded")

    async def _submit_credentials(self, page: Page) -> None:
        logger.info("Submitting credentials for user %s", self._masked_username())
        await page.wait_for_selector(LoginSelectors.username_input, timeout=_LOGIN_TIMEOUT_MS)
        await page.fill(LoginSelectors.username_input, self.settings.username or "")
        await page.fill(LoginSelectors.password_input, self.settings.password or "")
        await page.locator(LoginSelectors.submit_button).click()

    async def _await_authenticated(self, context: BrowserContext, page: Page) -> bool:
        logger.info("Waiting for authentication cookies to be issued")
        deadline = asyncio.get_running_loop().time() + _POST_LOGIN_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            if await self._is_authenticated(context):
                logger.info("Authentication cookies detected")
                return True
            if LoginSelectors.travel_url_pattern.search(page.url):
                logger.info("Login redirect completed to travel domain")
                return True
            await asyncio.sleep(2)
        logger.error("Timed out waiting for authenticated session")
        return False

    async def _capture_debug_artifacts(self, page: Page, context: BrowserContext) -> None:
        from pathlib import Path
        import time

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        debug_dir = Path("data/logs/login_debug")
        debug_dir.mkdir(parents=True, exist_ok=True)

        try:
            screenshot_path = debug_dir / f"login_timeout_{timestamp}.png"
            await page.screenshot(path=screenshot_path)
            logger.warning("Saved login timeout screenshot to %s", screenshot_path)
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("Failed to capture login screenshot: %s", exc)

        try:
            html_path = debug_dir / f"login_timeout_{timestamp}.html"
            html = await page.content()
            html_path.write_text(html)
            logger.warning("Saved login timeout DOM snapshot to %s", html_path)
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("Failed to write login HTML: %s", exc)

        try:
            cookies_path = debug_dir / f"cookies_{timestamp}.json"
            state = await context.storage_state()
            if isinstance(state, str):
                serialised = state
            else:
                import json

                serialised = json.dumps(state, indent=2)
            cookies_path.write_text(serialised)
            logger.warning("Saved login timeout storage state to %s", cookies_path)
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("Failed to capture storage state: %s", exc)

        await self._save_fingerprint_snapshot(page, label="timeout", timestamp=timestamp)

    async def _save_fingerprint_snapshot(
        self, page: Page, *, label: str, timestamp: str | None = None
    ) -> None:
        from pathlib import Path
        import json
        import time

        try:
            fingerprint = await self._collect_fingerprint(page)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("Skipping fingerprint capture for %s: %s", label, exc)
            return

        if not fingerprint:
            logger.debug("Fingerprint capture for %s returned empty payload", label)
            return

        ts = timestamp or time.strftime("%Y%m%d-%H%M%S")
        debug_dir = Path("data/logs/login_debug")
        debug_dir.mkdir(parents=True, exist_ok=True)
        path = debug_dir / f"fingerprint_{label}_{ts}.json"
        try:
            path.write_text(json.dumps(fingerprint, indent=2))
            logger.warning("Saved %s fingerprint snapshot to %s", label, path)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("Failed to write fingerprint snapshot %s: %s", path, exc)

    async def _collect_fingerprint(self, page: Page) -> dict:
        script = """
        (() => {
          const canvasFingerprint = () => {
            try {
              const canvas = document.createElement('canvas');
              const ctx = canvas.getContext('2d');
              if (!ctx) return null;
              ctx.textBaseline = 'top';
              ctx.font = "14px 'Arial'";
              ctx.fillStyle = '#f60';
              ctx.fillRect(0, 0, 100, 40);
              ctx.fillStyle = '#069';
              ctx.fillText('javascript-fp', 2, 2);
              ctx.fillStyle = '#f80';
              ctx.fillText('javascript-fp', 4, 15);
              return canvas.toDataURL();
            } catch (err) {
              return null;
            }
          };
          const webglInfo = () => {
            try {
              const canvas = document.createElement('canvas');
              const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
              if (!gl) return null;
              const dbg = gl.getExtension('WEBGL_debug_renderer_info');
              const vendor = dbg ? gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL) : gl.getParameter(gl.VENDOR);
              const renderer = dbg ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER);
              return { vendor, renderer };
            } catch (err) {
              return null;
            }
          };
          const timezone = (() => {
            try { return Intl.DateTimeFormat().resolvedOptions().timeZone; }
            catch { return null; }
          })();
          const plugins = (() => {
            try { return Array.from(navigator.plugins || []).map(p => ({ name: p.name, filename: p.filename, description: p.description })); }
            catch { return []; }
          })();
          const userAgentData = (() => {
            try {
              const data = navigator.userAgentData;
              if (!data) return null;
              const brands = Array.isArray(data.brands)
                ? data.brands.map(entry => ({ brand: entry.brand, version: entry.version }))
                : undefined;
              return {
                brands,
                mobile: data.mobile,
                platform: data.platform,
              };
            } catch (err) {
              return { error: String(err) };
            }
          })();
          return {
            userAgent: navigator.userAgent,
            platform: navigator.platform,
            languages: navigator.languages,
            language: navigator.language,
            hardwareConcurrency: navigator.hardwareConcurrency,
            maxTouchPoints: navigator.maxTouchPoints,
            deviceMemory: navigator.deviceMemory,
            vendor: navigator.vendor,
            productSub: navigator.productSub,
            webdriver: navigator.webdriver,
            screen: {
              width: screen.width,
              height: screen.height,
              availWidth: screen.availWidth,
              availHeight: screen.availHeight,
              colorDepth: screen.colorDepth,
              pixelDepth: screen.pixelDepth
            },
            window: {
              innerWidth: window.innerWidth,
              innerHeight: window.innerHeight,
              outerWidth: window.outerWidth,
              outerHeight: window.outerHeight,
              devicePixelRatio: window.devicePixelRatio
            },
            timezone,
            doNotTrack: navigator.doNotTrack,
            plugins,
            geolocationEnabled: 'geolocation' in navigator,
            cookieEnabled: navigator.cookieEnabled,
            localStorage: typeof localStorage !== 'undefined',
            sessionStorage: typeof sessionStorage !== 'undefined',
            canvasFingerprint: canvasFingerprint(),
            webglInfo: webglInfo(),
            userAgentData,
            timestamp: new Date().toISOString(),
          };
        })()
        """
        session = await page.context.new_cdp_session(page)
        try:
            result = await session.send(
                "Runtime.evaluate",
                {
                    "expression": script,
                    "returnByValue": True,
                    "awaitPromise": False,
                },
            )
            return result.get("result", {}).get("value", {})
        finally:
            try:
                await session.detach()
            except Exception:  # pragma: no cover - best effort cleanup
                pass

    async def _maybe_start_trace(self, context: BrowserContext, path: Path) -> bool:
        try:
            await context.tracing.start(screenshots=True, snapshots=True, sources=False)
            logger.info("Started login trace capture for %s", path.name)
            return True
        except Exception as exc:  # pragma: no cover - tracing is best-effort
            logger.debug("Skipping tracing start due to %s", exc)
            return False

    async def _maybe_stop_trace(
        self,
        context: BrowserContext,
        started: bool,
        path: Path,
    ) -> None:
        if not started:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # pragma: no cover - best effort directory creation
            logger.debug("Failed to prepare trace directory %s: %s", path.parent, exc)
        try:
            await context.tracing.stop(path=str(path))
            logger.warning("Saved login trace to %s", path)
        except Exception as exc:  # pragma: no cover - tracing is best-effort
            logger.debug("Failed to save login trace %s: %s", path, exc)

    def _install_session_watchers(self, context: BrowserContext):
        loop = asyncio.get_running_loop()
        cred_future: asyncio.Future[str] = loop.create_future()
        session_future: asyncio.Future[int] = loop.create_future()

        def on_request(request) -> None:
            if "auth/credentials-signin" in request.url and not cred_future.done():
                logger.info("Observed credentials-signin request: %s", request.url)
                cred_future.set_result(request.url)

        def on_response(response) -> None:
            if (
                "/book/api/auth/session" in response.url
                and response.status < 400
                and not session_future.done()
            ):
                logger.info("Observed auth session response with status %s", response.status)
                session_future.set_result(response.status)

        context.on("request", on_request)
        context.on("response", on_response)
        return cred_future, session_future, on_request, on_response

    def _remove_session_watchers(self, context: BrowserContext, on_request, on_response) -> None:
        if on_request:
            try:
                context.remove_listener("request", on_request)
            except Exception:
                pass
        if on_response:
            try:
                context.remove_listener("response", on_response)
            except Exception:
                pass

    async def _await_session_ready(
        self,
        cred_future: asyncio.Future[str] | None,
        session_future: asyncio.Future[int] | None,
    ) -> tuple[bool, bool]:
        cred_seen = False
        session_seen = False
        if cred_future:
            try:
                await asyncio.wait_for(cred_future, timeout=_SESSION_HANDSHAKE_TIMEOUT_S)
                cred_seen = True
            except asyncio.TimeoutError:
                logger.warning("Credentials-signin request not observed during login")
        if session_future:
            try:
                await asyncio.wait_for(session_future, timeout=_SESSION_HANDSHAKE_TIMEOUT_S)
                session_seen = True
            except asyncio.TimeoutError:
                logger.warning("Auth session response not observed during login")
        return cred_seen, session_seen

    async def _ensure_book_session(
        self,
        page: Page,
        context: BrowserContext,
        cred_future: asyncio.Future[str] | None,
        session_future: asyncio.Future[int] | None,
    ) -> tuple[bool, bool]:
        logger.info("Awaiting travel redirect to finalize session")
        try:
            await page.wait_for_url(
                LoginSelectors.travel_url_pattern,
                wait_until="domcontentloaded",
                timeout=_SESSION_HANDSHAKE_TIMEOUT_S * 1000,
            )
            try:
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                logger.debug("Travel page networkidle wait interrupted")
        except Exception:
            logger.warning("Travel redirect not observed on primary page; warming book session manually")
            await self._warm_book_session(context)
        if cred_future or session_future:
            return await self._await_session_ready(cred_future, session_future)
        return False, False

    async def _warm_book_session(self, context: BrowserContext) -> None:
        logger.info("Refreshing book site session in existing context")
        book_page = await context.new_page()
        try:
            await book_page.goto(LoginSelectors.book_root_url, wait_until="domcontentloaded")
            try:
                await book_page.wait_for_load_state("networkidle")
            except Exception:
                logger.debug("Book page networkidle wait interrupted")
        finally:
            await book_page.close()

    async def _is_authenticated(self, context: BrowserContext) -> bool:
        cookies = await context.cookies()
        names = {cookie.get("name") for cookie in cookies if cookie.get("name")}
        session_cookie = next((cookie for cookie in cookies if cookie.get("name") == _NEXT_AUTH_COOKIE), None)
        if session_cookie:
            expiry = session_cookie.get("expires")
            if expiry and expiry < (time.time() + _NEXT_AUTH_TTL_SECONDS):
                logger.info("NextAuth session cookie expires soon; forcing login refresh")
            else:
                logger.info("Detected NextAuth session cookie; reusing existing authentication")
                return True
        if _LEGACY_REQUIRED_COOKIES.issubset(names):
            logger.info("Detected legacy session cookies; reusing existing authentication")
            return True
        logger.info(
            "Authentication cookies missing; observed names: %s",
            ", ".join(sorted(names)) or "<none>",
        )
        return False

    async def _try_reuse_existing_session(self, page: Page) -> bool:
        try:
            await page.goto(LoginSelectors.book_root_url, wait_until="domcontentloaded")
        except Exception as exc:
            logger.debug("Failed to load travel site using stored session: %s", exc)
            return False

        current_url = page.url
        if LoginSelectors.is_login_redirect(current_url):
            logger.info("Stored session redirected to login (%s); fresh login required", current_url)
            return False

        logger.info("Session restored from storage state; landed on %s", current_url)
        try:
            await page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            logger.debug("Network idle wait interrupted during session reuse")
        return True

    def _masked_username(self) -> str:
        if not self.settings.username:
            return "<unset>"
        if len(self.settings.username) <= 3:
            return "***"
        return f"{self.settings.username[:3]}***"
