"""Selectors for American Express login flows."""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class LoginSelectors:
    login_button = "a[class*='loginButton']"
    login_entry_url = "https://www.americanexpress.com/en-us/account/login?linknav=us-travel-subnav-login&DestPage=https://www.americanexpress.com/en-us/travel/"
    username_input = "#eliloUserID"
    password_input = "#eliloPassword"
    submit_button = "#loginSubmit"
    otp_inputs = (
        "input[autocomplete='one-time-code']",
        "input[name*='verification']",
        "input[name*='oneTime']",
        "input[name*='one-time']",
        "input[name*='otp']",
        "input[name*='code']",
    )
    otp_submit_buttons = (
        "button[type='submit']",
        "button[class*='Submit']",
    )
    login_url_pattern = re.compile(r"americanexpress\.com/.*/login", re.IGNORECASE)
    oauth_connect_pattern = re.compile(r"americanexpress\.com/.*/oauth/connect", re.IGNORECASE)
    credentials_signin_pattern = re.compile(r"americanexpress\.com/.*/auth/credentials-signin", re.IGNORECASE)
    travel_credentials_signin_pattern = re.compile(
        r"travel\.americanexpress\.com/.*/auth/credentials-signin", re.IGNORECASE
    )
    travel_url_pattern = re.compile(r"americanexpress\.com/.*/travel", re.IGNORECASE)
    book_root_url = "https://www.travel.americanexpress.com/en-us/book/"
    _redirect_patterns: tuple[re.Pattern[str], ...] = (
        login_url_pattern,
        oauth_connect_pattern,
        credentials_signin_pattern,
        travel_credentials_signin_pattern,
    )

    @staticmethod
    def is_login_redirect(url: str) -> bool:
        return any(pattern.search(url) for pattern in LoginSelectors._redirect_patterns)
