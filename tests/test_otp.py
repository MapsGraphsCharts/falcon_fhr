from __future__ import annotations

from secure_scraper.utils.otp import generate_totp


def test_generate_totp_known_values():
    secret = "JBSWY3DPEHPK3PXP"
    assert generate_totp(secret, timestamp=0) == "282760"
    assert generate_totp(secret, timestamp=59) == "996554"
