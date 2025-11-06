from __future__ import annotations

from secure_scraper.config.settings import Settings


def test_settings_builds_stealth_kwargs(tmp_path):
    settings = Settings(
        stealth_enabled=True,
        stealth_platform="Win32",
        download_dir=tmp_path / "downloads",
    )

    kwargs = settings.stealth_kwargs()
    assert kwargs["navigator_platform_override"] == "Win32"
    settings.ensure_directories()
    assert settings.download_dir.exists()
