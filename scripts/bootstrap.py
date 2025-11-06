"""Development bootstrap helpers.

- Installs Python dependencies (pip install . - optional).
- Ensures Playwright browsers and OS dependencies are available.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str]) -> None:
    """Run a command and stream output."""
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def install_playwright(with_deps: bool = False) -> None:
    cmd = [sys.executable, "-m", "playwright", "install"]
    if with_deps:
        cmd.append("--with-deps")
    run(cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap local development")
    parser.add_argument(
        "--with-deps",
        action="store_true",
        help="Install system dependencies (Linux CI) via playwright install --with-deps",
    )
    parser.add_argument(
        "--skip-deps",
        action="store_true",
        help="Skip pip install -e . if dependencies already installed",
    )
    args = parser.parse_args()

    if not args.skip_deps:
        run([sys.executable, "-m", "pip", "install", "-e", ".[dev]"])

    install_playwright(with_deps=args.with_deps)
    print("Bootstrap complete")


if __name__ == "__main__":
    main()
