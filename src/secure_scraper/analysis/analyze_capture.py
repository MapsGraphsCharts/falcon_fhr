"""Summaries for network capture artifacts.

Given the JSON emitted by ``scripts/capture_network.py`` this helper surfaces
must-keep cookies and interesting tokens returned by the American Express
endpoints. Use it from the CLI:

    PYTHONPATH=src python -m secure_scraper.analysis.analyze_capture \
        --capture data/logs/network/network_capture_20251030-164722.json \
        --storage data/logs/network/storage_state_20251030-164722.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

TARGET_KEYWORDS = {
    "aat",
    "guid",
    "token",
    "session",
    "jwt",
    "publicguid",
    "csr",
    "x-amex",
}

INTERESTING_ENDPOINT_SNIPPETS = [
    "ReadUserSession",
    "UpdateUserSession",
    "persona",
    "identity",
    "one-xp",
    "global.americanexpress.com",
    "tlsonline",
]


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def summarise_hosts(captures: List[Dict[str, Any]], *, top: int = 15) -> List[Tuple[str, int]]:
    counter: Counter[str] = Counter()
    for cap in captures:
        url = cap.get("url") or ""
        host = url.split("//", 1)[-1].split("/", 1)[0]
        counter[host] += 1
    return counter.most_common(top)


def group_cookies(cookies: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cookie in cookies:
        grouped[cookie["domain"]].append(cookie)
    return dict(grouped)


def extract_interesting_requests(captures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    interesting: List[Dict[str, Any]] = []
    for cap in captures:
        url = cap.get("url", "")
        if any(snippet in url for snippet in INTERESTING_ENDPOINT_SNIPPETS):
            interesting.append(cap)
    return interesting


def flatten_payload(payload: Any, prefix: str = "") -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            full_key = f"{prefix}.{key}" if prefix else key
            items.update(flatten_payload(value, full_key))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            full_key = f"{prefix}[{index}]" if prefix else f"[{index}]"
            items.update(flatten_payload(value, full_key))
    else:
        items[prefix or "value"] = payload
    return items


def extract_tokens(captures: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    results: List[Tuple[str, Dict[str, Any]]] = []
    for cap in captures:
        body = cap.get("body_preview")
        if not body:
            continue
        try:
            parsed = json.loads(body)
        except Exception:
            continue
        flat = flatten_payload(parsed)
        matching = {k: v for k, v in flat.items() if any(keyword in k.lower() for keyword in TARGET_KEYWORDS)}
        if matching:
            results.append((cap.get("url", ""), matching))
    return results


def write_summary(
    *,
    capture_path: Path,
    storage_path: Path | None,
    output: Path | None = None,
) -> None:
    capture = load_json(capture_path)
    captures: List[Dict[str, Any]] = capture.get("captures", [])

    lines: List[str] = []
    lines.append(f"Capture: {capture_path}")
    lines.append(f"Total responses: {len(captures)}")

    lines.append("\nTop hosts:")
    for host, count in summarise_hosts(captures):
        lines.append(f"  - {host}: {count}")

    interesting = extract_interesting_requests(captures)
    lines.append(f"\nInteresting endpoints matched: {len(interesting)}")
    for cap in interesting[:10]:
        lines.append(f"  - {cap.get('status')} {cap.get('url')}")

    tokens = extract_tokens(interesting)
    lines.append(f"\nToken-like payload entries: {len(tokens)}")
    for url, mapping in tokens[:10]:
        lines.append(f"  - {url}")
        for key, value in mapping.items():
            lines.append(f"      {key}: {value}")

    if storage_path:
        storage = load_json(storage_path)
        cookies = storage.get("cookies", [])
        grouped = group_cookies(cookies)
        lines.append(f"\nStored cookies: {len(cookies)} total across {len(grouped)} domains")
        for domain, items in grouped.items():
            lines.append(f"  - {domain}: {len(items)}")
            for cookie in items:
                expiry = cookie.get("expires")
                lines.append(
                    f"      {cookie['name']} | secure={cookie.get('secure')} | httpOnly={cookie.get('httpOnly')} | expires={expiry}"
                )

    summary = "\n".join(lines)
    if output:
        output.write_text(summary)
        print(f"Summary written to {output}")
    else:
        print(summary)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarise network capture artifacts")
    parser.add_argument("--capture", type=Path, required=True, help="Path to capture JSON")
    parser.add_argument("--storage", type=Path, help="Path to storage_state JSON")
    parser.add_argument("--output", type=Path, help="Optional path to write summary text")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    write_summary(capture_path=args.capture, storage_path=args.storage, output=args.output)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
