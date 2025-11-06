"""JSON persistence helpers."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable


class JsonStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    async def write(
        self,
        data: Iterable[dict[str, object]],
        *,
        filename: str,
        subdir: str | None = None,
    ) -> Path:
        target_dir = self.root / subdir if subdir else self.root
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / filename
        serialisable = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "items": list(data),
        }
        path.write_text(json.dumps(serialisable, indent=2))
        return path
