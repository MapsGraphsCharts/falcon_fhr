"""Local shim that routes `playwright` imports to Patchright.

Patchright exposes a Playwright-compatible API under the `patchright` package
name. Some of our dependencies (and parts of this project) still import from
`playwright.*`, so we register the Patchright modules under the expected names.
"""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Iterable

import patchright as _patchright


def _register_submodule(name: str) -> ModuleType:
    module = importlib.import_module(f"patchright.{name}")
    sys.modules[f"{__name__}.{name}"] = module
    return module


for submodule_name in ("__main__", "async_api", "sync_api", "_impl", "driver"):
    try:
        _register_submodule(submodule_name)
    except ModuleNotFoundError:
        continue


_public_attrs: Iterable[str] = (
    getattr(_patchright, "__all__", None)
    or (name for name in dir(_patchright) if not name.startswith("_"))
)

globals().update({name: getattr(_patchright, name) for name in _public_attrs})

_excluded = {"importlib", "sys", "ModuleType", "_patchright", "_public_attrs", "_register_submodule", "__builtins__"}
__all__ = sorted(
    name for name in globals()
    if not name.startswith("_") and name not in _excluded
)


def __getattr__(name: str):
    return getattr(_patchright, name)


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(dir(_patchright)))
