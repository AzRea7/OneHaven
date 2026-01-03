# app/domain/parsing.py
from __future__ import annotations

from typing import Any


def to_int(x: Any) -> int | None:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def to_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def get_first(payload: dict[str, Any], *keys: str) -> Any:
    """Return first non-empty key from payload."""
    for k in keys:
        v = payload.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def get_nested(payload: dict[str, Any], path: str) -> Any:
    """Tiny dot-path getter: 'address.line1' or 'address.city'."""
    cur: Any = payload
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur
