# onehaven/backend/app/services/normalize.py
from __future__ import annotations

import re
from typing import Final

# If you later decide to allow townhouse/condo, change this set and you're done.
DISALLOWED_TYPES: Final[set[str]] = {"condo", "townhouse", "manufactured", "land"}

def normalize_property_type(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip().lower()
    s = s.replace("/", " ")
    s = re.sub(r"\s+", " ", s)

    # common buckets
    if "manufact" in s or "mobile" in s:
        return "manufactured"
    if "land" in s or "lot" in s:
        return "land"
    if "condo" in s:
        return "condo"
    if "town" in s or "townhouse" in s or "row" in s:
        return "townhouse"
    if "multi" in s or "duplex" in s or "triplex" in s or "fourplex" in s or "2-4" in s:
        return "multi_family"
    if "single" in s or "sfr" in s or "house" in s or "detached" in s:
        return "single_family"

    # fall through: keep normalized raw string (useful for debug)
    return s

def is_allowed_type(norm_type: str | None) -> bool:
    # Keep unknown as allowed for now (you can tighten later)
    if not norm_type:
        return True
    return norm_type not in DISALLOWED_TYPES

def is_disallowed_type(raw_type: str | None) -> tuple[bool, str | None, str | None]:
    """
    Contract expected by refresh:
      returns (disallowed: bool, norm_type: str|None, reason_key: str|None)

    reason_key is designed for your drop_reasons counters:
      - raw_type::<Original>
      - norm_type::<normalized>
    """
    if not raw_type:
        return (False, None, None)

    norm = normalize_property_type(raw_type)
    if not norm:
        return (False, None, None)

    if norm in DISALLOWED_TYPES:
        # both raw and normalized reasons are useful
        return (True, norm, f"raw_type::{raw_type}")

    return (False, norm, None)
