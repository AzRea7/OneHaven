# backend/app/services/normalize.py
from __future__ import annotations

import re

# Your internal allow-list for this engine (SFH only, optionally duplex/plex if you want later)
ALLOWED_NORM_TYPES: set[str] = {
    "single_family",
    # enable later if desired:
    # "duplex",
    # "triplex",
    # "fourplex",
}

# Anything here is excluded from refresh/top-deals
DISALLOWED_NORM_TYPES: set[str] = {
    "condo",
    "townhouse",
    "apartment",
    "multifamily",     # generic multi-family
    "manufactured",
    "mobile_home",
    "land",
    "lot",
    "farm",
    "commercial",
    "unknown",
}


def normalize_property_type(raw: object) -> str:
    """
    Map messy upstream property type strings into your internal normalized types.
    Conservative: unknown => 'unknown' (which we disallow).
    """
    if raw is None:
        return "unknown"

    s = str(raw).strip().lower()
    s = re.sub(r"[\s_/|-]+", " ", s)

    # Common explicit disallowed types
    if any(k in s for k in ["condo", "condominium"]):
        return "condo"
    if any(k in s for k in ["townhouse", "town home", "townhouse/condo", "town house", "rowhouse", "row house"]):
        return "townhouse"
    if any(k in s for k in ["apartment", "apt", "flat"]):
        return "apartment"
    if any(k in s for k in ["manufactured", "mobile", "trailer", "modular"]):
        return "manufactured"
    if any(k in s for k in ["land", "lot", "vacant", "acre", "acreage"]):
        return "land"
    if any(k in s for k in ["commercial", "retail", "industrial", "office"]):
        return "commercial"
    if any(k in s for k in ["farm", "agricultural"]):
        return "farm"

    # Multi-family signals
    if any(k in s for k in ["multi family", "multifamily", "2 family", "3 family", "4 family", "plex"]):
        return "multifamily"

    # Allowed single family patterns
    if any(k in s for k in ["single family", "singlefamily", "sfh", "detached", "house"]):
        return "single_family"

    # Some providers use "Residential" without details — treat as unknown so we don't pollute.
    if s in ("residential", "home", "property"):
        return "unknown"

    return "unknown"


def is_allowed_type(norm_type: str | None) -> bool:
    """
    True if normalized type is explicitly allowed by the engine.
    This is used by ingest.py and tests.
    """
    if not norm_type:
        return False
    return norm_type in ALLOWED_NORM_TYPES


def is_disallowed_type(raw: object) -> tuple[bool, str, str]:
    """
    Returns: (is_disallowed, normalized_type, reason_key)
    reason_key is useful for drop_reasons counters.
    """
    norm = normalize_property_type(raw)

    if norm in DISALLOWED_NORM_TYPES:
        return True, norm, f"norm_type::{norm}"

    if not is_allowed_type(norm):
        return True, norm, f"norm_type::{norm}"

    return False, norm, ""
