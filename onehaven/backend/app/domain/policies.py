# app/domain/policies.py
from __future__ import annotations

from dataclasses import dataclass

from .types import Strategy, Enrichment


DISALLOWED_RAW_TYPES = {
    "condo",
    "condominium",
    "townhouse",
    "town home",
    "manufactured",
    "mobile",
    "land",
    "lot",
}


def is_disallowed_property_type(raw_type: str | None) -> bool:
    if not raw_type:
        return False
    s = raw_type.strip().lower()
    return any(bad in s for bad in DISALLOWED_RAW_TYPES)


def score_gate(strategy: Strategy, enrichment: Enrichment) -> tuple[bool, str | None]:
    """
    Returns (blocked, reason)
    """
    if strategy == Strategy.rental and not enrichment.rent_estimate:
        return True, "missing rent_estimate (rental strategy hard gate)"
    return False, None
