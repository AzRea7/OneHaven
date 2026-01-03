# app/domain/deal_scoring.py
from __future__ import annotations


def estimate_arv(list_price: float | None) -> float | None:
    # MOVE your existing implementation from app/scoring/deal.py
    if not list_price:
        return None
    return float(list_price) * 1.15  # placeholder


def estimate_rehab(sqft: int | None) -> float:
    # MOVE your existing implementation from app/scoring/deal.py
    if not sqft:
        return 15000.0
    return max(15000.0, float(sqft) * 10.0)


def deal_score(
    list_price: float | None,
    arv: float | None,
    rehab: float | None,
    rent_estimate: float | None,
    *,
    strategy: str,
) -> float:
    # MOVE your existing implementation from app/scoring/deal.py
    if not list_price or list_price <= 0:
        return 0.0
    if strategy == "rental":
        if not rent_estimate:
            return 0.0
        return min(100.0, (rent_estimate * 12.0 / list_price) * 1000.0)
    # flip
    if not arv:
        return 0.0
    spread = arv - list_price - (rehab or 0.0)
    return max(0.0, min(100.0, (spread / list_price) * 100.0))
