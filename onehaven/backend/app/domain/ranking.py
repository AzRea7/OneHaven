# app/domain/ranking.py
from __future__ import annotations

from typing import Any


def rank_score(deal_score: float, motivation_score: float, *, strategy: str) -> float:
    # MOVE your existing implementation from app/scoring/ranker.py
    # placeholder: weighted sum
    return max(0.0, min(100.0, deal_score * 0.85 + motivation_score * 15.0))


def explain(
    deal_score: float,
    motivation_score: float,
    *,
    is_auction: bool,
    absentee: bool,
    equity: float | None,
    drivers: dict[str, Any] | None = None,
) -> str:
    # MOVE your existing explain() implementation from app/scoring/ranker.py
    d = drivers or {}
    return f"deal={deal_score:.2f} | motivation={motivation_score:.2f} | drivers={d}"
