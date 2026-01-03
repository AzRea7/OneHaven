# app/domain/motivation.py
from __future__ import annotations

from ..models import Lead, Property


def motivation_score(prop: Property, lead: Lead) -> float:
    """
    Deterministic, stable signature.
    Replace logic with your real motivation model later.
    """
    # placeholder default while you wire real logic
    return 0.2
