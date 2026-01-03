# app/domain/types.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Strategy(str, Enum):
    rental = "rental"
    flip = "flip"


@dataclass(frozen=True)
class Enrichment:
    rent_estimate: float | None
    arv_estimate: float | None
    rent_source: str | None = None
    arv_source: str | None = None


@dataclass(frozen=True)
class Score:
    deal_score: float
    motivation_score: float
    rank_score: float
    explain: str
    blocked: bool = False
    block_reason: str | None = None
