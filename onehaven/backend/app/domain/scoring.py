# app/domain/scoring.py
from __future__ import annotations

from dataclasses import dataclass

from .types import Strategy, Enrichment, Score
from .policies import score_gate


@dataclass(frozen=True)
class DealInputs:
    list_price: float | None
    bedrooms: float | None
    bathrooms: float | None
    sqft: float | None


def compute_score(
    *,
    strategy: Strategy,
    deal: DealInputs,
    enrichment: Enrichment,
) -> Score:
    blocked, reason = score_gate(strategy, enrichment)
    if blocked:
        return Score(
            deal_score=0.0,
            motivation_score=0.0,
            rank_score=0.0,
            explain=f"blocked: {reason}",
            blocked=True,
            block_reason=reason,
        )

    # --- Minimal deterministic scoring kernel ---
    # Rental: favor high rent-to-price
    # Flip: favor arv - price spread
    if not deal.list_price or deal.list_price <= 0:
        return Score(0.0, 0.0, 0.0, "blocked: missing/invalid list_price", True, "invalid list_price")

    if strategy == Strategy.rental:
        rent = enrichment.rent_estimate or 0.0
        # rent yield approximation (monthly rent / price)
        rent_yield = rent / deal.list_price
        rank = min(100.0, max(0.0, rent_yield * 10000.0))  # scale to ~0-100
        return Score(
            deal_score=rank,
            motivation_score=0.0,
            rank_score=rank,
            explain=f"rental score from rent_yield={rent_yield:.5f}",
        )

    # flip
    arv = enrichment.arv_estimate or 0.0
    spread = arv - deal.list_price
    spread_pct = spread / deal.list_price
    rank = min(100.0, max(0.0, spread_pct * 100.0))
    return Score(
        deal_score=rank,
        motivation_score=0.0,
        rank_score=rank,
        explain=f"flip score from spread_pct={spread_pct:.3f}",
    )
