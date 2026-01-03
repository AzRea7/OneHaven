# tests/test_domain_rental_gating.py
from app.domain.types import Strategy, Enrichment
from app.domain.scoring import compute_score, DealInputs


def test_rental_hard_gate_blocks_without_rent():
    score = compute_score(
        strategy=Strategy.rental,
        deal=DealInputs(list_price=200000, bedrooms=3, bathrooms=2, sqft=1500),
        enrichment=Enrichment(rent_estimate=None, arv_estimate=250000),
    )
    assert score.blocked is True
    assert "missing rent_estimate" in (score.explain or "")
    assert score.rank_score == 0.0
