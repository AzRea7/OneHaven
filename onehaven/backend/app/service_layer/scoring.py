# app/service_layer/scoring.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, Property
from ..domain.deal_scoring import estimate_arv, estimate_rehab, deal_score
from ..domain.ranking import rank_score, explain as explain_ranker
from ..domain.motivation import motivation_score


async def score_lead(session: AsyncSession, lead: Lead, prop: Property, **kwargs: Any) -> None:
    """
    Score lead using domain modules.
    IMPORTANT: uses enriched lead.arv_estimate when present, otherwise heuristic.
    """
    arv = lead.arv_estimate if lead.arv_estimate is not None else estimate_arv(lead.list_price)
    rehab = estimate_rehab(prop.sqft)
    lead.rehab_estimate = rehab

    dscore = float(
        deal_score(
            lead.list_price,
            arv,
            rehab,
            lead.rent_estimate,
            strategy=str(lead.strategy.value),
        )
    )

    mscore = float(motivation_score(prop, lead))
    rscore = float(rank_score(dscore, mscore, strategy=str(lead.strategy.value)))

    lead.deal_score = dscore
    lead.motivation_score = mscore
    lead.rank_score = rscore
    lead.updated_at = datetime.utcnow()

    drivers = {
        "gross_yield": None,
        "dscr_proxy": None,
        "coc_proxy": None,
        "rent_sanity": None,
        "price_to_arv": None,
        "base_discount": None,
    }

    try:
        if lead.list_price and lead.rent_estimate and lead.list_price > 0:
            drivers["gross_yield"] = (lead.rent_estimate * 12.0) / lead.list_price
        if lead.list_price and arv and arv > 0:
            drivers["price_to_arv"] = lead.list_price / arv
            drivers["base_discount"] = max((arv - lead.list_price) / arv, 0.0)
    except Exception:
        pass

    try:
        ex = explain_ranker(
            dscore,
            mscore,
            is_auction=bool(kwargs.get("is_auction", False)),
            absentee=False,
            equity=None,
            drivers=drivers,
        )
    except Exception:
        ex = f"deal={dscore:.2f} | motivation={mscore:.2f}"

    lead.explain_json = ex
    await session.flush()
