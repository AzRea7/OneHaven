# app/scoring/deal.py
from __future__ import annotations

from dataclasses import dataclass


def estimate_rehab(sqft: int | None) -> float | None:
    if not sqft:
        return None
    return float(sqft) * 25.0  # v0 light rehab


def estimate_arv(list_price: float | None) -> float | None:
    if list_price is None:
        return None
    return list_price * 1.15  # v0


def estimate_rent(beds: int | None, sqft: int | None) -> float | None:
    if beds is None and sqft is None:
        return None
    base = 900.0
    if beds:
        base += beds * 250.0
    if sqft:
        base += max(sqft - 800, 0) * 0.5
    return base


def _monthly_mortgage_payment(loan_amount: float, annual_rate: float = 0.07, years: int = 30) -> float:
    r = annual_rate / 12.0
    n = years * 12
    if r <= 0:
        return loan_amount / n
    return loan_amount * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def gross_yield(list_price: float | None, rent: float | None) -> float | None:
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return None
    return (rent * 12.0) / list_price


def rental_sanity_score(list_price: float | None, rent: float | None) -> float:
    gy = gross_yield(list_price, rent)
    if gy is None:
        return 0.2  # unknown => small neutral

    x = (gy - 0.01) / (0.10 - 0.01)
    return max(0.0, min(1.0, x))


def dscr_proxy(list_price: float | None, rent: float | None) -> float | None:
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return None
    noi = rent * 0.65
    loan = list_price * 0.80
    debt = _monthly_mortgage_payment(loan)
    if debt <= 0:
        return None
    return noi / debt


def dscr_proxy_score(list_price: float | None, rent: float | None) -> float:
    dscr = dscr_proxy(list_price, rent)
    if dscr is None:
        return 0.2
    x = (dscr - 0.8) / (1.25 - 0.8)
    return max(0.0, min(1.0, x))


def coc_proxy(list_price: float | None, rent: float | None) -> float | None:
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return None
    noi = rent * 0.65
    loan = list_price * 0.80
    debt = _monthly_mortgage_payment(loan)
    cf = noi - debt
    cash = list_price * 0.23
    if cash <= 0:
        return None
    return (cf * 12.0) / cash


def coc_proxy_score(list_price: float | None, rent: float | None) -> float:
    coc = coc_proxy(list_price, rent)
    if coc is None:
        return 0.2
    x = (coc + 0.05) / (0.08 + 0.05)
    return max(0.0, min(1.0, x))


@dataclass(frozen=True)
class RentalViability:
    gross_yield: float | None
    rent_sanity_score: float
    dscr: float | None
    dscr_score: float
    coc: float | None
    coc_score: float
    viability_score: float


def rental_viability(list_price: float | None, rent: float | None) -> RentalViability:
    sanity = rental_sanity_score(list_price, rent)
    dscr_s = dscr_proxy_score(list_price, rent)
    coc_s = coc_proxy_score(list_price, rent)
    viability = (0.40 * sanity) + (0.35 * dscr_s) + (0.25 * coc_s)
    return RentalViability(
        gross_yield=gross_yield(list_price, rent),
        rent_sanity_score=sanity,
        dscr=dscr_proxy(list_price, rent),
        dscr_score=dscr_s,
        coc=coc_proxy(list_price, rent),
        coc_score=coc_s,
        viability_score=max(0.0, min(1.0, viability)),
    )


def deal_score(
    list_price: float | None,
    arv: float | None,
    rehab: float | None,
    rent: float | None = None,
    strategy: str = "rental",
) -> float:
    """
    v1 deal score:
      - base discount-to-ARV
      - rehab penalty
      - for rentals: multiply by rental viability (rent sanity + DSCR + CoC)
    """
    if list_price is None or arv is None or arv <= 0:
        return 0.0

    discount = max((arv - list_price) / arv, 0.0)  # 0..1
    rehab_penalty = 0.0
    if rehab is not None and arv > 0:
        rehab_penalty = min(rehab / arv, 1.0) * 0.5

    base = max(min(discount - rehab_penalty, 1.0), 0.0)

    if strategy == "rental":
        v = rental_viability(list_price, rent)
        return max(0.0, min(1.0, base * v.viability_score))

    return base  # ✅ FIX: no trailing comma


# -----------------------------
# Compatibility wrapper
# -----------------------------
def score_deal(
    *,
    list_price: float | None,
    beds: int | None,
    baths: float | None,
    sqft: int | None,
    rent: float | None,
    strategy: str,
) -> tuple[float, dict]:
    """
    Convenience wrapper used by ingest/scoring pipelines.
    Returns:
      (deal_score_value, drivers_dict)
    """
    rehab = estimate_rehab(sqft)
    arv = estimate_arv(list_price)

    # if rent missing, estimate a crude fallback (but you'll gate/penalize later)
    rent_used = rent if rent is not None else estimate_rent(beds=beds, sqft=sqft)

    ds = deal_score(
        list_price=list_price,
        arv=arv,
        rehab=rehab,
        rent=rent_used,
        strategy=strategy,
    )

    drivers = {
        "arv": arv,
        "rehab": rehab,
        "rent_used": rent_used,
        "gross_yield": gross_yield(list_price, rent_used),
        "dscr_proxy": dscr_proxy(list_price, rent_used),
        "coc_proxy": coc_proxy(list_price, rent_used),
        "rent_sanity": rental_sanity_score(list_price, rent_used),
        "price_to_arv": (list_price / arv) if (list_price and arv) else None,
        "base_discount": ((arv - list_price) / arv) if (list_price and arv) else None,
    }
    return float(ds), drivers
