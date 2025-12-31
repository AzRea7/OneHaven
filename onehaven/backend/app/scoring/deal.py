# app/scoring/deal.py
from __future__ import annotations

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
    """
    Standard amortization payment.
    """
    r = annual_rate / 12.0
    n = years * 12
    if r <= 0:
        return loan_amount / n
    return loan_amount * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def rental_sanity_score(list_price: float | None, rent: float | None) -> float:
    """
    Punish “rent doesn’t remotely match price”.
    Uses rent-to-price (annualized) as a crude cap-rate proxy.

    Example:
      price=400k, rent=2600 => annual rent 31.2k => 7.8% gross => decent-ish
      price=4M, rent=2200 => annual rent 26.4k => 0.66% => terrible => near zero score
    """
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return 0.2  # unknown => small neutral (don’t zero out everything)
    gross_yield = (rent * 12.0) / list_price  # e.g. 0.08 = 8%
    # Map: 1% => ~0, 6% => ~0.6, 10% => ~1 (rough)
    x = (gross_yield - 0.01) / (0.10 - 0.01)
    return max(0.0, min(1.0, x))


def dscr_proxy_score(list_price: float | None, rent: float | None) -> float:
    """
    Very rough DSCR proxy:
    NOI ≈ rent * 0.65 (35% expense load)
    Debt service based on 80% LTV @ 7% 30yr
    DSCR = NOI / debt_service
    """
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return 0.2
    noi = rent * 0.65
    loan = list_price * 0.80
    debt = _monthly_mortgage_payment(loan)
    if debt <= 0:
        return 0.0
    dscr = noi / debt
    # Map DSCR: 0.8 => 0, 1.0 => 0.4, 1.25 => 1.0
    x = (dscr - 0.8) / (1.25 - 0.8)
    return max(0.0, min(1.0, x))


def coc_proxy_score(list_price: float | None, rent: float | None) -> float:
    """
    Rough cash-on-cash proxy:
    Cash invested = 20% down + 3% closing
    Cashflow ≈ NOI - debt
    CoC annual = cashflow*12 / cash_invested
    """
    if not list_price or not rent or list_price <= 0 or rent <= 0:
        return 0.2
    noi = rent * 0.65
    loan = list_price * 0.80
    debt = _monthly_mortgage_payment(loan)
    cf = noi - debt
    cash = list_price * 0.23
    if cash <= 0:
        return 0.0
    coc = (cf * 12.0) / cash
    # Map: -5% => 0, 0% => 0.4, 8% => 1.0
    x = (coc + 0.05) / (0.08 + 0.05)
    return max(0.0, min(1.0, x))


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
        sanity = rental_sanity_score(list_price, rent)
        dscr_s = dscr_proxy_score(list_price, rent)
        coc_s = coc_proxy_score(list_price, rent)
        viability = (0.40 * sanity) + (0.35 * dscr_s) + (0.25 * coc_s)
        return max(0.0, min(1.0, base * viability))

    return base
