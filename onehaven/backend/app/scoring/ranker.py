# app/scoring/ranker.py
from __future__ import annotations

from typing import Any


def rank_score(deal: float, motivation: float, strategy: str) -> float:
    """
    Strategy weighting:
      rental: deal matters more (cashflow viability proxy)
      flip: motivation matters more (seller urgency)
    """
    if strategy == "flip":
        w_deal, w_mot = 0.45, 0.55
    else:
        w_deal, w_mot = 0.65, 0.35
    return (w_deal * deal) + (w_mot * motivation)


def _fmt_float(x: float | None, decimals: int = 2) -> str | None:
    if x is None:
        return None
    try:
        return f"{float(x):.{decimals}f}"
    except Exception:
        return None


def explain(
    deal: float,
    motivation: float,
    *,
    is_auction: bool,
    absentee: bool,
    equity: float | None,
    drivers: dict[str, Any] | None = None,
) -> str:
    """
    Human-debuggable explanation string.
    Keep it short, but information-dense.

    Example:
      gross_yield=0.09 | dscr=1.18 | coc=0.14 | rent_sanity=ok | price_to_arv=0.83 |
      deal=0.13 | motivation=0.03 | equity≈0.13 | auction_signal
    """
    bits: list[str] = []

    if drivers:
        # stable ordering for readability
        order = [
            "gross_yield",
            "dscr_proxy",
            "coc_proxy",
            "rent_sanity",
            "price_to_arv",
            "base_discount",
        ]
        for k in order:
            if k not in drivers:
                continue
            v = drivers.get(k)
            if isinstance(v, float) or isinstance(v, int):
                # gross_yield/coc shown as decimals (0.09 not 9%)
                if k in ("gross_yield", "coc_proxy", "price_to_arv", "base_discount"):
                    s = _fmt_float(float(v), 3)
                else:
                    s = _fmt_float(float(v), 2)
                if s is not None:
                    bits.append(f"{k}={s}")
            else:
                bits.append(f"{k}={v}")

    bits.append(f"deal={deal:.2f}")
    bits.append(f"motivation={motivation:.2f}")

    if equity is not None:
        bits.append(f"equity≈{equity:.2f}")
    if is_auction:
        bits.append("auction_signal")
    if absentee:
        bits.append("absentee_signal")

    return " | ".join(bits)
