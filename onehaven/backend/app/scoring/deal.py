def estimate_rehab(sqft: int | None) -> float | None:
    if not sqft:
        return None
    # v0 heuristic: $25/sqft light rehab
    return float(sqft) * 25.0


def estimate_arv(list_price: float | None) -> float | None:
    if list_price is None:
        return None
    # v0 heuristic: ARV ~ 1.15x list (replace with your quantile model later)
    return list_price * 1.15


def estimate_rent(beds: int | None, sqft: int | None) -> float | None:
    if beds is None and sqft is None:
        return None
    # v0 heuristic: simple beds-based
    base = 900.0
    if beds:
        base += beds * 250.0
    if sqft:
        base += max(sqft - 800, 0) * 0.5
    return base


def deal_score(list_price: float | None, arv: float | None, rehab: float | None) -> float:
    """
    v0 deal score: reward discount to ARV, penalize rehab ratio.
    0..1-ish
    """
    if list_price is None or arv is None or arv <= 0:
        return 0.0
    discount = max((arv - list_price) / arv, 0.0)  # 0..1
    rehab_penalty = 0.0
    if rehab is not None and arv > 0:
        rehab_penalty = min(rehab / arv, 1.0) * 0.5
    return max(min(discount - rehab_penalty, 1.0), 0.0)
