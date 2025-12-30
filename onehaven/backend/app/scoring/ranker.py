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


def explain(deal: float, motivation: float, is_auction: bool, absentee: bool, equity: float | None) -> str:
    bits = []
    bits.append(f"deal={deal:.2f}")
    bits.append(f"motivation={motivation:.2f}")
    if is_auction:
        bits.append("auction_signal")
    if absentee:
        bits.append("absentee_signal")
    if equity is not None:
        bits.append(f"equity≈{equity:.2f}")
    return " | ".join(bits)
