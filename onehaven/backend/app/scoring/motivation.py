from dataclasses import dataclass

@dataclass(frozen=True)
class MotivationSignals:
    is_auction: bool
    absentee: bool
    years_held: float | None
    equity_frac: float | None


def motivation_score(sig: MotivationSignals) -> float:
    """
    v0 rule model: auction + absentee + long hold + equity => higher.
    0..1
    """
    score = 0.0
    if sig.is_auction:
        score += 0.45
    if sig.absentee:
        score += 0.20
    if sig.years_held is not None:
        score += min(sig.years_held / 10.0, 1.0) * 0.20
    if sig.equity_frac is not None:
        score += max(min(sig.equity_frac, 1.0), 0.0) * 0.25
    return max(min(score, 1.0), 0.0)
