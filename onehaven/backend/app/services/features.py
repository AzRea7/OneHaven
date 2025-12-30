from datetime import datetime

def years_since(dt: datetime | None) -> float | None:
    if not dt:
        return None
    days = (datetime.utcnow() - dt).days
    return max(days / 365.25, 0.0)


def equity_proxy(arv: float | None, price: float | None) -> float | None:
    if arv is None or price is None or arv <= 0:
        return None
    return (arv - price) / arv  # fraction below ARV


def vacancy_proxy(owner_mailing: str | None, address_line: str | None) -> float:
    # crude: if mailing differs from property address, treat as "absentee-ish"
    if not owner_mailing or not address_line:
        return 0.0
    return 1.0 if owner_mailing.strip().lower() not in address_line.strip().lower() else 0.0
