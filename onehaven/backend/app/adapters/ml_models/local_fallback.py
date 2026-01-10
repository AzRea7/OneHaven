# app/adapters/ml_models/local_fallback.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...models import Property
from ...service_layer.estimates import EstimateResult


def _get_attr(obj: Any, *names: str) -> Any:
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return None


def _coerce_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _coerce_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


@dataclass(frozen=True)
class Percentiles:
    p10: float
    p50: float
    p90: float

    def as_dict(self) -> dict[str, float]:
        return {"p10": float(self.p10), "p50": float(self.p50), "p90": float(self.p90)}


def _bands(p50: float, *, spread: float) -> Percentiles:
    p10 = max(0.0, p50 * (1.0 - spread))
    p90 = max(0.0, p50 * (1.0 + spread))
    return Percentiles(p10=p10, p50=p50, p90=p90)


def _local_value_heuristic(prop: Property) -> tuple[float | None, dict[str, Any]]:
    last_sale_price = _coerce_float(_get_attr(prop, "last_sale_price", "lastSalePrice"))
    sqft = _coerce_int(_get_attr(prop, "sqft", "square_footage", "squareFootage"))
    beds = _coerce_int(_get_attr(prop, "beds", "bedrooms"))
    baths = _coerce_float(_get_attr(prop, "baths", "bathrooms"))

    DEFAULT_PPSF = 165.0  # baseline placeholder; tune later

    features: dict[str, Any] = {
        "last_sale_price": last_sale_price,
        "sqft": sqft,
        "beds": beds,
        "baths": baths,
        "ppsf_used": None,
        "rule": None,
    }

    if last_sale_price and last_sale_price > 0:
        p50 = last_sale_price * 1.08
        features["rule"] = "anchor:last_sale_price*1.08"
        return p50, features

    if sqft and sqft > 0:
        ppsf = DEFAULT_PPSF
        if beds and beds >= 4:
            ppsf *= 1.03
        if baths and baths >= 2.5:
            ppsf *= 1.02

        p50 = float(sqft) * float(ppsf)
        features["ppsf_used"] = ppsf
        features["rule"] = "anchor:sqft*ppsf"
        return p50, features

    return None, features


def _local_rent_heuristic(prop: Property, *, value_p50: float | None) -> tuple[float | None, dict[str, Any]]:
    beds = _coerce_int(_get_attr(prop, "beds", "bedrooms"))
    sqft = _coerce_int(_get_attr(prop, "sqft", "square_footage", "squareFootage"))

    features: dict[str, Any] = {
        "beds": beds,
        "sqft": sqft,
        "value_p50": value_p50,
        "rule": None,
        "grm": None,
    }

    if value_p50 and value_p50 > 0:
        grm = 120.0
        monthly = float(value_p50) / (grm * 12.0)
        if sqft and sqft >= 2000:
            monthly *= 1.05
        if beds and beds >= 4:
            monthly *= 1.04
        features["grm"] = grm
        features["rule"] = "anchor:value/grm/12"
        return max(0.0, monthly), features

    if beds and beds > 0:
        base = 950.0 + (beds - 2) * 275.0
        features["rule"] = "anchor:beds_baseline"
        return max(0.0, base), features

    return None, features


async def predict_local_value(prop: Property) -> EstimateResult:
    p50, feats = _local_value_heuristic(prop)
    if p50 is None:
        return EstimateResult(value=None, source="local_model:no_signal", raw={"features": feats})

    pct = _bands(float(p50), spread=0.18)
    raw = {"kind": "value", "method": "heuristic_v1", **pct.as_dict(), "features": feats}

    # ✅ return percentiles explicitly
    return EstimateResult(
        value=pct.p50,
        source="local_model",
        raw=raw,
        p10=pct.p10,
        p50=pct.p50,
        p90=pct.p90,
    )


async def predict_local_rent_long_term(prop: Property) -> EstimateResult:
    value_est = await predict_local_value(prop)
    value_p50 = value_est.value

    p50, feats = _local_rent_heuristic(prop, value_p50=value_p50)
    if p50 is None:
        return EstimateResult(value=None, source="local_model:no_signal", raw={"features": feats})

    pct = _bands(float(p50), spread=0.14)
    raw = {"kind": "rent_long_term", "method": "heuristic_v1", **pct.as_dict(), "features": feats}

    return EstimateResult(
        value=pct.p50,
        source="local_model",
        raw=raw,
        p10=pct.p10,
        p50=pct.p50,
        p90=pct.p90,
    )
