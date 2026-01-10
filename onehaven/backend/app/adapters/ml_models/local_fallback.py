# app/adapters/ml_models/local_fallback.py
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Protocol

from ...config import settings
from ...models import Property
from ...service_layer.estimates import EstimateResult


class _Predictor(Protocol):
    def predict(self, X: list[list[float]]) -> list[float]: ...


@dataclass(frozen=True)
class Percentiles:
    p10: float
    p50: float
    p90: float

    def as_dict(self) -> dict[str, float]:
        return {"p10": float(self.p10), "p50": float(self.p50), "p90": float(self.p90)}


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


def _get_attr(obj: Any, *names: str) -> Any:
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return None


def _feature_vector(prop: Property) -> tuple[list[float], dict[str, Any]]:
    """
    Minimal but stable numeric feature vector.
    Keep it boring and versioned: stability > cleverness.
    """
    sqft = _coerce_float(_get_attr(prop, "sqft", "square_footage", "squareFootage")) or 0.0
    beds = _coerce_float(_get_attr(prop, "beds", "bedrooms")) or 0.0
    baths = _coerce_float(_get_attr(prop, "baths", "bathrooms")) or 0.0
    year = _coerce_float(_get_attr(prop, "year_built", "yearBuilt")) or 0.0
    lot = _coerce_float(_get_attr(prop, "lot_size", "lotSize")) or 0.0
    lat = _coerce_float(_get_attr(prop, "latitude", "lat")) or 0.0
    lon = _coerce_float(_get_attr(prop, "longitude", "lon")) or 0.0

    # Zip is categorical; we hash it into a stable numeric bucket.
    zip_code = str(_get_attr(prop, "zip_code", "zipCode") or "")
    zip_bucket = float(int(hashlib.sha256(zip_code.encode("utf-8")).hexdigest(), 16) % 10_000)

    feats = [sqft, beds, baths, year, lot, lat, lon, zip_bucket]
    meta = {
        "sqft": sqft,
        "beds": beds,
        "baths": baths,
        "year_built": year,
        "lot_size": lot,
        "lat": lat,
        "lon": lon,
        "zip_bucket": zip_bucket,
        "zipCode": zip_code,
        "feature_schema": "v1:[sqft,beds,baths,year,lot,lat,lon,zip_bucket]",
    }
    return feats, meta


def _feature_hash(feats: list[float]) -> str:
    raw = json.dumps([round(x, 6) for x in feats], separators=(",", ":"), sort_keys=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _safe_load_joblib(path: str) -> Any | None:
    try:
        import joblib  # type: ignore
        return joblib.load(path)
    except Exception:
        return None


@dataclass
class QuantileBundle:
    q10: _Predictor
    q50: _Predictor
    q90: _Predictor
    model_version: str


def _load_quantile_models(target: str) -> QuantileBundle | None:
    """
    Expects files like:
      models/rent_q10.joblib
      models/rent_q50.joblib
      models/rent_q90.joblib
    or:
      models/value_q10.joblib ...
    """
    base = settings.LOCAL_MODEL_DIR
    q10p = os.path.join(base, f"{target}_q10.joblib")
    q50p = os.path.join(base, f"{target}_q50.joblib")
    q90p = os.path.join(base, f"{target}_q90.joblib")

    q10 = _safe_load_joblib(q10p)
    q50 = _safe_load_joblib(q50p)
    q90 = _safe_load_joblib(q90p)

    if not (q10 and q50 and q90):
        return None

    return QuantileBundle(q10=q10, q50=q50, q90=q90, model_version=settings.MODEL_VERSION)


def _load_conformal_delta(target: str) -> float | None:
    """
    Optional conformal calibration artifact:
      models/rent_conformal_delta.json  -> {"delta": 123.4}
    This is a simple symmetric widening amount applied to (p10,p90).
    """
    path = os.path.join(settings.LOCAL_MODEL_DIR, f"{target}_conformal_delta.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        delta = _coerce_float(obj.get("delta"))
        if delta is None:
            return None
        return max(0.0, float(delta))
    except Exception:
        return None


def _apply_conformal(p: Percentiles, delta: float | None) -> Percentiles:
    if not delta:
        return p
    # widen interval; keep p50 unchanged
    return Percentiles(
        p10=max(0.0, p.p10 - delta),
        p50=p.p50,
        p90=max(0.0, p.p90 + delta),
    )


def _predict_quantiles(prop: Property, *, target: str) -> tuple[Percentiles | None, dict[str, Any]]:
    feats, meta = _feature_vector(prop)
    fh = _feature_hash(feats)
    meta["feature_hash"] = fh

    bundle = _load_quantile_models(target)
    if not bundle:
        meta["mode"] = "heuristic"
        return None, meta

    X = [feats]
    p10 = float(bundle.q10.predict(X)[0])
    p50 = float(bundle.q50.predict(X)[0])
    p90 = float(bundle.q90.predict(X)[0])

    # basic monotonic fix (quantiles should be ordered)
    p10, p50, p90 = sorted([p10, p50, p90])

    out = Percentiles(p10=max(0.0, p10), p50=max(0.0, p50), p90=max(0.0, p90))
    delta = _load_conformal_delta(target)
    out = _apply_conformal(out, delta)

    meta["mode"] = "local_model"
    meta["model_version"] = bundle.model_version
    meta["conformal_delta"] = delta
    return out, meta


def _bands(p50: float, spread: float) -> Percentiles:
    p10 = max(0.0, p50 * (1.0 - spread))
    p90 = max(0.0, p50 * (1.0 + spread))
    return Percentiles(p10=p10, p50=p50, p90=p90)


def _heuristic_rent(prop: Property) -> tuple[Percentiles, dict[str, Any]]:
    sqft = _coerce_int(_get_attr(prop, "sqft", "square_footage", "squareFootage")) or 1100
    beds = _coerce_int(_get_attr(prop, "beds", "bedrooms")) or 3

    # crude baseline; replace with learned model ASAP
    base = 900.0 + (sqft * 0.55) + (beds * 120.0)
    out = _bands(base, spread=0.22)
    return out, {"mode": "heuristic", "rule": "base+sqft+beds", "base": base}


def _heuristic_value(prop: Property) -> tuple[Percentiles, dict[str, Any]]:
    last_sale = _coerce_float(_get_attr(prop, "last_sale_price", "lastSalePrice"))
    sqft = _coerce_int(_get_attr(prop, "sqft", "square_footage", "squareFootage")) or 1100

    if last_sale and last_sale > 0:
        p50 = last_sale
        rule = "last_sale"
    else:
        # crude $/sqft; placeholder only
        p50 = float(sqft) * 165.0
        rule = "sqft*ppsf"

    out = _bands(p50, spread=0.18)
    return out, {"mode": "heuristic", "rule": rule}


def predict_local_rent_long_term(prop: Property) -> EstimateResult:
    """
    A) Local fallback for rent:
      - try local quantile model
      - else heuristic
    """
    q, meta = _predict_quantiles(prop, target="rent")
    if q is None:
        q, meta2 = _heuristic_rent(prop)
        meta.update(meta2)

    return EstimateResult(
        p10=q.p10,
        p50=q.p50,
        p90=q.p90,
        meta_json={"local": meta},
    )


def predict_local_value(prop: Property) -> EstimateResult:
    """
    A) Local fallback for value (optional; used for ARV-ish signals later).
    """
    q, meta = _predict_quantiles(prop, target="value")
    if q is None:
        q, meta2 = _heuristic_value(prop)
        meta.update(meta2)

    return EstimateResult(
        p10=q.p10,
        p50=q.p50,
        p90=q.p90,
        meta_json={"local": meta},
    )
