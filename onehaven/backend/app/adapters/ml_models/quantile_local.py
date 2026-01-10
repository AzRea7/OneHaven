from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...config import settings
from ...models import Property


@dataclass(frozen=True)
class Quantiles:
    p10: float | None
    p50: float | None
    p90: float | None

    def clamp(self) -> "Quantiles":
        # enforce monotonicity if numbers exist
        vals = [self.p10, self.p50, self.p90]
        if any(v is None for v in vals):
            return self
        p10, p50, p90 = float(self.p10), float(self.p50), float(self.p90)
        if p10 > p50:
            p10 = p50
        if p50 > p90:
            p90 = p50
        return Quantiles(p10=p10, p50=p50, p90=p90)


class LocalQuantileModel:
    """
    Local quantile prediction adapter.

    In “month-without-vendor” mode:
      - returns a baseline estimate + honest wide intervals
      - keeps pipeline behavior stable
      - lets you populate EstimateCache(p10/p50/p90) for golden tests

    Later (real ML):
      - drop trained artifacts into data/models/
      - this loader can be extended to load LightGBM quantile models
    """

    def __init__(self) -> None:
        self.model_dir = Path(settings.ML_MODEL_DIR)

        # Optional config file for simple baseline tuning without retraining
        self.baseline_path = self.model_dir / "baseline_quantiles.json"
        self._baseline: dict[str, Any] | None = None

    def _load_baseline(self) -> dict[str, Any]:
        if self._baseline is not None:
            return self._baseline
        if self.baseline_path.exists():
            self._baseline = json.loads(self.baseline_path.read_text(encoding="utf-8"))
        else:
            self._baseline = {}
        return self._baseline

    def predict_rent(self, prop: Property) -> Quantiles:
        """
        Uses simple heuristics now, designed to be replaced by a trained model.

        Key idea (the “heads turn” part):
          We return uncertainty explicitly, and we are OK being wide
          until we can calibrate on real outcomes.
        """
        b = self._load_baseline()

        beds = float(prop.bedrooms or 3)
        baths = float(prop.bathrooms or 2)
        sqft = float(prop.square_feet or 1600)

        # baseline $/sqft/month proxy (intentionally conservative)
        # You can tune these in baseline_quantiles.json without retraining.
        base_rate = float(b.get("rent_rate_per_sqft", 1.25))  # dollars per sqft per month / 1000-ish scale
        bed_bonus = float(b.get("bed_bonus", 125.0))
        bath_bonus = float(b.get("bath_bonus", 90.0))

        # scale sqft to something sane
        p50 = (sqft * base_rate / 1000.0) * 1000.0 + (beds - 2) * bed_bonus + (baths - 1) * bath_bonus

        # uncertainty width:
        # wide in absence of data; narrows later via conformal calibration
        width = max(250.0, 0.22 * p50)

        q = Quantiles(p10=max(0.0, p50 - width), p50=max(0.0, p50), p90=max(0.0, p50 + width))
        return q.clamp()
