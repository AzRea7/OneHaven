from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...config import settings
from .quantile_local import Quantiles


@dataclass(frozen=True)
class ConformalCalibrator:
    """
    Minimal conformal calibrator for prediction intervals.

    Stores an additive adjustment (delta) that widens/narrows intervals so that
    empirically P(y in [p10, p90]) ~= target coverage on held-out data.

    This is intentionally simple:
      - You can upgrade to per-segment deltas later (zip buckets, property type, etc.)
    """
    delta: float = 0.0  # widen interval by +/- delta

    @classmethod
    def load(cls) -> "ConformalCalibrator":
        path = Path(settings.ML_MODEL_DIR) / "conformal.json"
        if not path.exists():
            return cls(delta=0.0)
        obj = json.loads(path.read_text(encoding="utf-8"))
        return cls(delta=float(obj.get("delta", 0.0)))

    def apply(self, q: Quantiles) -> Quantiles:
        if q.p10 is None or q.p50 is None or q.p90 is None:
            return q
        return Quantiles(
            p10=max(0.0, float(q.p10) - self.delta),
            p50=float(q.p50),
            p90=max(0.0, float(q.p90) + self.delta),
        ).clamp()
