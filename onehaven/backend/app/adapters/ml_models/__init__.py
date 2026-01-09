# app/adapters/ml_models/__init__.py
from .local_fallback import predict_local_value, predict_local_rent_long_term

__all__ = ["predict_local_value", "predict_local_rent_long_term"]
