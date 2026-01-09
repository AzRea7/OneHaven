# app/entrypoints/api/routers/debug.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from ..deps import require_api_key
from ....service_layer.estimates import snapshot_global_stats, reset_global_stats

router = APIRouter(tags=["debug"])

@router.get("/debug/estimates/stats", dependencies=[Depends(require_api_key)])
def debug_estimates_stats(reset: bool = Query(default=False)) -> dict[str, Any]:
    if reset:
        reset_global_stats()
    return {"estimate_cache": snapshot_global_stats()}
