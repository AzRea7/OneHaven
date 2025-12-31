# app/api/connectors.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from ..security import require_api_key
from ..connectors.wayne import WayneAuctionConnector, get_health

router = APIRouter(prefix="/connectors", tags=["connectors"])


@router.get("/wayne/health", dependencies=[Depends(require_api_key)])
def wayne_health():
    return get_health()


@router.get("/wayne/test", dependencies=[Depends(require_api_key)])
async def wayne_test(zip: str = Query(...), limit: int = 50):
    c = WayneAuctionConnector()
    leads = await c.fetch(zipcode=zip, limit=limit)
    return {
        "zip": zip,
        "returned_leads": len(leads),
        "health": get_health(),
        "sample": [l.payload for l in leads[:3]],
        "snapshots_dir": get_health()["snapshots_dir"],
    }
