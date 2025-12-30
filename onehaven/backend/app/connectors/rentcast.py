import httpx
from .base import RawLead
from ..config import settings

class RentCastConnector:
    """
    Minimal stub. Replace endpoint paths with whatever you use.
    """
    def __init__(self) -> None:
        self.base = settings.RENTCAST_BASE_URL
        self.key = settings.RENTCAST_API_KEY

    async def fetch_listings(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        if not self.key:
            # No key: return empty for dev
            return []

        headers = {"X-Api-Key": self.key}
        # NOTE: endpoint path may differ by plan/product; keep this pluggable.
        url = f"{self.base}/listings/sale"
        params = {"zipCode": zipcode, "limit": limit}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()

        leads: list[RawLead] = []
        for item in data if isinstance(data, list) else data.get("listings", []):
            leads.append(
                RawLead(
                    source="rentcast_listing",
                    source_ref=str(item.get("id") or item.get("listingId") or ""),
                    payload=item,
                    provenance={"zip": zipcode, "provider": "rentcast"},
                )
            )
        return leads
