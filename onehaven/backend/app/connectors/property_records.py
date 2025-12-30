import httpx
from ..config import settings

class PropertyRecordsClient:
    """
    Minimal enrichment client. Returns dict you merge into your Property.
    """
    def __init__(self) -> None:
        self.base = settings.RENTCAST_BASE_URL
        self.key = settings.RENTCAST_API_KEY

    async def enrich(self, address: str, city: str, state: str, zipcode: str) -> dict:
        if not self.key:
            return {}

        headers = {"X-Api-Key": self.key}
        url = f"{self.base}/property"
        params = {"address": address, "city": city, "state": state, "zipCode": zipcode}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=headers, params=params)
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json() if isinstance(r.json(), dict) else {}
