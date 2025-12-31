import httpx
from ..config import settings


class PropertyRecordsClient:
    """
    Minimal enrichment client. Returns dict you merge into your Property.
    Intentionally decoupled from listings provider.
    """

    def __init__(self) -> None:
        # If you don't set PROPERTY_RECORDS_BASE_URL/KEY, this client is effectively disabled.
        self.base = settings.PROPERTY_RECORDS_BASE_URL
        self.key = settings.PROPERTY_RECORDS_API_KEY

    async def enrich(self, address: str, city: str, state: str, zipcode: str) -> dict:
        if not self.key or not self.base:
            return {}

        headers = {"X-Api-Key": self.key}
        url = f"{self.base}/property"
        params = {"address": address, "city": city, "state": state, "zipCode": zipcode}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=headers, params=params)
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            js = r.json()
            return js if isinstance(js, dict) else {}
