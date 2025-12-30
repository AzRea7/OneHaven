import httpx
from bs4 import BeautifulSoup
from .base import RawLead

WAYNE_SEARCH_URL = "https://waynecountytreasurermi.com/search.html"

class WayneAuctionConnector:
    """
    A brittle-but-usable v0 scraper.
    In production: cache + retries + monitor HTML changes.
    """
    async def fetch_by_zip(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(WAYNE_SEARCH_URL)
            r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")

        # v0: if the site requires form submission/JS, this won’t work.
        # Keep this connector interface anyway; replace implementation later.
        leads: list[RawLead] = []

        # Placeholder: no structured results parsed
        # (You will likely need to reverse engineer the POST/search calls or use a headless fetch.)
        return leads
