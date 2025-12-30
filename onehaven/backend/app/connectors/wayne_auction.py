from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

from ..config import settings
from .base import RawLead

WAYNE_SEARCH_URL = "https://waynecountytreasurermi.com/search.html"


@dataclass
class ParserHealth:
    fetched: int = 0
    parsed_rows: int = 0
    leads_emitted: int = 0
    errors: int = 0


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


class WayneAuctionConnector:
    """
    Production-grade scaffold:
      - caches responses (simple disk snapshot)
      - stores raw HTML snapshots for debugging
      - has parser health counters
    To fully finish: reverse engineer the POST/search calls if results are not in initial GET HTML.
    """
    def __init__(self) -> None:
        self.timeout = settings.WAYNE_HTTP_TIMEOUT_S
        self.cache_enabled = settings.WAYNE_HTTP_CACHE_ENABLED
        self.snap_dir = os.path.join(os.getcwd(), "data", "wayne_snapshots")
        _ensure_dir(self.snap_dir)
        self.health = ParserHealth()

    def _snapshot_path(self, key: str) -> str:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return os.path.join(self.snap_dir, f"{ts}_{key}.html")

    async def _fetch_html(self, url: str, method: str = "GET", data: dict | None = None) -> str:
        self.health.fetched += 1

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            if method.upper() == "POST":
                r = await client.post(url, data=data or {})
            else:
                r = await client.get(url)

            r.raise_for_status()
            html = r.text

        # snapshot every fetch (cheap, and priceless for debugging)
        path = self._snapshot_path(_sha(url + (str(data) if data else "")))
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

        return html

    def _parse_results(self, html: str, zipcode: str, limit: int) -> list[RawLead]:
        """
        v1 parser: looks for tables/cards with addresses.
        If site is JS-rendered, this will emit 0; then you must switch to POST endpoints.
        """
        soup = BeautifulSoup(html, "lxml")

        leads: list[RawLead] = []

        # Heuristic example: search for any row containing the zip and an address-like pattern
        # Adjust once you inspect snapshots.
        text = soup.get_text(" ", strip=True)
        if zipcode not in text:
            return leads

        # Example: find all links that look like property detail pages
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            label = a.get_text(" ", strip=True)
            if not label:
                continue
            if zipcode not in label and zipcode not in href:
                continue
            if "property" not in href and "details" not in href and "parcel" not in href:
                continue

            # minimal payload; you will enrich once you know fields
            payload = {
                "address": label,
                "city": "DETROIT",
                "state": "MI",
                "zipCode": zipcode,
                "propertyType": "single_family",
            }
            leads.append(
                RawLead(
                    source="wayne_auction",
                    source_ref=href,
                    payload=payload,
                    provenance={"zip": zipcode, "provider": "wayne_treasurer", "detail_url": href},
                )
            )
            if len(leads) >= limit:
                break

        self.health.parsed_rows += len(leads)
        self.health.leads_emitted += len(leads)
        return leads

    async def fetch_by_zip(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        try:
            # v0 GET
            html = await self._fetch_html(WAYNE_SEARCH_URL)

            leads = self._parse_results(html, zipcode=zipcode, limit=limit)

            # If 0 leads, it may require POST search:
            # Once you inspect snapshot HTML, you’ll find the form names and post URL.
            # Then you implement:
            #   html = await self._fetch_html(WAYNE_SEARCH_URL, method="POST", data={"zip":zipcode,...})
            #   leads = self._parse_results(html,...)
            return leads

        except Exception:
            self.health.errors += 1
            return []
