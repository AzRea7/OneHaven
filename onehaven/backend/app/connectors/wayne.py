# app/connectors/wayne.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

from .base import RawLead
from ..config import settings


@dataclass
class WayneHealth:
    fetched: int = 0
    parsed_batches: int = 0
    parsed_properties: int = 0
    leads_emitted: int = 0
    errors: int = 0
    last_error: str | None = None
    snapshots_dir: str = "data/wayne_snapshots/"


_health = WayneHealth()


def get_health() -> dict:
    return {
        "fetched": _health.fetched,
        "parsed_batches": _health.parsed_batches,
        "parsed_properties": _health.parsed_properties,
        "leads_emitted": _health.leads_emitted,
        "errors": _health.errors,
        "last_error": _health.last_error,
        "snapshots_dir": _health.snapshots_dir,
    }


def _snapshot(name: str, content: str) -> str:
    os.makedirs(_health.snapshots_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(_health.snapshots_dir, f"{ts}_{name}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class WayneAuctionConnector:
    """
    Legal-safe connector rules:
      - only public pages
      - no CAPTCHA/login bypass
      - rate limit
      - snapshot raw HTML for debugging
      - parser health tracking

    NOTE: Wayne sites change. If parsing yields 0 suddenly, raise visibility and keep last known data.
    """

    def __init__(self) -> None:
        self.base = getattr(settings, "WAYNE_BASE_URL", "https://waynecountytreasurermi.com").rstrip("/")
        self.verify_ssl = bool(getattr(settings, "WAYNE_VERIFY_SSL", True))
        self.timeout_s = int(getattr(settings, "WAYNE_HTTP_TIMEOUT_S", 30))
        self.sleep_s = float(getattr(settings, "WAYNE_HTTP_SLEEP_S", 0.6))

    async def fetch(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        """
        This is intentionally conservative: fetch a public search/list page and parse.
        If Wayne is JS-heavy, this may yield 0 until you switch to official PDF lists or permitted feed.
        """
        url = f"{self.base}/search.html"
        params = {"q": zipcode}  # placeholder; you will update after observing real query params

        try:
            async with httpx.AsyncClient(timeout=self.timeout_s, verify=self.verify_ssl) as client:
                _health.fetched += 1
                r = await client.get(url, params=params)
                r.raise_for_status()

            html = r.text
            _snapshot(f"wayne_search_{zipcode}", html)

            # polite rate limit between calls
            time.sleep(self.sleep_s)

            # Parse minimal table-like content (site likely needs reverse engineering)
            soup = BeautifulSoup(html, "lxml")
            _health.parsed_batches += 1

            # Very defensive: look for rows that resemble auction items
            # You will refine this after you inspect snapshots in data/wayne_snapshots/
            rows = soup.select("table tr")
            leads: list[RawLead] = []

            for tr in rows[: limit + 1]:
                tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
                if not tds or len(tds) < 2:
                    continue

                # Heuristic extraction – adjust after looking at HTML
                text = " | ".join(tds)
                if zipcode not in text:
                    continue

                # crude address guess: first cell often contains address
                address_line = tds[0]
                city = "DETROIT"
                state = "MI"

                payload = {
                    "addressLine": address_line,
                    "city": city,
                    "state": state,
                    "zipCode": zipcode,
                    "propertyType": "single_family",
                    "raw_text": text,
                }

                _health.parsed_properties += 1

                leads.append(
                    RawLead(
                        source="wayne_auction",
                        source_ref=text[:120],
                        payload=payload,
                        provenance={"zip": zipcode, "provider": "wayne_public_site"},
                    )
                )

            _health.leads_emitted += len(leads)
            return leads[:limit]

        except Exception as e:
            _health.errors += 1
            _health.last_error = str(e)
            return []
