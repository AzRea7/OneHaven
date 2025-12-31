# onehaven/backend/app/connectors/wayne_auction.py
from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import certifi
import httpx
from bs4 import BeautifulSoup

from ..config import settings
from .base import RawLead

BASE = "https://waynecountytreasurermi.com/"
BATCHES_URL = urljoin(BASE, "batches.html")
PROPS_URL = urljoin(BASE, "properties.html")  # ?batchId=<id>


@dataclass
class WayneHealth:
    fetched: int = 0
    parsed_batches: int = 0
    parsed_properties: int = 0
    leads_emitted: int = 0
    errors: int = 0
    last_error: str | None = None


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _sleep_polite() -> None:
    if settings.WAYNE_HTTP_SLEEP_S > 0:
        time.sleep(settings.WAYNE_HTTP_SLEEP_S)


class WayneAuctionConnector:
    """
    Conservative connector:
      - snapshots fetched HTML
      - extracts batch ids, then property rows
      - emits RawLead objects for the target zip

    SSL strategy:
      - default: verify SSL using certifi bundle
      - dev-only escape hatch: WAYNE_VERIFY_SSL=0 (do not use in prod)
    """

    def __init__(self) -> None:
        self.timeout = settings.WAYNE_HTTP_TIMEOUT_S
        self.snap_dir = os.path.join(os.getcwd(), "data", "wayne_snapshots")
        _ensure_dir(self.snap_dir)
        self.health = WayneHealth()

    def _snapshot_path(self, key: str) -> str:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return os.path.join(self.snap_dir, f"{ts}_{key}.html")

    def _http_verify(self) -> bool | str:
        """
        httpx 'verify' can be:
          - True/False
          - path to CA bundle
        """
        if not settings.WAYNE_VERIFY_SSL:
            return False
        # If user provides explicit CA bundle path, use it; else certifi.
        if getattr(settings, "WAYNE_CA_BUNDLE", None):
            return settings.WAYNE_CA_BUNDLE
        return certifi.where()

    async def _fetch_html(self, url: str) -> str:
        self.health.fetched += 1
        _sleep_polite()

        headers = {
            "User-Agent": settings.WAYNE_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        async with httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers=headers,
            verify=self._http_verify(),
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            html = r.text

        with open(self._snapshot_path(_sha(url)), "w", encoding="utf-8") as f:
            f.write(html)

        return html

    def _parse_batch_ids(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        batch_ids: list[str] = []

        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            first = tds[0].get_text(" ", strip=True)
            if first and re.fullmatch(r"\d{1,6}", first):
                batch_ids.append(first)

        # de-dupe keep order
        out: list[str] = []
        seen: set[str] = set()
        for b in batch_ids:
            if b not in seen:
                out.append(b)
                seen.add(b)

        self.health.parsed_batches += len(out)
        return out

    def _parse_property_rows(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        props: list[dict[str, Any]] = []

        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            row_text = tr.get_text(" ", strip=True)
            zipm = re.search(r"\b\d{5}\b", row_text)
            if not zipm:
                continue

            detail_href = None
            a = tr.find("a", href=True)
            if a:
                detail_href = a["href"]

            addr = tds[1].get_text(" ", strip=True)

            props.append(
                {
                    "address_line": addr,
                    "zip": zipm.group(0),
                    "detail_url": urljoin(BASE, detail_href) if detail_href else None,
                    "raw_row": row_text[:500],
                }
            )

        self.health.parsed_properties += len(props)
        return props

    async def fetch_by_zip(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        """
        Return auction leads for a single zip.
        Payload intentionally minimal; enrichment comes later.
        """
        try:
            batches_html = await self._fetch_html(BATCHES_URL)
            batch_ids = self._parse_batch_ids(batches_html)

            leads: list[RawLead] = []

            # keep bounded; auctions pages can be big
            for bid in batch_ids[:25]:
                props_html = await self._fetch_html(f"{PROPS_URL}?batchId={bid}")
                rows = self._parse_property_rows(props_html)

                for r in rows:
                    if r["zip"] != zipcode:
                        continue

                    payload = {
                        "addressLine": r["address_line"],
                        # TODO: if you later parse details page, you can set actual city.
                        # For now, we keep a safe fallback.
                        "city": "DETROIT",
                        "state": "MI",
                        "zipCode": zipcode,
                        "propertyType": "single_family",
                    }

                    leads.append(
                        RawLead(
                            source="wayne_auction",
                            source_ref=r.get("detail_url") or f"batch:{bid}:{_sha(r['raw_row'])}",
                            payload=payload,
                            provenance={
                                "zip": zipcode,
                                "provider": "wayne_treasurer",
                                "batch_id": bid,
                                "detail_url": r.get("detail_url"),
                            },
                        )
                    )

                    if len(leads) >= limit:
                        break

                if len(leads) >= limit:
                    break

            self.health.leads_emitted += len(leads)
            return leads

        except Exception as e:
            self.health.errors += 1
            self.health.last_error = str(e)
            return []
