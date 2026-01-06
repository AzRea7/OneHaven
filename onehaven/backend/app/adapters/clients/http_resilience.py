# app/adapters/clients/http_resilience.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

from ...config import settings


@dataclass
class _CircuitState:
    fails: int = 0
    opened_at: float | None = None


_CIRCUIT = _CircuitState()
_RATE_LOCK = asyncio.Lock()
_LAST_TS = 0.0


def _circuit_is_open(now: float) -> bool:
    if _CIRCUIT.opened_at is None:
        return False
    return (now - _CIRCUIT.opened_at) < float(settings.HTTP_CIRCUIT_RESET_S)


def _circuit_on_success() -> None:
    _CIRCUIT.fails = 0
    _CIRCUIT.opened_at = None


def _circuit_on_failure() -> None:
    _CIRCUIT.fails += 1
    if _CIRCUIT.fails >= int(settings.HTTP_CIRCUIT_FAIL_THRESHOLD):
        _CIRCUIT.opened_at = time.time()


async def _rate_limit() -> None:
    """Very simple per-process limiter."""
    global _LAST_TS
    rps = float(settings.HTTP_RATE_LIMIT_RPS)
    if rps <= 0:
        return
    min_gap = 1.0 / rps
    async with _RATE_LOCK:
        now = time.time()
        wait = (_LAST_TS + min_gap) - now
        if wait > 0:
            await asyncio.sleep(wait)
        _LAST_TS = time.time()


async def resilient_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    data: Any | None = None,
) -> httpx.Response:
    now = time.time()
    if _circuit_is_open(now):
        raise httpx.HTTPError(f"circuit_open: refusing external call to {url}")

    await _rate_limit()

    timeout = httpx.Timeout(float(settings.HTTP_TIMEOUT_S))
    max_retries = int(settings.HTTP_MAX_RETRIES)
    backoff = float(settings.HTTP_BACKOFF_BASE_S)

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.request(method, url, headers=headers, params=params, json=json, data=data)

            if resp.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError("retryable_status", request=resp.request, response=resp)

            resp.raise_for_status()
            _circuit_on_success()
            return resp
        except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as e:
            last_exc = e
            _circuit_on_failure()
            if attempt >= max_retries:
                break
            await asyncio.sleep(min(5.0, backoff * (2**attempt)))

    assert last_exc is not None
    raise last_exc
