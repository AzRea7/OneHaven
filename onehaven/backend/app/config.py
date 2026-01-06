# app/config.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Runtime ---
    ENV: str = "dev"  # dev|prod
    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    # --- Minimal B2B Auth (your API) ---
    API_KEY: str | None = None  # clients send X-API-Key: <key>

    # --- Ingestion source switch ---
    # rentcast_listings | mls_reso | mls_grid | realcomp_direct
    INGESTION_SOURCE: str = "rentcast_listings"

    # --- Traceability label (what MLS your endpoint represents) ---
    MLS_PRIMARY_NAME: str = "unknown"

    # --- RentCast ---
    RENTCAST_BASE_URL: str = "https://api.rentcast.io"
    RENTCAST_API_KEY: str = ""

    # --- Generic RESO Web API (when you have a token already) ---
    RESO_BASE_URL: str = ""
    RESO_ACCESS_TOKEN: str = ""

    # --- Direct Realcomp (OAuth2 -> RESO Web API) ---
    REALCOMP_RESO_BASE_URL: str = ""      # e.g. https://<host>/reso/odata
    REALCOMP_TOKEN_URL: str = ""          # e.g. https://<host>/oauth/token
    REALCOMP_CLIENT_ID: str = ""
    REALCOMP_CLIENT_SECRET: str = ""
    REALCOMP_SCOPE: str = ""              # optional; leave blank if not required

    # --- HTTP resiliency knobs (shared across external calls) ---
    HTTP_TIMEOUT_S: float = 20.0
    HTTP_MAX_RETRIES: int = 3
    HTTP_BACKOFF_BASE_S: float = 0.35

    # soft rate limit per-process (not perfect, but prevents self-DDoS)
    HTTP_RATE_LIMIT_RPS: float = 8.0

    # circuit breaker
    HTTP_CIRCUIT_FAIL_THRESHOLD: int = 8
    HTTP_CIRCUIT_RESET_S: float = 30.0


settings = Settings()
