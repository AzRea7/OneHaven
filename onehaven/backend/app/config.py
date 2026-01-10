# app/config.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "dev"
    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    API_KEY: str | None = None

    # rentcast_listings | mls_reso | mls_grid | realcomp_direct
    # Default to realcomp_direct because RentCast is not assumed available.
    INGESTION_SOURCE: str = "realcomp_direct"

    MLS_PRIMARY_NAME: str = "realcomp"

    # RentCast (optional; leave blank if you don’t have access)
    RENTCAST_BASE_URL: str = "https://api.rentcast.io"
    RENTCAST_API_KEY: str = ""

    # Generic RESO Web API (token already acquired)
    RESO_BASE_URL: str = ""
    RESO_ACCESS_TOKEN: str = ""

    # Direct Realcomp (OAuth2 -> RESO Web API)
    REALCOMP_RESO_BASE_URL: str = ""      # e.g. https://<host>/reso/odata
    REALCOMP_TOKEN_URL: str = ""          # e.g. https://<host>/oauth/token
    REALCOMP_CLIENT_ID: str = ""
    REALCOMP_CLIENT_SECRET: str = ""
    REALCOMP_SCOPE: str = ""              # optional

    HTTP_TIMEOUT_S: float = 20.0
    HTTP_MAX_RETRIES: int = 3
    HTTP_BACKOFF_BASE_S: float = 0.35
    HTTP_RATE_LIMIT_RPS: float = 8.0
    HTTP_CIRCUIT_FAIL_THRESHOLD: int = 8
    HTTP_CIRCUIT_RESET_S: float = 30.0


settings = Settings()
