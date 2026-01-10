# app/config.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Core
    ENV: str = "dev"
    API_KEY: str = "change-me"

    # DB (canonical)
    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    # Ingestion provider switch
    # dev-safe default so vendor downtime doesn't break local iteration
    INGESTION_SOURCE: str = "stub_json"

    # RESO / MLS (generic)
    RESO_BASE_URL: str | None = None
    RESO_ACCESS_TOKEN: str | None = None

    # Direct Realcomp (OAuth2 -> RESO Web API)
    REALCOMP_RESO_BASE_URL: str | None = None
    REALCOMP_TOKEN_URL: str | None = None
    REALCOMP_CLIENT_ID: str | None = None
    REALCOMP_CLIENT_SECRET: str | None = None
    REALCOMP_SCOPE: str | None = ""

    # RentCast (kept for modularity, but you will disable usage in the provider builder)
    RENTCAST_BASE_URL: str | None = "https://api.rentcast.io/v1"
    RENTCAST_API_KEY: str | None = None

    # Local ML models (A)
    LOCAL_MODEL_DIR: str = "./models"
    MODEL_VERSION: str = "local_v0"

    # Pydantic settings
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
