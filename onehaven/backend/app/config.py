# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Runtime ---
    ENV: str = "dev"  # dev|prod
    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    # --- Minimal B2B Auth (your API) ---
    API_KEY: str | None = None  # clients send X-API-Key: <key>

    # --- Listings provider (RentCast today; could be swapped later) ---
    RENTCAST_API_KEY: str | None = None
    RENTCAST_BASE_URL: str = "https://api.rentcast.io/v1"

    # --- RESO / MLS provider (optional; used by reso_web_api.py) ---
    RESO_BASE_URL: str | None = None
    RESO_ACCESS_TOKEN: str | None = None

    # --- Property records provider (separate from listings) ---
    PROPERTY_RECORDS_API_KEY: str | None = None
    PROPERTY_RECORDS_BASE_URL: str | None = None

    # --- Wayne auction connector (public pages) ---
    WAYNE_HTTP_TIMEOUT_S: int = 30
    WAYNE_HTTP_CACHE_ENABLED: bool = True
    WAYNE_HTTP_SLEEP_S: float = 0.25  # be polite
    WAYNE_USER_AGENT: str = "onehaven/1.0 (+local dev)"
    WAYNE_VERIFY_SSL: bool = True
    WAYNE_CA_BUNDLE: str | None = None

    DEFAULT_REGION: str = "se_michigan"

    # --- Scheduler tuning ---
    SCHED_REFRESH_REGION: str = "se_michigan"
    SCHED_REFRESH_INTERVAL_MINUTES: int = 1440  # daily
    SCHED_DISPATCH_INTERVAL_MINUTES: int = 5
    SCHED_DISPATCH_BATCH_SIZE: int = 50

    # --- Ingestion selection ---
    INGESTION_SOURCE: str = "rentcast_listings" 
    MLS_PRIMARY_NAME: str = "unknown"

    RESO_BASE_URL: str = ""
    RESO_ACCESS_TOKEN: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
