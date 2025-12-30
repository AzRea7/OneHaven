from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    RENTCAST_API_KEY: str | None = None
    RENTCAST_BASE_URL: str = "https://api.rentcast.io/v1"

    # Separate config for property records provider (can be same provider)
    PROPERTY_RECORDS_API_KEY: str | None = None
    PROPERTY_RECORDS_BASE_URL: str = "https://api.rentcast.io/v1"

    # Wayne ingestion tuning
    WAYNE_HTTP_CACHE_ENABLED: bool = True
    WAYNE_HTTP_TIMEOUT_S: int = 30

    DEFAULT_REGION: str = "se_michigan"


settings = Settings()
