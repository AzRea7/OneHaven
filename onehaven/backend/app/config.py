from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    # Listings provider (RentCast today; could be swapped later)
    RENTCAST_API_KEY: str | None = None
    RENTCAST_BASE_URL: str = "https://api.rentcast.io/v1"

    # Property records provider (keep separate from listings!)
    PROPERTY_RECORDS_API_KEY: str | None = None
    PROPERTY_RECORDS_BASE_URL: str | None = None

    DEFAULT_REGION: str = "se_michigan"

    # Scheduler tuning
    SCHED_REFRESH_REGION: str = "se_michigan"
    SCHED_REFRESH_INTERVAL_MINUTES: int = 1440
    SCHED_DISPATCH_INTERVAL_MINUTES: int = 5


settings = Settings()
