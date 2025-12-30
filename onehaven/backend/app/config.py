from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    HAVEN_DB_URL: str = "sqlite+aiosqlite:///./haven.db"

    RENTCAST_API_KEY: str | None = None
    RENTCAST_BASE_URL: str = "https://api.rentcast.io/v1"

    DEFAULT_REGION: str = "se_michigan"


settings = Settings()
