from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """ Настройки приложения """

    APP_NAME: str = "AI Agent Prompt Injection Detector"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False

    # База данных
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_NAME: str = "detector_db"

    @property
    def DATABASE_URL(self) -> str:
        """ Формирует asyncpg URL для SQLAlchemy """
        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


settings = Settings()