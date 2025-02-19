from pydantic_settings import BaseSettings
from datetime import datetime, timedelta

class Settings(BaseSettings):
    # Service URLs
    LOKI_URL: str = "http://localhost:3100"
    VICTORIA_URL: str = "http://localhost:8428"

    # Pipeline settings
    QUERY_INTERVAL: int = 300  # 5 minutes in seconds
    BATCH_SIZE: int = 1000
    LOG_LEVEL: str = "INFO"

    # Client settings
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5  # seconds
    REQUEST_TIMEOUT: int = 30  # seconds

    # Query settings
    LOKI_QUERY: str = '{topic="iaas-database-auditlogs"}'  # Verified Loki LogQL query format
    START_TIME: datetime = datetime.now() - timedelta(hours=1)
    END_TIME: datetime = datetime.now()

    class Config:
        env_file = ".env"
        case_sensitive = True
        env_prefix = ""  # No prefix for environment variables

settings = Settings()