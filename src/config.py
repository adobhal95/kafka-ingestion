from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_DB: str
    POSTGRES_PASSWORD: str
    KAFKA_SERVER: str
    KAFKA_USERNAME: str
    KAFKA_PASSWORD: str
    SCHEMA_SERVER: str
    SCHEMA_USERNAME: str
    SCHEMA_PASSWORD: str
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: str
    SNOWFLAKE_WAREHOUSE: str
    SNOWFLAKE_DATABASE: str
    SNOWFLAKE_SCHEMA: str
    SNOWFLAKE_ROLE: str
    model_config = SettingsConfigDict(env_file=f"{Path(__file__).resolve().parents[1]}/.env", env_file_encoding='utf-8')


env_setting = Settings()
