from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='/opt/airflow/.env', 
        env_file_encoding='utf-8', 
        extra='ignore'
    )
    DFT_YEAR: int
    DFT_MONTH: int
    DFT_MONTH_STR: str
    DFT_CHUNK_SIZE: int
    BASE_URL: str
    BASE_DIR_PATH: str
    DFT_DWLD_CHUNK_SIZE: int
