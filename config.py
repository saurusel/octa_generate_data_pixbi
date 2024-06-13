from pydantic_settings import BaseSettings
from pydantic import SecretStr


class Settings(BaseSettings):
    db_name: str
    db_user: str
    db_pass: SecretStr
    db_host: str
    db_port: int

    class Config:
        env_file = ".env"


settings = Settings()
