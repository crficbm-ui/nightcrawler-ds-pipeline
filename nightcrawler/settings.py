from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class SerpAPISettings(BaseSettings):
    url: str = ""
    token: str = ""
    model_config = SettingsConfigDict(env_prefix='nightcrawler_serpapi_')


class DiffbotSettings(BaseSettings):
    url: str = ""
    token: str = ""
    check_interval: int = 30
    model_config = SettingsConfigDict(env_prefix='nightcrawler_diffbot_')


class Settings(BaseSettings):
    serpapi: SerpAPISettings = Field(default_factory=SerpAPISettings)
    diffbot: DiffbotSettings = Field(default_factory=DiffbotSettings)
