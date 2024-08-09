from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from helpers.serp_api import SerpAPI

import os

class SerpAPISettings(BaseSettings):
    """
    Configuration settings for SerpAPI integration.

    Attributes:
        token (str): The API token for authenticating with SerpAPI.
        model_config (SettingsConfigDict): Configuration dictionary to define environment variable prefixes.
    """
    token: str = os.getenv("SERP_API_TOKEN")
    model_config: SettingsConfigDict = SettingsConfigDict(env_prefix='nightcrawler_serpapi_')


class ZyteSettings(BaseSettings):
    """
    Configuration settings for Diffbot (Zyte) integration.

    Attributes:
        url (str): The base URL for the Diffbot service.
        token (str): The API token for authenticating with Diffbot.
        check_interval (int): The interval (in seconds) for checking the status of jobs or tasks.
        model_config (SettingsConfigDict): Configuration dictionary to define environment variable prefixes.
    """
    url: str = ""
    token: str = os.getenv("ZYTE_API_TOKEN")
    check_interval: int = 30
    model_config: SettingsConfigDict = SettingsConfigDict(env_prefix='nightcrawler_diffbot_')


class Settings(BaseSettings):
    """
    Centralized application settings combining multiple service configurations.

    Attributes:
        serpapi (SerpAPISettings): Configuration settings for SerpAPI.
        diffbot (ZyteSettings): Configuration settings for Diffbot (Zyte).
    """
    serpapi: SerpAPISettings = Field(default_factory=SerpAPISettings)
    diffbot: ZyteSettings = Field(default_factory=ZyteSettings)
