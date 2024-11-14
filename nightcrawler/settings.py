from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

import os

try:
    from libnightcrawler.settings import Settings as StorageSettings
except ImportError:
    StorageSettings = BaseSettings


class SerpAPISettings(BaseSettings):
    """
    Configuration settings for SerpAPI integration.

    Attributes:
        token (str): The API token for authenticating with SerpAPI.
        model_config (SettingsConfigDict): Configuration dictionary to define environment variable prefixes.
    """

    token: str = os.getenv("SERP_API_TOKEN")
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_prefix="nightcrawler_serpapi_"
    )


class ZyteSettings(BaseSettings):
    """
    Configuration settings for Zyte integration.

    Attributes:
        url (str): The base URL for the Zyte service.
        token (str): The API token for authenticating with Zyte.
        check_interval (int): The interval (in seconds) for checking the status of jobs or tasks.
        model_config (SettingsConfigDict): Configuration dictionary to define environment variable prefixes.
    """

    url: str = ""
    token: str = os.getenv("ZYTE_API_TOKEN")
    check_interval: int = 30
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_prefix="nightcrawler_zyte_"
    )


class DataForSeoAPISettings(BaseSettings):
    """
    Configuration settings for DataForSeoAPI integration.

    Attributes:
        api_config (dict): A dict to lookup the settings for a given location
    """

    domain: str = "api.dataforseo.com"
    username: str = os.getenv("DATAFORSEO_USERNAME")
    password: str = os.getenv("DATAFORSEO_PASSWORD")


class DeliveryPolicyExtractionSettings(BaseSettings):
    llm_api_prompt: str = """
    # Description of the task
        You are an assistant whose role is to determine whether the text on an e-commerce site's shipping policy page mentions shipping to {country_long}.

        
        # Response Instructions
            • When the e-commerce website's shipping policy page mentions that the site delivers globally, worldwide or in Europe, you should assume that the site delivers in {country_long} even if it is not explicitly mentioned.
            • You must respond in the following JSON format:    
                {{"is_shipping_{country}_answer": "<answer to if the site delivers to {country_long}>", 
                "is_shipping_{country}_justification": "<justification for the answer to if the site delivers to {country_long}>"}} 

                •  "<answer to if the site delivers to {country_long}>" is:
                    •  "yes" - if the text strictly mentions the fact that the website ships to {country_long} or if the text mentions that the site delivers globally, worldwide or in Europe
                    •  "no" - if the text strictly mentions the fact that the website does not ship to {country_long}
                    •  "not_clear" - if there is a lack of information about whether the site delivers to {country_long}
                
                • "<justification for the answer to if the site delivers to {country_long}>" is a brief explanation of the response choice for the relevant website. Explanations must be in English and must not exceed 80 tokens.
        
        # Example 1:
        • Text of an e-commerce shipping policy page:
            Lieferungen von Bestellungen über den Online Shop erfolgen weltweit.

            Bestellungen aus dem Ausland:
            Region
            Europa
            Albanien, Belarus (Weißrussland), Bosnien und Herzegowina, Gibraltar, Guernsey, Island, Jersey, Liechtenstein, Litauen, Mazedonien, Moldawien, Montenegro, Norwegen, {country_long}, Serbien, Ukraine

            Südamerika
            Argentinien, Bolivien, Brasilien, Chile, Ecuador, Falklandinseln (Malwinen), Französisch-Guayana, Guyana, Kolumbien, Paraguay, Peru, Suriname, Uruguay, Venezuela

        • Expected JSON response:
            {{"is_shipping_{country}_answer": "yes",
            "is_shipping_{country}_justification": "The text mentions that the site delivers worldwide, including {country_long}"}} 

        # Example 2:
        • Text of an e-commerce shipping policy page:
            VERSANDBEDINGUNGEN
            Der Versand innerhalb Deutschlands erfolgt als DHL-Paket.

            Die nachstehenden Versandkosten beinhalten die gesetzliche Mehrwertsteuer:
            • 4,99 € innerhalb Deutschland

            Soweit in der Artikelbeschreibung keine andere Frist angegeben ist, erfolgt die Lieferung der Ware innerhalb von 3 5 Tagen* nach Vertragsschluss (bei Vorauszahlung erst nach Eingang des vollständigen Kaufpreises und der Versandkosten).

            * gilt für Lieferungen innerhalb Deutschlands

            Die Abgabe unserer Artikel erfolgt nur in haushaltsüblichen Mengen.

        • Expected JSON response:
            {{"is_shipping_{country}_answer": "no",
            "is_shipping_{country}_justification": "{country_long} is not mentioned in the text and the text doesn't mention worldwide or European shipping"}} 
        
            
        # Text to be used for the task:
        Here is the text of the e-commerce site's shipping policy page you have to work on:
    """


class Settings(StorageSettings):
    """
    Centralized application settings combining multiple service configurations.

    Attributes:
        serp_api (SerpAPISettings): Configuration settings for SerpAPI.
        zyte (ZyteSettings): Configuration settings for Zyte.
    """

    serp_api: SerpAPISettings = Field(default_factory=SerpAPISettings)
    zyte: ZyteSettings = Field(default_factory=ZyteSettings)
    store_intermediate: bool = True
    model_config: SettingsConfigDict = SettingsConfigDict(env_prefix="nightcrawler_")
    data_for_seo: DataForSeoAPISettings = Field(default_factory=DataForSeoAPISettings)
    delivery_policy: DeliveryPolicyExtractionSettings = Field(
        default_factory=DeliveryPolicyExtractionSettings
    )
