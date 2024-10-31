from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

import os

try:
    from libnightcrawler.settings import Settings as StorageSettings
except:
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

    api_params: dict = {
        "CH": {"location": "Switzerland", "language": "German"},
        "AT": {"location": "Austria", "language": "German"},
    }


class CountryFiltererSettings(BaseSettings):
    config: dict = {
        "CH": {
            "FILTERER_NAME": "known_domains+url",
            "COUNTRY": "ch",
            "PATH_CURRENT_PROJECT_SETTINGS": "nightcrawler/process/country_filtering",
            "DICT_MAPPING_KNOWN_DOMAINS": {
                "pos": "domains_pos",
                "unknwn": "domains_unknwn",
                "neg": "domains_neg",
            },
            "KEYS_TO_SAVE": [
                "filterer_name",
                "RESULT",
                "label_justif",
                "urls_shipping_policy_page_found_analysis", 
                "url_shipping_policy_page_kept",
            ],
            "SAVE_NEW_CLASSIFIED_DOMAINS": True,
            },
        "AT": {
            "FILTERER_NAME": "known_domains+url",
            "COUNTRY": "at",
            "PATH_CURRENT_PROJECT_SETTINGS": "nightcrawler/process/country_filtering",
            "DICT_MAPPING_KNOWN_DOMAINS": {
                "pos": "domains_pos",
                "unknwn": "domains_unknwn",
                "neg": "domains_neg",
            },
            "KEYS_TO_SAVE": [
                "filterer_name",
                "RESULT",
                "label_justif",
                "urls_shipping_policy_page_found_analysis", 
                "url_shipping_policy_page_kept",
            ],
            "SAVE_NEW_CLASSIFIED_DOMAINS": True,
            },
    }

    config_url_filterer: dict = {
        "CH": {
            "countries": ["ch", "che"],
            "top_level_domains": ["ch", "swiss"],
            "sub_level_domains": [],
            "languages": [
                "de-ch", "en-ch", "fr-ch", "gsw-ch", "it-ch", "pt-ch", "rm-ch", "wae-ch",
                "ch-de", "ch-en", "ch-fr", "ch-gsw", "ch-it", "ch-pt", "ch-rm", "ch-wae",
                "de_ch", "en_ch", "fr_ch", "gsw_ch", "it_ch", "pt_ch", "rm_ch", "wae_ch",
                "ch_de", "ch_en", "ch_fr", "ch_gsw", "ch_it", "ch_pt", "ch_rm", "ch_wae",
            ],
            "currencies": ["chf"],
        },
        "AT": {
            "countries": ["at"],
            "top_level_domains": [
                "at", "com", "de", "ch", "au", "eu",
            ],
            "sub_level_domains": [],
            "languages": [
                "de-at", "en-at", "sl-at", "hr-at", "hu-at", "ch-at", "it-at", "fr-at",
                "at-de", "at-en", "at-sl", "at-hr", "at-hu", "at-ch", "at-it", "at-fr",
                "de_at", "en_at", "sl_at", "hr_at", "hu_at", "ch_at", "it_at", "fr_at",
                "at_de", "at_en", "at_sl", "at_hr", "at_hu", "at_ch", "at_it", "at_fr",
            ],
            "currencies": ["eur"],
        }
    }

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

    config: dict = {
        "CH": {
            "COUNTRY": "ch",
            "COUNTRY_LONG": "Switzerland",
            "PATH_CURRENT_PROJECT_SETTINGS": "nightcrawler/process/country_filtering",
            "DICT_MAPPING_KNOWN_DOMAINS": {
                "pos": "domains_pos",
                "unknwn": "domains_unknwn",
                "neg": "domains_neg",
            },
            "KEYS_TO_SAVE": [
                "filterer_name",
                "RESULT",
                "label_justif",
                "url_shipping_policy_page_kept",
                "urls_shipping_policy_page_found_analysis",
            ],
            "SAVE_NEW_CLASSIFIED_DOMAINS": True,
        },
        "AT": {
            "COUNTRY": "at",
            "COUNTRY_LONG": "Austria",
            "PATH_CURRENT_PROJECT_SETTINGS": "nightcrawler/process/country_filtering",
            "DICT_MAPPING_KNOWN_DOMAINS": {
                "pos": "domains_pos",
                "unknwn": "domains_unknwn",
                "neg": "domains_neg",
            },
            "KEYS_TO_SAVE": [
                "filterer_name",
                "RESULT",
                "label_justif",
                "url_shipping_policy_page_kept",
                "urls_shipping_policy_page_found_analysis",
            ],
            "SAVE_NEW_CLASSIFIED_DOMAINS": True,
        }
    }

    config_filterer: dict = {
        "CH": {
            "keywords_shipping_policy": [
                "livraison", "expédit", "expedit",
                "lieferung", "versand", "liefer",
                "deliveri", "ship",
                "consegna", "spedizion",
            ],
            "urls_domains_shipping_pos": [
                "www.ebay.co.uk",
                "www.ebay.de",
                "www.herbkart.com",
                "www.hood.de",
                "www.ebay.com",
                "gloriaexports.com",
            ],
            "urls_domains_shipping_unknwn": [
                "www.etsy.com",
                "www.joom.com",
                "www.inspireuplift.com",
                "stockx.com",
                "saner.health",
                "www.amama.com.au",
                "www.biblio.com",
                "www.uline.com",
                "sparklingspices.us",
                "www.victorinox.com",
                "biaxol.com",
            ],
            "urls_domains_shipping_neg": ["www.eneba.com"],
            "llm_api_config": {"model": "mistral-large-latest", "temperature": 0.0},
            "llm_api_prompt": llm_api_prompt.format(
                country=config["CH"]["COUNTRY"], country_long=config["CH"]["COUNTRY_LONG"]
            ),
            "zyte_api_product_page_config": {
                "browserHtml": True,
                "screenshot": False,
                "product": False,
                "geolocation": "CH",
            },
            "zyte_api_policy_page_config": {
                "browserHtml": True,
                "screenshot": False,
                "product": False,
                "geolocation": "CH",
            },
            "use_concurrency": False,
        },
        "AT": {
            "keywords_shipping_policy": [
                "lieferung", "versand", "liefer",    # German (mainly spoken in Austria)
                "deliveri", "ship",                  # English
                "dostava", "pošiljka",               # Slovenian (spoken in Carinthia and Styria)
                "dostava", "pošiljka",               # Croatian (also a minority language in Austria)
                "dostava", "pošta"                   # Hungarian (spoken in some parts of Austria)
            ],
            "urls_domains_shipping_pos": [
            ],
            "urls_domains_shipping_unknwn": [
            ],
            "urls_domains_shipping_neg": [
            ],
            "llm_api_config": {"model": "mistral-large-latest", "temperature": 0.0},
            "llm_api_prompt": llm_api_prompt.format(
                country=config["AT"]["COUNTRY"], country_long=config["AT"]["COUNTRY_LONG"]
            ),
            "zyte_api_product_page_config": {
                "browserHtml": True,
                "screenshot": False,
                "product": False,
                "geolocation": "AT",
            },
            "zyte_api_policy_page_config": {
                "browserHtml": True,
                "screenshot": False,
                "product": False,
                "geolocation": "AT",
            },
            "use_concurrency": False,
        }
    }

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
    country_filtering: CountryFiltererSettings = Field(
        default_factory=CountryFiltererSettings
    )
    delivery_policy: DeliveryPolicyExtractionSettings = Field(
        default_factory=DeliveryPolicyExtractionSettings
    )
