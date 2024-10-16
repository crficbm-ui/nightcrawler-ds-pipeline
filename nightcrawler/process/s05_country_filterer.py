import logging
import ast
import time
import re
import urllib.parse

import abc
import tqdm

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

from urllib.parse import urlparse
from typing import List
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


from nightcrawler.base import DeliveryPolicyData, PipelineResult, BaseStep

from helpers import LOGGER_NAME
from helpers.api.llm_apis import MistralAPI
from helpers.api.zyte_api import ZyteAPI
from helpers.context import Context
from helpers.settings import Settings
from helpers import utils_io, utils_strings

import pandas as pd

logger = logging.getLogger(LOGGER_NAME)


# Download NLTK resources (only required for the first time)
nltk.download("punkt")
nltk.download("stopwords")
nltk.download("punkt_tab")

STEMMER = PorterStemmer()
LANGS = stopwords.fileids()


from nightcrawler.base import BaseCountryFilterer


# Master country filterer
class MasterCountryFilterer(BaseCountryFilterer):
    """Master country filterer."""

    def __init__(
        self,
        filterer_name: str,
        country: str | None = None,
        config: dict | None = None,
        config_filterers: dict | None = None,
        **setting,
    ) -> None:
        super().__init__(name=filterer_name, config=config)
        
        self.filterers: list[BaseCountryFilterer] = []
        for filterer_name in filterer_name.split("+"):
            match filterer_name:
                case "known_domains":
                    self.filterers.append(
                        KnownDomainsFilterer(
                            **setting,
                            config=config,
                            config_filterers=config_filterers,
                            country=country,
                        )
                    )
                case "url":
                    self.filterers.append(
                        UrlCountryFilterer(
                            **setting,
                            config_filterers=config_filterers,
                            country=country,
                        )
                    )
                case _:
                    raise ValueError(f"Unknown filterer: {filterer_name}")

    def filter_page(self, **page: str) -> int:
        """Filter page with Master.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """

        for filterer in self.filterers:
            page = filterer.filter_page(**page)
            if "RESULT" in page:
                page["filterer_name"] = filterer.name
                return page

        # Enter here if no filterer has returned a result (ie neither known_domains nor url)
        page["RESULT"] = self.RESULT_UNKNOWN
        page["filterer_name"] = "unknown"

        return page


# Known domains filterer
class KnownDomainsFilterer(BaseCountryFilterer):
    """Known domains filterer."""

    def __init__(
        self,
        *,
        domains_pos: list[str] | None = None,
        domains_unknwn: list[str] | None = None,
        domains_neg: list[str] | None = None,
        country: str | None = None,
        config: dict | None = None,
        config_filterers: dict | None = None,
    ) -> None:
        super().__init__(
            name="known_domains",
            config=config,
            config_filterers=config_filterers,
            country=country,
        )
        # Known domains
        self.domains_pos = domains_pos or self.setting.get("domains_pos") # or []
        self.domains_unknwn = domains_unknwn or self.setting.get(
            "domains_unknwn"
        ) # or []
        self.domains_neg = domains_neg or self.setting.get("domains_neg") # or []

        # Keep these variables to save new classified domains
        self.path_settings = config.get("PATH_CURRENT_PROJECT_SETTINGS")
        self.country = country

    def filter_page(self, **page: str) -> int:
        """Filter page with known domains.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """
        # Recover url
        url = page.get("page_url", "")

        # Process url
        url = process_url(url)

        # Parse url
        url_parsed = parse_url(url)

        # Extract domain
        domain = extract_domain(url_parsed)

        # Store domain
        page["domain"] = domain

        # Check if the domain is already known
        if domain in self.domains_pos:
            logger.info(f"Domain {domain} already classified as positive")
            page["RESULT"] = self.RESULT_POSITIVE

        elif domain in self.domains_unknwn:
            logger.info(f"Domain {domain} already classified as unknown")
            page["RESULT"] = self.RESULT_UNKNOWN

        elif domain in self.domains_neg:
            logger.info(f"Domain {domain} already classified as negative")
            page["RESULT"] = self.RESULT_NEGATIVE

        return page


def process_url(url: str) -> str:
    """Process url."""

    return url.lower()


def parse_url(url: str) -> urllib.parse.ParseResult:
    """Parse url."""

    return urllib.parse.urlparse(url)


def extract_domain(url_parsed: urllib.parse.ParseResult) -> str:
    """Extract domain from url."""

    return url_parsed.hostname or url_parsed.netloc


def extract_top_level_domain(domain: str) -> str:
    """Extract top-level domain from domain."""

    return domain.split(".")[-1]


def extract_sub_level_domains(domain: str) -> list[str]:
    """Extract sub-level domains from domain."""

    return domain.split(".")[:-1]


def extract_path_directories(url_parsed: urllib.parse.ParseResult) -> list[str]:
    """Extract path directories from url."""

    return url_parsed.path.split("/")


def extract_query_values(url_parsed: urllib.parse.ParseResult) -> list[str]:
    """Extract query values from url."""

    return [
        subvalue
        for values in urllib.parse.parse_qs(url_parsed.query).values()
        for value in values
        for subvalue in re.split(r"\W+", value)
    ]


# Url country filterer
class UrlCountryFilterer(BaseCountryFilterer):
    """Url country filterer."""

    def __init__(
        self,
        *,
        countries: list[str] | None = None,
        top_level_domains: list[str] | None = None,
        sub_level_domains: list[str] | None = None,
        languages: list[str] | None = None,
        currencies: list[str] | None = None,
        country: str | None = None,
        config_filterers: dict | None = None,
    ) -> None:
        super().__init__(name="url", config_filterers=config_filterers, country=country)

        self.countries = countries or self.setting.get("countries") or []
        self.top_level_domains = (
            top_level_domains or self.setting.get("top_level_domains") or []
        )
        self.sub_level_domains = (
            sub_level_domains or self.setting.get("sub_level_domains") or []
        )
        self.languages = languages or self.setting.get("languages") or []
        self.currencies = currencies or self.setting.get("currencies") or []

    def filter_page(self, **page: str) -> int:
        """Filter page with Url.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """
        # Recover url
        url = page.get("page_url", "")

        # Process url
        url = process_url(url)

        # Parse url
        url_parsed = parse_url(url)

        # Extract domain
        domain = extract_domain(url_parsed)

        # Check top-level domain
        top_level_domain = extract_top_level_domain(domain)

        if utils_strings.check_string_equals_any_substring(
            top_level_domain, self.top_level_domains
        ):
            page["RESULT"] = self.RESULT_POSITIVE

        # Check sub-level domains
        sub_level_domains = extract_sub_level_domains(domain)

        if utils_strings.check_any_string_equals_any_substring(
            sub_level_domains, self.top_level_domains + self.sub_level_domains
        ):
            page["RESULT"] = self.RESULT_POSITIVE

        # Check path directories
        path_directories = extract_path_directories(url_parsed)

        if utils_strings.check_any_string_equals_any_substring(
            path_directories, self.countries + self.languages + self.currencies
        ):
            page["RESULT"] = self.RESULT_POSITIVE

        # Check query parameters
        query_values = extract_query_values(url_parsed)

        if utils_strings.check_any_string_equals_any_substring(
            query_values, self.countries + self.languages + self.currencies
        ):
            page["RESULT"] = self.RESULT_POSITIVE

        return page
    

class CountryFilterer(BaseStep):
    """Implementation of the country filterer (step 5)"""

    _entity_name: str = __qualname__

    SETTINGS = Settings().country_filtering
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_FILTERERS = SETTINGS.config_filterers

    def __init__(self, context: Context, *args, **kwargs):
        self.config = kwargs.get("config", self.DEFAULT_CONFIG)
        self.config_filterers = kwargs.get(
            "config_filterers", self.DEFAULT_CONFIG_FILTERERS
        )
        super().__init__(self._entity_name)
        self.context = context

    def get_step_results(
        self, previous_steps_results: PipelineResult
    ) -> List[DeliveryPolicyData]:
        dataset = pd.DataFrame(
            {"page_url": [e.url for e in previous_steps_results.results]}
        )

        # Instantiate filterer
        filterer = MasterCountryFilterer(
            filterer_name=self.config["FILTERER_NAME"],
            country=self.config["COUNTRY"],
            config=self.config,
            config_filterers=self.config_filterers
        )

        # Perform filtering
        time_start = time.time()
        dataset = filterer.perform_filtering(dataset)
        time_end = time.time()

        # Compute time elapsed
        dataset["time_elapsed"] = time_end - time_start

        # Transform dataset to a dictionary
        dataset_to_dict = dataset.to_dict(orient="records")  # dict, list, records

        stage_results = []
        for element in previous_steps_results.results:
            if element.url in dataset["page_url"].values:
                entry = next(
                    (
                        item
                        for item in dataset_to_dict
                        if item["page_url"] == element.url
                    ),
                    None,
                )
                stage_results.append(
                    DeliveryPolicyData(
                        domain=entry.get("domain"),
                        filtererName=entry.get("filterer_name"),
                        **element,
                    )
                )

        return stage_results

    # def get_step_results(
    #     self, previous_steps_results: PipelineResult
    # ) -> List[DeliveryPolicyData]:
    #     dataset = pd.DataFrame(
    #         {"page_url": [e["url"] for e in previous_steps_results["results"]]}
    #     )

    #     # Instantiate filterer
    #     filterer = MasterCountryFilterer(
    #         filterer_name=self.config["FILTERER_NAME"],
    #         country=self.config["COUNTRY"],
    #         config=self.config,
    #         config_filterers=self.config_filterers
    #     )

    #     # Perform filtering
    #     time_start = time.time()
    #     dataset = filterer.perform_filtering(dataset)
    #     time_end = time.time()

    #     # Compute time elapsed
    #     dataset["time_elapsed"] = time_end - time_start

    #     # Transform dataset to a dictionary
    #     dataset_to_dict = dataset.to_dict(orient="records")  # dict, list, records

    #     stage_results = []
    #     for element in previous_steps_results["results"]:
    #         if element["url"] in dataset["page_url"].values:
    #             entry = next(
    #                 (
    #                     item
    #                     for item in dataset_to_dict
    #                     if item["page_url"] == element["url"]
    #                 ),
    #                 None,
    #             )
    #             stage_results.append(
    #                 DeliveryPolicyData(
    #                     domain=entry.get("domain"),
    #                     filtererName=entry.get("filterer_name"),
    #                     **element,
    #                 )
    #             )

    #     return stage_results

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic

        results = self.get_step_results(previous_step_results)

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results, pipelineResults=previous_step_results
        )

        self.store_results(
            pipeline_results,
            self.context.output_dir,
            self.context.processing_filename_country_filtering,
        )
        return pipeline_results
