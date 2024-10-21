import logging
import time
import re
import urllib.parse
from typing import List
import pandas as pd
from abc import ABC, abstractmethod
import tqdm

from nightcrawler.base import CountryFilteringData, PipelineResult, BaseStep
from helpers.context import Context
from helpers.settings import Settings
from helpers import utils_io, utils_strings, LOGGER_NAME
from helpers.utils import filter_dict_keys

logger = logging.getLogger(LOGGER_NAME)


# ---------------------------------------------------
# BaseCountryFilterer
# ---------------------------------------------------


# base country filterer
class BaseCountryFilterer(ABC):
    """Base class for country filterers."""

    RESULT_POSITIVE = +1
    RESULT_UNKNOWN = 0
    RESULT_NEGATIVE = -1

    def __init__(
        self,
        name: str,
        country: str | None = None,
        config: dict | None = None,
        config_filterer: dict | None = None,
    ) -> None:
        super().__init__()

        self.name = name

        if config:
            self.config = config
        else:
            self.config = {}

        if country:
            if name == "known_domains":
                self.setting = utils_io.load_setting(
                    path_settings=config.get("PATH_CURRENT_PROJECT_SETTINGS"),
                    country=country,
                    file_name=name,
                )
            else:
                self.setting = config_filterer
        else:
            self.setting = {}

    @abstractmethod
    def filter_page(self, **page: str) -> int:
        """Filter page.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """

        raise NotImplementedError

    def perform_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform filtering.
        Classifies for each url of the dataframe if delivery is possible to the country of interest.
        Executes (if specified in the settings) successively the known domains filter which will check 
        if in the domain registry if the domain associated with the url has already been classified, 
        then the url filter which will check if the url contains characters indicating delivery to the 
        country of interest.
        If the url is not classified by the 2 previous filters as delivering or not in the country of 
        interest then it is classified as "unknown".
        Each new classified domain is added to the domain registry.

        Args:
            df (pd.DataFrame): dataFrame to filter.

        Returns:
            pd.DataFrame: filtered dataFrame.
        """
        
        tqdm.tqdm.pandas(desc=f"Filtering with {self.name}...", leave=False)

        # Create a list to store rows' indexes and labels
        list_pages_labeled = []

        # Recover known_domains filterer index if known_domains filterer is used
        self.index_known_domains_filterer = self.get_index_known_domains_filterer()
        
        def pseudo_filter_page(row):
            return row.name, self.filter_page(
                **row.dropna().to_dict()
            )

        with tqdm.tqdm(total=len(df)) as pbar:
            for _, row in df.iterrows():
                # Get page_labeled and add the index
                row_index, page_labeled = pseudo_filter_page(row)
                page_labeled["index"] = row_index

                # Add it to the list
                list_pages_labeled.append(page_labeled)
                
                # Add the domain to known domains if known_domains filterer is used and domain is not already in known_domains and is not unknown
                if (self.index_known_domains_filterer is not None) & (
                    page_labeled["filterer_name"] not in ["known_domains", "unknown"] # i.e., = "url"
                ):
                    # Add domain to known_domains filterer
                    try:
                        self.add_domain_to_known_domains_filterer(
                            page_labeled=page_labeled
                        )

                    except Exception as e:
                        print(f"Error: {e}")

                pbar.update(1)
        
        # If the setting is set to save new classified domains
        if self.config["SAVE_NEW_CLASSIFIED_DOMAINS"]:
            # Save new known domains
            self.save_new_known_domains()

        # Create a df with the outputs
        df_labeled = pd.DataFrame(list_pages_labeled)

        return df_labeled

    def get_index_known_domains_filterer(self):
        # Recover index of known_domains filterer
        list_filterer_names = self.name.split("+")
        index_known_domains_filterer = (
            list_filterer_names.index("known_domains")
            if "known_domains" in list_filterer_names
            else None
        )

        return index_known_domains_filterer
    
    def add_domain_to_known_domains_filterer(self, page_labeled):
        # Recover label from page_labeled
        label, domain = page_labeled["RESULT"], page_labeled["domain"]

        # Filter page_labeled with relevant keys
        page_labeled_filtered = filter_dict_keys(
            original_dict=page_labeled, keys_to_save=self.config["KEYS_TO_SAVE"]
        )

        # Extract known_domains_filterer
        known_domains_filterer = self.filterers[self.index_known_domains_filterer]

        # Add domain labeled to dict
        if label == 1:
            known_domains_filterer.domains_pos[domain] = page_labeled_filtered

        elif label == 0:
            known_domains_filterer.domains_unknwn[domain] = page_labeled_filtered

        elif label == -1:
            known_domains_filterer.domains_neg[domain] = page_labeled_filtered

        # Update self.filterers
        self.filterers[self.index_known_domains_filterer] = known_domains_filterer

    def save_new_known_domains(self):
        # Extract known_domains_filterer
        known_domains_filterer = self.filterers[self.index_known_domains_filterer]

        dict_known_domains = {
            "domains_pos": known_domains_filterer.domains_pos,
            "domains_unknwn": known_domains_filterer.domains_unknwn,
            "domains_neg": known_domains_filterer.domains_neg,
        }

        _ = utils_io.save_and_load_setting(
            setting=dict_known_domains,
            path_settings=known_domains_filterer.path_settings,
            country=known_domains_filterer.country,
            file_name=known_domains_filterer.name,
        )


# ---------------------------------------------------
# MasterCountryFilterer
# ---------------------------------------------------


# Master country filterer
class MasterCountryFilterer(BaseCountryFilterer):
    """Master country filterer."""

    def __init__(
        self,
        filterer_name: str,
        country: str | None = None,
        config: dict | None = None,
        config_filterer: dict | None = None,
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
                            country=country,
                        )
                    )
                case "url":
                    self.filterers.append(
                        UrlCountryFilterer(
                            **setting,
                            config_filterer=config_filterer,
                            country=country,
                        )
                    )
                case _:
                    raise ValueError(f"Unknown filterer: {filterer_name}")

    def filter_page(self, **page: str) -> int:
        """Filter page with Master.
        Executes the known domain filterer and then the url filterer. 
        Naturally, if the known domain filterer gives a result for the url, then the url filterer is not executed.
        
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

        # Enter here if no filterer has returned a result (i.e., neither known_domains nor url)
        page["RESULT"] = self.RESULT_UNKNOWN
        page["filterer_name"] = "unknown"

        return page


# ---------------------------------------------------
# KnownDomainsFilterer
# ---------------------------------------------------


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
    ) -> None:
        super().__init__(
            name="known_domains",
            config=config,
            country=country,
        )
        # Known domains
        self.domains_pos = domains_pos or self.setting.get("domains_pos")
        self.domains_unknwn = domains_unknwn or self.setting.get(
            "domains_unknwn"
        )
        self.domains_neg = domains_neg or self.setting.get("domains_neg")

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


# ---------------------------------------------------
# UrlCountryFilterer
# ---------------------------------------------------


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
        config_filterer: dict | None = None,
    ) -> None:
        super().__init__(name="url", config_filterer=config_filterer, country=country)

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


# ---------------------------------------------------
# CountryFilterer
# ---------------------------------------------------

# Country filterer
class CountryFilterer(BaseStep):
    """Implementation of the country filterer (step 5)"""

    _entity_name: str = __qualname__

    SETTINGS = Settings().country_filtering
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_URL_FILTERER = SETTINGS.config_url_filterer

    def __init__(self, context: Context, *args, **kwargs):
        country = kwargs.get("country")

        self.config = self.DEFAULT_CONFIG.get(country)
        self.config_url_filterer = self.DEFAULT_CONFIG_URL_FILTERER.get(country)

        super().__init__(self._entity_name)
        self.context = context

    def get_step_results(
        self, previous_steps_results: PipelineResult
    ) -> List[CountryFilteringData]:
        dataset = pd.DataFrame(
            {"page_url": [e.url for e in previous_steps_results.results]}
        )

        # Instantiate filterer
        filterer = MasterCountryFilterer(
            filterer_name=self.config["FILTERER_NAME"],
            country=self.config["COUNTRY"],
            config=self.config,
            config_filterer=self.config_url_filterer
        )

        # Perform filtering
        dataset = filterer.perform_filtering(dataset)

        # Transform dataset to a dictionary
        dataset_to_dict = dataset.to_dict(orient="records")

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
                    CountryFilteringData(
                        domain=entry.get("domain"),
                        filtererName=entry.get("filterer_name"),
                        deliveringToCountry=entry.get("RESULT"),
                        **element,
                    )
                )

        return stage_results

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic

        time_start = time.time()
        results = self.get_step_results(previous_step_results)
        time_end = time.time()
        previous_step_results.meta.time_country_filterer = time_end - time_start

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
