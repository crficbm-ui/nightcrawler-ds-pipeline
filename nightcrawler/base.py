import logging
import re
from re import Pattern
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, Iterator, List, Union
from collections.abc import Mapping
from datetime import datetime, timezone
from abc import ABC, abstractmethod
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from helpers.utils import _get_uuid, write_json, filter_dict_keys
from helpers.context import Context
from helpers import utils_io, LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


# ---------------------------------------------------
# Data Model - Abstract Classes used to either enforce specific class initialization or to provide common functionalities to its children classes.
# ---------------------------------------------------
@dataclass
class ObjectUtilitiesContainer(ABC, Mapping):
    """Abstract base class that allows for list-like object handling."""

    def to_dict(self) -> Dict[str, Optional[str]]:
        """
        Converts the dataclass to a dictionary, excluding any fields that are None.

        Returns:
            Dict[str, Optional[str]]: A dictionary representation of the instance with None fields removed.
        """
        return {
            k: v
            for k, v in asdict(self).items()
            if v is not None and v != -1 and v != ""
        }

    def get(self, attr: str, default: Any = None) -> Any:
        """
        Retrieves the value of the specified attribute.

        Args:
            attr (str): The name of the attribute to retrieve.
            default (Any): The value to return if the attribute is not found. Defaults to None.

        Returns:
            Any: The value of the attribute if it exists, otherwise the default value.
        """
        return getattr(self, attr, default)

    def __getitem__(self, key: str) -> Any:
        """
        Allows dict-like access to the attributes of the instance.
        The function will raise an error if the key is not found.

        Args:
            key (str): The attribute name to access.

        Returns:
            Any: The value of the attribute corresponding to the key.
        """
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(f"{key} not found in {self.__class__.__name__}")

    def __iter__(self) -> Iterator[str]:
        """
        Allows iteration over the attribute names of the instance.

        Returns:
            str: The attribute names.
        """
        return iter(self.to_dict())

    def __len__(self) -> int:
        """
        Returns:
            int: The number of non-None attributes.
        """
        return len(self.to_dict())

    def keys(self):
        """
        Returns:
            dict_keys: A view object that displays the keys of the dictionary
        representation of the instance.
        """
        return self.to_dict().keys()


# ---------------------------------------------------
# Data Model - Stage Classes
# ---------------------------------------------------
@dataclass
class MetaData(ObjectUtilitiesContainer):
    """Metadata class for storing information about the full pipeline run valid for all crawlresults"""

    keyword: str = field(default_factory=str)
    numberOfResults: int = field(default_factory=int)
    numberOfResultsAfterStage: int = field(default_factory=int)
    resultDate: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    )
    uuid: str = field(init=False)
    time_country_filterer: float = field(default_factory=float)
    time_delivery_policy_extractor: float = field(default_factory=float)

    def __post_init__(self):
        self.uuid = _get_uuid(self.keyword, self.resultDate)


@dataclass
class ExtractSerpapiData(ObjectUtilitiesContainer):
    """Data class for step 1, 2 and 3 (all steps serpapi related): Extract URLs using Serpapi"""

    offerRoot: str
    url: str
    keywordEnriched: Optional[str] = (
        None  # this is only used for keyword enrichement and holds enriched keyword (i.e. keyword is 'viagra' and enriched is 'viagra kaufen'). This is only used when '-e' is set
    )
    keywordVolume: Optional[
        float
    ] = -1  # this is only used for keyword enrichement and indicated how often this enriched keyword is used based on dataforseo estimates. This is only used when '-e' is set
    keywordLanguage: Optional[str] = (
        None  # this is only used for keyword enrichement and indicated the language of the enriched keyword (i.e. 'viagra kaufen' would be 'DE'). This is only used when '-e' is set
    )
    keywordLocation: Optional[str] = (
        None  # this is only used for keyword enrichement and indicated the dataforseo localization option i.e. 'CH'. This is only used when '-e' is set
    )
    imageUrl: Optional[str] = (
        None  # this is only used for the reverse image search and indicates the direct url to the image
    )


@dataclass
class ExtractZyteData(ExtractSerpapiData):
    """Data class for step 4: Use Zyte to retrieve structured information from each URL collected by serpapi"""

    price: Optional[str] = None
    title: Optional[str] = None
    fullDescription: Optional[str] = None
    zyteExecutionTime: Optional[float] = 0.0
    html: Optional[str] = None
    zyteProbability: Optional[float] = 0.0


@dataclass
class ProcessData(ExtractZyteData):
    """Data class for step 5: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists. All these depend on the --country input of the pipeline call.
    TODO replace the manual filtering logic with Mistral call by Nicolas W.


    TODO make this more abstract (not for CH but for any country) i.e.:
    country: Optional[str]
    countryInUrl: Optional[bool]
    webextensionInUrl: Optional[bool]
    currencyInUrl: Optional[bool]
    soldToCountry: Optional[bool]

    -> change the processor accordingly

    """

    ch_de_in_url: Optional[bool] = False
    swisscompany_in_url: Optional[bool] = False
    web_extension_in_url: Optional[bool] = False
    francs_in_url: Optional[bool] = False
    result_sold_CH: Optional[bool] = False


@dataclass
class CountryFilteringData(ExtractZyteData): # ProcessData
    """Data class for step 6: delivery policy filtering based on offline analysis of domains public delivery information"""

    domain: Optional[str] = None
    filtererName: Optional[str] = None
    deliveringToCountry: Optional[int] = None

@dataclass
class DeliveryPolicyData(CountryFilteringData): # ProcessData
    """Data class for step 6: delivery policy filtering based on offline analysis of domains public delivery information"""

    domain: Optional[str] = None
    filtererName: Optional[str] = None
    deliveringToCountry: Optional[int] = None
    labelJustif: Optional[str] = None

@dataclass
class PageTypeData(DeliveryPolicyData):
    """Data class for step 7: page type filtering based on either a probability of Zyte (=default) or a custom BERT model deployed on the mutualized GPU. The pageType can be either 'ecommerce_product' or 'other'."""

    pageType: Optional[str] = None


@dataclass
class BlockedContentData(PageTypeData):
    """Data class for step 8: blocked / corrupted content detection based the prediction with a BERT model."""

    # TODO add fields relevant to only this step
    pass


@dataclass
class ContentDomainData(BlockedContentData):
    """Data class for step 9: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)"""

    # TODO add fields relevant to only this step
    pass


@dataclass
class ProcessSuspiciousnessData(ContentDomainData):
    """Data class for step 10: binary classifier per organisation, whether a product is classified as suspicious or not.
    TODO: maybe this class can be deleted and the Suspiciousness step could return directly CrawlResultData as most likely no new variables will come used after this step
    TODO: add fields relevant to only this step
    """

    pass


@dataclass
class CrawlResultData(ProcessSuspiciousnessData):
    """Data class for step 11: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away."""

    # TODO add fields relevant to only this step
    pass


# ---------------------------------------------------
# Data Model - Class that will hold the MetaData and the Data per step. This is the main object that is passed from one step to the next and always append the new fields to the data objects and after the step will modify the MetaData.
# ---------------------------------------------------


@dataclass
class PipelineResult(ObjectUtilitiesContainer):
    """Class for storing a comprehensive report, including Zyte data."""

    meta: MetaData
    results: List[CrawlResultData]


# ---------------------------------------------------
# Data Model - Abstract Classes providing shared functionalities and / or enforcing method implementation at children classes.
# ---------------------------------------------------


class BaseStep(ABC):
    """
    Class that provides core functionality for all steps in the pipeline:
        - enforces an apply function that is used uniformly as the entry point to a new step. We do not care what happens inside apply, just that it exists.
        - provides a method to store the results on the filesystem the pipeline is running on. There should be a CLI option similar to --local-results in
          TODO where the current behaviour is fine, but the default should be to store to S3.
    """

    _step_counter = 0

    def __init__(self, context: Context) -> None:
        self._entity_name = (
            self.__class__.__qualname__
        )  # Automatically set name for children
        self.context = context
        BaseStep._step_counter += 1

    def store_results(
        self, structured_results: PipelineResult, output_dir: str, filename: str
    ) -> None:
        """
        Stores the structured results into a JSON file.

        Args:
            structured_results (PipelineResult): The structured data to be stored.
            output_dir (str): The directory where the JSON file will be saved.
        """
        # TODO try with deep copy
        structured_results_dict = PipelineResult(meta=MetaData(), results=[])
        structured_results_dict.meta = structured_results.meta.to_dict()
        structured_results_dict.results = [
            result.to_dict() for result in structured_results.results
        ]

        write_json(
            output_dir,
            f"{BaseStep._step_counter}_{filename}",
            structured_results_dict.to_dict(),
        )

    def add_pipeline_steps_to_results(
        self,
        currentStepResults: List[Any],
        pipelineResults: PipelineResult,
        currentStepResultsIsPipelineResultsObject=True,
    ) -> PipelineResult:
        # Depending on the class implementation the currentStepResults is either a List of DataObjects (default) or already a PipelineResult Object.
        if currentStepResultsIsPipelineResultsObject:
            # If it is a PipelineResult object, it does contain the results of all previous steps and the current results.
            results = currentStepResults
            # Update the number of results after stage
            pipelineResults.meta.numberOfResultsAfterStage = len(currentStepResults)
        else:
            # If not, we have to append the list of DataObjects generated during the current step to the results of the last step.
            results = pipelineResults.results + currentStepResults
            pipelineResults.meta.numberOfResultsAfterStage = len(results)

        updatedResults = PipelineResult(meta=pipelineResults.meta, results=results)
        return updatedResults

    @abstractmethod
    def apply_step(self, *args: Any, **kwargs: Any) -> Any:
        """Enforces the apply_step method, leaves the implementation up for the children classes"""
        pass

    def apply(self, *args: Any, **kwargs: Any) -> PipelineResult:
        logger.info(f"Executing step {BaseStep._step_counter}: {self._entity_name}")
        results = self.apply_step(*args, **kwargs)
        self.context.crawlStatus = self._entity_name + " successfull"
        return results


class Extract(BaseStep):
    """
    Abstract base class that enforces methods to interact with an API, process its results, and store the data.

    Subclasses should implement the following methods:
        - initiate_client: Initiate the API client.
        - retrieve_response: Make the API calls and retrieve data.
        - structure_results: Postprocess the API results into the desired format.
        - store_results: Write the results to the file system.
        - apply: Run the entire data collection process.

    Attributes:
        context (Any): The context object containing configuration and settings.
    """

    @abstractmethod
    def initiate_client(self) -> Any:
        """
        Initializes the API client necessary for making requests.
        Should set up any authentication or connection requirements.

        Returns:
            Any: An initialized API client instance.
        """
        pass

    @abstractmethod
    def retrieve_response(
        self, *args: Any, **kwargs: Any
    ) -> Union[List[PipelineResult], List[PipelineResult]]:
        """
        Makes the API calls to retrieve data.

        Args:
            *args (Any): Positional arguments required by the API call.
            **kwargs (Any): Keyword arguments required by the API call.

        Returns:
            Union[List[PipelineResult], List[PipelineResult]]: The raw response data from the API, either as a list of `PipelineResult` or `PipelineResult` instances.
        """
        pass

    @abstractmethod
    def structure_results(
        self,
        data: Union[List[PipelineResult], List[PipelineResult]],
        *args: Any,
        **kwargs: Any,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            data (Union[List[PipelineResult], List[PipelineResult]]): The raw data returned from the API.
            *args (Any): Additional positional arguments for structuring the data.
            **kwargs (Any): Additional keyword arguments for structuring the data.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The structured and processed data, either as a list of URLs (strings) or a list of dictionaries.
        """
        pass


# ---------------------------------------------------
# Extract - Martkeplaces
# ---------------------------------------------------


@dataclass
class Marketplace:
    name: str
    root_domain_name: str
    search_url_pattern: str
    product_page_url_pattern: str

    @property
    def keyword_pattern(self) -> Pattern:
        return re.compile(self.search_url_pattern % r"(\w+)")


def filter_product_page_urls(urls: list[str]) -> list[str]:
    accepted_urls = []
    for url in urls:
        if any(
            [
                re.match(marketplace.product_page_url_pattern, url)
                for marketplace in GOOGLE_SITE_MARKETPLACES
            ]
        ):
            accepted_urls.append(url)
    return accepted_urls


DEFAULT_MARKETPLACES = [
    Marketplace(
        "anibis",
        "anibis.ch",
        "https://www.anibis.ch/de/c/alle-kategorien?fts=%s",
        r"^https://www\.anibis\.ch/de/d-",
    ),
    Marketplace(
        "petitesannonces",
        "petitesannonces.ch",
        "https://www.petitesannonces.ch/recherche/?q=%s",
        r"^https://www\.petitesannonces\.ch/a/",
    ),
    Marketplace(
        "visomed",
        "visomed-marketplace.ch",
        "https://visomed-marketplace.ch/?s=%s",
        r"^https://visomed-marketplace\.ch/shop/",
    ),
    Marketplace(
        "locanto",
        "locanto.ch",
        "https://www.locanto.ch/q/?query=%s",
        r"^https://[a-zA-Z\-]*\.locanto\.ch/ID_",
    ),
    Marketplace(
        "gratisinserat",
        "gratisinserat.ch",
        "https://www.gratisinserat.ch/li/?q=%s",
        r"^https://www\.gratisinserat\.ch/[\w\-]*/[\w\-]*/\d*$",
    ),
]

GOOGLE_SITE_MARKETPLACES = DEFAULT_MARKETPLACES + [
    Marketplace(
        "tutti",
        "tutti.ch",
        "https://www.tutti.ch/fr/li/toute-la-suisse?q=%s",
        r"^https://www\.tutti\.ch/fr/vi/",
    ),
    Marketplace(
        "ricardo",
        "ricardo.ch",
        "https://www.ricardo.ch/de/s/%s",
        r"^https://www\.ricardo\.ch/de/a/",
    ),
    # TODO this was taken from MediCrawl withouth further testing. this should be tested.
]


# ---------------------------------------------------
# Process - Page types used in step 7: page type detection
# ---------------------------------------------------
class PageTypes:
    ECOMMERCE_PRODUCT = "ecommerce_product"
    ECOMMERCE_OTHER = "ecommerce_other"
    WEB_PRODUCT_ARTICLE = "web_product_article"
    WEB_ARTICLE = "web_article"
    BLOGPOST = "blogpost"
    OTHER = "other"


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
# BaseShippingPolicyFilterer
# ---------------------------------------------------


class BaseShippingPolicyFilterer(ABC):
    """Base class for shipping policy filterer."""

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

        # Name
        self.name = name
        
        # Config
        self.config = config

        # Known domains
        self.known_domains = utils_io.load_setting(
            path_settings=config.get("PATH_CURRENT_PROJECT_SETTINGS"),
            country=country,
            file_name="known_domains",
        )
        self.domains_pos = self.known_domains.get("domains_pos", {})
        self.domains_unknwn = self.known_domains.get("domains_unknwn", {})
        self.domains_neg = self.known_domains.get("domains_neg", {})

        # Setting
        self.setting = config_filterer

        # Keywords
        self.keywords_shipping_policy = self.setting.get("keywords_shipping_policy")

        # Url domains already classified
        self.urls_domains_shipping_pos = self.setting.get("urls_domains_shipping_pos")
        self.urls_domains_shipping_unknwn = self.setting.get("urls_domains_shipping_unknwn")
        self.urls_domains_shipping_neg = self.setting.get("urls_domains_shipping_neg")

        # LLM API
        self.llm_api_prompt = self.setting.get("llm_api_prompt")
        self.llm_api_config = self.setting.get("llm_api_config")

        # Product page Zyte API
        self.zyte_api_product_page_config = self.setting.get("zyte_api_product_page_config")

        # Policy page Zyte API
        self.zyte_api_policy_page_config = self.setting.get("zyte_api_policy_page_config")

        # Use concurrency
        self.use_concurrency = self.setting.get("use_concurrency")

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
        Classifies for each url of the dataframe if the delivery is possible to the country of interest.
        Executes the Shipping policy filter only for urls which have not yet been classified by the offline 
        filterers (i.e., known_domains and url): it extracts the product page, then identifies in its footer 
        the shipping policy page then extracts it and sends it to an LLM asking it to say if the site delivers 
        or not or does not know in the country of interest.
        Each new classified domain is added to the domain registry.
        The extraction of the product and shipping policy pages for all urls is done in parallel using the 
        Zyte API in order to go faster.

        Args:
            df (pd.DataFrame): dataFrame to filter.

        Returns:
            pd.DataFrame: filtered dataFrame.
        """

        tqdm.tqdm.pandas(desc=f"Filtering with {self.name}...", leave=False)

        # Create a list to store rows' indexes and labels
        list_pages_labeled = []

        def pseudo_filter_page(row):
            if row.filterer_name == "unknown":
                domain_result, domain_filterer_name = self.recover_domain_result_filterer_name(row)
                if domain_result and domain_filterer_name:
                    row.filterer_name, row.result = domain_filterer_name, domain_result
                    return row.name, row.dropna().to_dict()
                else:
                    row.filterer_name = self.name
                    return row.name, self.filter_page(
                        **row.dropna().to_dict()
                    )
            
            else:
                return row.name, row.dropna().to_dict()
        
        if not self.use_concurrency:
            with tqdm.tqdm(total=len(df)) as pbar:
                for _, row in df.iterrows():
                    # Get page_labeled and add the index
                    row_index, page_labeled = pseudo_filter_page(row)
                    page_labeled["index"] = row_index

                    # Add it to the list
                    list_pages_labeled.append(page_labeled)

                    # If the page has been classified by the shipping policy filterer
                    if page_labeled["filterer_name"] == self.name:
                        # Add domain to known_domains variable
                        try:
                            self.add_domain_to_known_domains(
                                page_labeled=page_labeled
                            )

                        except Exception as e:
                            print(f"Error: {e}")

                    pbar.update(1)

        else:
            # Use ThreadPoolExecutor to call zyte api in parallel for the shipping policy step
            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = [
                    executor.submit(pseudo_filter_page, row) for _, row in df.iterrows()
                ]

                with tqdm.tqdm(total=len(futures)) as pbar:
                    for future in as_completed(futures):
                        # Get page_labeled and add the index
                        row_index, page_labeled = future.result()
                        page_labeled["index"] = row_index

                        # Add it to the list
                        list_pages_labeled.append(page_labeled)

                        # If the page has been classified by the shipping policy filterer
                        if page_labeled["filterer_name"] == self.name:
                            # Add domain to known_domains variable
                            try:
                                self.add_domain_to_known_domains(
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

    def add_domain_to_known_domains(self, page_labeled):
        # Recover label from page_labeled
        label, domain = page_labeled["RESULT"], page_labeled["domain"]

        # Filter page_labeled with relevant keys
        page_labeled_filtered = filter_dict_keys(
            original_dict=page_labeled, keys_to_save=self.config["KEYS_TO_SAVE"]
        )

        # Add domain labeled to dict
        if label == 1:
            self.domains_pos[domain] = page_labeled_filtered

        elif label == 0:
            self.domains_unknwn[domain] = page_labeled_filtered

        elif label == -1:
            self.domains_neg[domain] = page_labeled_filtered

    def save_new_known_domains(self):
        dict_known_domains = {
            "domains_pos": self.domains_pos,
            "domains_unknwn": self.domains_unknwn,
            "domains_neg": self.domains_neg,
        }

        _ = utils_io.save_and_load_setting(
            setting=dict_known_domains,
            path_settings=self.config.get("PATH_CURRENT_PROJECT_SETTINGS"),
            country=self.country,
            file_name="known_domains",
        )

    def recover_domain_result_filterer_name(self, page):
        # Recover domain
        domain = page.get("domain")

        # Recover domain_result and domain_filterer_name
        if domain in self.domains_pos:
            domain_result, domain_filterer_name = self.domains_pos[domain].get("RESULT"), self.domains_pos[domain].get("filterer_name")
            return domain_result, domain_filterer_name
        
        elif domain in self.domains_unknwn:
            domain_result, domain_filterer_name = self.domains_unknwn[domain].get("RESULT"), self.domains_unknwn[domain].get("filterer_name")
            return domain_result, domain_filterer_name
        
        elif domain in self.domains_neg:
            domain_result, domain_filterer_name = self.domains_neg[domain].get("RESULT"), self.domains_neg[domain].get("filterer_name")
            return domain_result, domain_filterer_name

        return None, None
