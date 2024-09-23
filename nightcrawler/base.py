import logging
import re
from re import Pattern
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, Iterator, List, Union
from collections.abc import Mapping
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from helpers.utils import _get_uuid, write_json
from helpers.context import Context
from helpers import LOGGER_NAME

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

    def __post_init__(self):
        self.uuid = _get_uuid(self.keyword, self.resultDate)


@dataclass
class ExtractSerpapiData(ObjectUtilitiesContainer):
    """Data class for step 1: Extract URLs using Serpapi"""

    offerRoot: str
    url: str
    keywordEnriched: Optional[str] = None
    keywordVolume: Optional[float] = -1
    keywordLanguage: Optional[str] = None
    keywordLocation: Optional[str] = None
    imageUrl: Optional[str] = (
        None  # this is only used for the reverse image search and indicates the direct url to the image
    )


@dataclass
class ExtractZyteData(ExtractSerpapiData):
    """Data class for step 2: Use Zyte to process the URLs further"""

    price: Optional[str] = None
    title: Optional[str] = None
    fullDescription: Optional[str] = None
    zyteExecuctionTime: Optional[float] = 0.0


@dataclass
class ProcessData(ExtractZyteData):
    """Data class for step 3: Process the results using DataProcessor based on the country,
    i.e. url-filtering."""

    """
    TODO make this more abstract (not for CH but for any country) i.e.:
    country: Optional[str]
    countryInUrl: Optional[bool]
    webextensionInUrl: Optional[bool]
    currencyInUrl: Optional[bool]
    soltToCountry: Optional[bool]

    -> change the processor accordingly
    
    """
    ch_de_in_url: Optional[bool] = False
    swisscompany_in_url: Optional[bool] = False
    web_extension_in_url: Optional[bool] = False
    francs_in_url: Optional[bool] = False
    result_sold_CH: Optional[bool] = False


@dataclass
class DeliveryPolicyData(ProcessData):
    """Data class for step 4: delivery policy filtering"""

    pass


@dataclass
class PageTyteData(DeliveryPolicyData):
    """Data class for step 5: page type filtering"""

    pass


@dataclass
class BlockedContentData(PageTyteData):
    """Data class for step 6: blocked / corrupted content detection"""

    pass


@dataclass
class ContentDomainData(BlockedContentData):
    """Data class for step 7: content domain filtering"""

    pass


@dataclass
class ProcessSuspiciousnessData(ContentDomainData):
    """Data class for step 8: suspiciousness classifier
    TODO: maybe this class can be deleted and the Suspiciousness step could return directly CrawlResultData as most likely no new variables will come used after this step"""

    pass


@dataclass
class CrawlResultData(ProcessSuspiciousnessData):
    """Data class for step 9: ranking"""

    pass


# ---------------------------------------------------
# Data Model - Class used to store the data classes as json objects
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
    Class that provides core functionalities for all steps in the pipeline:
        - enforces an apply function
        - provides a method to store the results

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
        if not self.context.settings.store_intermediate:
            return

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
