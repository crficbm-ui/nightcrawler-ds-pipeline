import logging
import re
from re import Pattern
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, Iterator, List, Union
from collections.abc import Mapping
from datetime import datetime
from abc import ABC, abstractmethod


from helpers.utils import _get_uuid
from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


# ---------------------------------------------------
# Data Model - Helper Classes used to either enforce specific class initialization or to provide common functionalities to its child classes.
# ---------------------------------------------------
@dataclass
class Iterable(ABC, Mapping):
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
            if v is not None and (not isinstance(v, list) or v)
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

        Args:
            key (str): The attribute name to access.

        Returns:
            Any: The value of the attribute corresponding to the key.

        Raises:
            KeyError: If the attribute does not exist.
        """
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(f"{key} not found in {self.__class__.__name__}")

    def __iter__(self) -> Iterator[str]:
        """
        Allows iteration over the attribute names of the instance.

        Yields:
            str: The attribute names.
        """
        return iter(self.to_dict())

    def __len__(self) -> int:
        """
        Returns the number of non-None attributes.

        Returns:
            int: The number of non-None attributes.
        """
        return len(self.to_dict())

    def keys(self):
        """
        Returns the keys of the dictionary representation.

        Returns:
            KeysView[str]: The keys of the dictionary representation.
        """
        return self.to_dict().keys()


# ---------------------------------------------------
# Data Model - Stage Classes
# ---------------------------------------------------
@dataclass
class MetaData(Iterable):
    """Metadata class for storing information about the extraction process."""

    keyword: str = field(default_factory=str)
    numberOfResults: int = field(default_factory=int)
    numberOfResultsAfterStage: int = field(default_factory=int)
    resultDate: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    )
    uuid: str = field(init=False)

    def __post_init__(self):
        self.uuid = _get_uuid(self.keyword, self.resultDate)


@dataclass
class ExtractSerpapiData(Iterable):
    """Class for handling SERP API results with optional attributes."""

    url: str
    offerRoot: str


@dataclass
class ExtractZyteData(ExtractSerpapiData):
    """Class for handling data extracted from Zyte."""

    price: Optional[str]
    title: Optional[str]
    fullDescription: Optional[str]
    seconds_taken: Optional[float]


@dataclass
class ProcessData(ExtractZyteData):
    """Class for handling data extracted from Zyte."""

    ch_de_in_url: Optional[bool]
    swisscompany_in_url: Optional[bool]
    web_extension_in_url: Optional[bool]
    francs_in_url: Optional[bool]
    result_sold_CH: Optional[bool]


# ---------------------------------------------------
# Data Model - Report Classes
# ---------------------------------------------------


@dataclass
class PipelineResult(Iterable):
    """Class for storing a comprehensive report, including Zyte data."""

    meta: MetaData
    results: List[ProcessData]


# ---------------------------------------------------
# Data Model - Stage Enforcing Classes
# ---------------------------------------------------


class Extract(ABC):
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

    def __init__(self, *args: Any) -> None:
        logger.info(f"Initializing step: {args[0]}")

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

    @abstractmethod
    def store_results(
        self, structured_data: Union[List[PipelineResult], List[PipelineResult]]
    ) -> None:
        """
        Stores the structured data into the file system or another storage mechanism.

        Args:
            structured_data (Union[List[PipelineResult], List[PipelineResult]]): The processed data that needs to be stored.
        """
        pass

    @abstractmethod
    def apply(
        self, *args: Any, **kwargs: Any
    ) -> Union[List[PipelineResult], List[PipelineResult]]:
        """
        Orchestrates the entire process by calling the other methods in sequence:
        - initiate_client
        - retrieve_response
        - structure_results
        - store_results

        Args:
            *args (Any): Positional arguments required for the process.
            **kwargs (Any): Keyword arguments required for the process.

        Returns:
            Union[List[PipelineResult], List[PipelineResult]]: The final structured results.
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
