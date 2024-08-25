import logging
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
class ExtractMetaData(Iterable):
    """Metadata class for storing information about the extraction process."""

    keyword: str = field(default_factory=str)
    numberOfResults: int = field(default_factory=int)
    fullOutput: bool = field(default_factory=bool)
    resultDate: str = field(
        default_factory=lambda: datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    )
    uuid: str = field(init=False)

    def __post_init__(self):
        self.uuid = _get_uuid(self.keyword, self.resultDate)


@dataclass
class ExtractSerpapiData(Iterable):
    """Class for handling SERP API results with optional attributes."""

    url: Optional[str] = None


@dataclass
class ExtractZyteData(ExtractSerpapiData):
    """Class for handling data extracted from Zyte."""

    price: str = field(default_factory=str)
    title: str = field(default_factory=str)
    fullDescription: str = field(default_factory=str)
    seconds_taken: Optional[float] = None


# ---------------------------------------------------
# Data Model - Report Classes
# ---------------------------------------------------


@dataclass
class ExtractResults(Iterable):
    """Class for storing a comprehensive report, including Zyte data."""

    meta: ExtractMetaData
    results: Optional[List[ExtractSerpapiData]] = None
    zyte: Optional[List[ExtractZyteData]] = None


# TODO: Add data model for processor

# ---------------------------------------------------
# Data Model - Stage Enforcing Classes
# ---------------------------------------------------


@dataclass
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
    ) -> Union[List[ExtractResults], List[ExtractResults]]:
        """
        Makes the API calls to retrieve data.

        Args:
            *args (Any): Positional arguments required by the API call.
            **kwargs (Any): Keyword arguments required by the API call.

        Returns:
            Union[List[ExtractResults], List[ExtractResults]]: The raw response data from the API, either as a list of `ExtractResults` or `ExtractResults` instances.
        """
        pass

    @abstractmethod
    def structure_results(
        self,
        data: Union[List[ExtractResults], List[ExtractResults]],
        *args: Any,
        **kwargs: Any,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            data (Union[List[ExtractResults], List[ExtractResults]]): The raw data returned from the API.
            *args (Any): Additional positional arguments for structuring the data.
            **kwargs (Any): Additional keyword arguments for structuring the data.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The structured and processed data, either as a list of URLs (strings) or a list of dictionaries.
        """
        pass

    @abstractmethod
    def store_results(
        self, structured_data: Union[List[ExtractResults], List[ExtractResults]]
    ) -> None:
        """
        Stores the structured data into the file system or another storage mechanism.

        Args:
            structured_data (Union[List[ExtractResults], List[ExtractResults]]): The processed data that needs to be stored.
        """
        pass

    @abstractmethod
    def apply(
        self, *args: Any, **kwargs: Any
    ) -> Union[List[ExtractResults], List[ExtractResults]]:
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
            Union[List[ExtractResults], List[ExtractResults]]: The final structured results.
        """
        pass
