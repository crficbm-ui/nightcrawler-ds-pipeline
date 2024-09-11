import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class DataCollector(ABC):
    """
    Abstract base class that enforces methods to interact with an API, process its results,
    and store the data. Subclasses should implement the following methods:

        - initiate_client: Initiate the API client.
        - retrieve_response: Make the API calls and retrieve data.
        - structure_results: Postprocess the API results into the desired format.
        - store_results: Write the results to the file system.
        - apply: Run the entire data collection process.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    @abstractmethod
    def store_results(
        self, structured_data: Union[List[str], List[Dict[str, Any]]]
    ) -> None:
        """
        Stores the structured data into the file system or another storage mechanism.

        Args:
            structured_data (Union[List[str], List[Dict[str, Any]]]): The processed data that needs to be stored, either as a list of URLs or a list of dictionaries.
        """
        pass

    @abstractmethod
    def apply(
        self, *args: Any, **kwargs: Any
    ) -> Union[List[str], List[Dict[str, Any]]]:
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
            Union[List[str], List[Dict[str, Any]]]: The final structured results.
        """
        pass
