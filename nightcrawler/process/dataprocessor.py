import logging

from typing import List, Dict
from helpers.context import Context
from ..process.filter_swiss_result import filter_per_country_results
from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class DataProcessor:
    """
    Implements the data processing

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name = __qualname__  # type: ignore

    def __init__(self, context: Context) -> None:
        """
        Initializes the DataProcessor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        self.context = Context()

    def step_country_filtering(
        self, country: str = "", urlpath: str = ""
    ) -> List[Dict[str, str]]:
        """
        Filters results based on the specified country and returns the filtered data. This method filters results
        according to the provided country and URL path. If no URL path is provided, it defaults to test data from the
        repository.

        Args:
            country (str, optional): The country code used to filter results. Defaults to an empty string.
            urlpath (str, optional): The path to the JSON file containing the raw data to be processed. If not provided,
            the method uses `self.zyte_filename`. Defaults to an empty string.

        Returns:
            country_filtered_results (List[Dict[str, str]]): A list of dictionaries representing the filtered results
            for the specified country.
        """

        if not urlpath:
            # TODO: url what happens in the exception that no path is provided?
            # TODO: Make sure with unit tests that you cannot get this far. You should not be able to.
            urlpath = self.zyte_filename

        country_filtered_results = filter_per_country_results(
            self.context, country, urlpath
        )
        if len(country_filtered_results) == 0:
            logger.warning(
                "After filtering per country variable, no results move further in the pipeline"
            )

        return country_filtered_results

    def apply(self, urls: List[Dict[str, str]]):
        # TODO: Depending in the arguments, we run a full pipeline
        """
        Performs data processing.

        Args:
            keyword (str): The keyword to search for.
        """
        pass
