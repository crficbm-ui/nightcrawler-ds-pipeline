import logging
from typing import List, Dict
from helpers.context import Context
from helpers.decorators import timeit
from helpers import LOGGER_NAME

from nightcrawler.process.filter_swiss_result import filter_per_country_results

logger = logging.getLogger(LOGGER_NAME)


class DataProcessor:
    """
    Implements the data processing.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the DataProcessor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logger.info(f"Initializing step: {self._entity_name}")
        self.context = context

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
                                     the method uses `self.context.zyte_filename`. Defaults to an empty string.

        Returns:
            List[Dict[str, str]]: A list of dictionaries representing the filtered results for the specified country.
        """
        if not urlpath:
            # TODO: Handle the case where no path is provided. Ensure that this scenario is covered by unit tests.
            urlpath = self.context.zyte_filename

        country_filtered_results = filter_per_country_results(
            self.context, country, urlpath
        )

        if not country_filtered_results.get("results"):
            logger.warning(
                "After filtering per country variable, no results move further in the pipeline."
            )

        return country_filtered_results

    @timeit
    def apply(self, urls: List[Dict[str, str]] = None) -> None:
        """
        Placeholder for the full data processing pipeline.

        Args:
            urls (List[Dict[str, str]], optional): A list of URLs to process. Defaults to an empty list.

        Raises:
            NotImplementedError: Raised as the full pipeline is not yet implemented.
        """
        # TODO: Implement the full processing pipeline depending on the arguments.
        logger.error(
            "Full processor pipeline not yet implemented. For the time being, use the '--country=CH' argument."
        )
        pass
