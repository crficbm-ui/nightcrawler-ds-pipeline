import logging
from typing import List, Dict, Union, Any

from helpers.context import Context
from helpers.utils import write_json
from helpers import LOGGER_NAME
from nightcrawler.extract.datacollector import DataCollector

from nightcrawler.logic.s01_serp import SerpLogic

logger = logging.getLogger(LOGGER_NAME)


class SerpapiExtractor(DataCollector, SerpLogic):
    """
    Implements data collection using Zyte and SerpAPI.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the SerpApi data collector with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logger.info(f"Initializing data collection: {self._entity_name}")
        self.context = context

        SerpLogic.__init__(self, api_token=self.context.settings.serp_api.token)

    def store_results(
        self,
        structured_results: Union[List[str], List[Dict[str, Any]]],
        output_dir: str,
    ) -> None:
        """
        Stores the structured search results to a JSON file.

        Args:
            structured_results (Union[List[str], List[Dict[str, Any]]]): The structured search results.
        """
        write_json(output_dir, self.context.serpapi_filename, structured_results)

    def apply(
        self,
        keyword: str,
        number_of_results: int,
        output_dir: str,
        full_output: bool = False,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Orchestrates the entire process of data collection: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            keyword (str): The search keyword.
            number_of_results (int): The number of search results to retrieve.
            output_dir: Path to directory where the results will be stored.
            full_output (bool): Flag indicating whether to return the full output or just the URLs.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The final structured search results.
        """
        structured_results = self.apply_one(keyword, number_of_results, full_output)
        self.store_results(structured_results, output_dir)
        return structured_results
