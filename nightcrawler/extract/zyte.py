import logging
from typing import List, Dict, Any

from helpers.context import Context
from helpers.utils import write_json
from helpers import LOGGER_NAME

from nightcrawler.extract.datacollector import DataCollector

from nightcrawler.logic.s01_zyte import ZyteLogic

logger = logging.getLogger(LOGGER_NAME)


class ZyteExtractor(DataCollector):
    """
    Implements the data collection via Zyte.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context, *args, **kwargs) -> None:
        """
        Initializes the ZyteAPI data collector with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logger.info(f"Initializing data collection: {self._entity_name}")
        self.context = context

        self.logic = self._init_logic(context)

    def _init_logic(self, *args, **kwargs) -> ZyteLogic:
        return ZyteLogic(*args, **kwargs)

    def store_results(
        self, structured_results: List[Dict[str, str]], output_dir: str
    ) -> None:
        """
        Stores the structured results into a JSON file.

        Args:
            structured_results (List[Dict[str, str]]): The structured data to be stored.
        """
        write_json(output_dir, self.context.zyte_filename, structured_results)

    def apply(self, urls: List[str], output_dir: str) -> List[Dict[str, Any]]:
        """
        Orchestrates the entire data collection process: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            urls (List[str]): The list of URLs to retrieve data from.

        Returns:
            List[Dict[str, str]]: The final structured results.
        """

        logic_input = [{"url": url} for url in urls]
        structured_results = self.logic.apply_batch(logic_input)

        self.store_results(structured_results, output_dir)
        return structured_results
