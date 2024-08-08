import logging
import pandas as pd
from abc import ABC
from typing import List, Dict
from nightcrawler.contex import Context
from nightcrawler.utils import write_json


class DataCollector(ABC):
    """
    Implements the data collection via DiffBot and SerpAPI.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the DataCollector with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logging.info(f"Initializing data collection : {self._entity_name}")
        self.context = context

    def get_diffbot_bulk(self, urlpath: str = "") -> List[Dict[str, str]]:
        """
        Retrieves data in bulk from DiffBot using the provided URL path.

        Args:
            urlpath (str): The path to the file containing URLs. If not provided, 
                           it defaults to `self.context.diffbot_output_path`.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing the URL and a placeholder title.
        """
        # TODO: Implement DiffBot bulk calls

        if not urlpath:
            urlpath = self.context.diffbot_output_path

        with open(urlpath, "r") as file:
            urls = file.read().splitlines()  # Assuming each URL is on a new line
        results = [{"url": url, "title": "xxx"} for url in urls]
        return results

    def get_urls_from_serpapi(self, keywords: List[str]) -> List[str]:
        """
        Retrieves URLs from SerpAPI based on the provided keywords.

        Args:
            keywords (List[str]): A list of keywords to search for.

        Returns:
            List[str]: A list of URLs corresponding to the search keywords.
        """
        # TODO: Implement SerpAPI calls
        results = [f"www.{keyword}.ch" for keyword in keywords]  # toy example
        return results
