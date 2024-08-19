from nightcrawler.extract.datacollector import DataCollector

import logging
from typing import List, Dict, Tuple, Any
from tqdm.auto import tqdm

from helpers.context import Context
from helpers.utils import write_json
from helpers.api.zyte_api import ZyteAPI, DEFAULT_CONFIG
from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class ZyteExtractor(DataCollector):
    """
    Implements the data collection via Zyte.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the ZyteAPI data collector with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logger.info(f"Initializing data collection: {self._entity_name}")
        self.context = context

    def initiate_client(self) -> Tuple[ZyteAPI, Dict[str, Any]]:
        """
        Initializes and returns the ZyteAPI client and its configuration.

        Returns:
            Tuple[ZyteAPI, Dict[str, Any]]: The ZyteAPI client instance and its configuration.
        """
        client = ZyteAPI()
        return client, DEFAULT_CONFIG

    def retrieve_response(
        self, client: ZyteAPI, urls: List[str], api_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Makes the API calls to ZyteAPI to retrieve data from the provided URLs.

        Args:
            client (ZyteAPI): The ZyteAPI client instance.
            urls (List[str]): The list of URLs to retrieve data from.

        Returns:
            List[Dict[str, Any]]: The list of responses from ZyteAPI.
        """
        responses = []
        with tqdm(total=len(urls)) as pbar:
            for url in urls:
                response = client.call_api(url, api_config)
                if not response:
                    logger.error(f"Failed to collect product from {url}")
                    continue
                responses.append(response)
                pbar.update(1)
        return responses

    def structure_results(
        self, responses: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """
        Processes and structures the raw API responses into the desired format.

        Args:
            responses (List[Dict[str, Any]]): The raw data returned from the API.

        Returns:
            List[Dict[str, str]]: The structured results containing relevant product information.
        """
        results = []
        for response in responses:
            product = response.get("product", {})
            results.append(
                {
                    "price": product.get("price", "") + product.get("currencyRaw", ""),
                    "title": product.get("name", ""),
                    "full_description": product.get("description", ""),
                    "seconds_taken": str(response.get("seconds_taken", 0)),
                }
            )
        return results

    def store_results(
        self, structured_results: List[Dict[str, str]], output_dir: str
    ) -> None:
        """
        Stores the structured results into a JSON file.

        Args:
            structured_results (List[Dict[str, str]]): The structured data to be stored.
        """
        write_json(output_dir, self.context.zyte_filename, structured_results)

    def apply(self, urls: List[str], output_dir: str) -> List[Dict[str, str]]:
        """
        Orchestrates the entire data collection process: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            urls (List[str]): The list of URLs to retrieve data from.

        Returns:
            List[Dict[str, str]]: The final structured results.
        """
        client, api_config = self.initiate_client()
        responses = self.retrieve_response(client, urls, api_config)
        structured_results = self.structure_results(responses)
        self.store_results(structured_results, output_dir)
        return structured_results
