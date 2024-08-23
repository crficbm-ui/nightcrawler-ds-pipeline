import logging
from typing import List, Dict, Union, Any

from helpers.context import Context
from helpers.utils import write_json
from helpers.api.serp_api import SerpAPI
from helpers import LOGGER_NAME
from nightcrawler.extract.datacollector import DataCollector

logger = logging.getLogger(LOGGER_NAME)

class SerpapiExtractor(DataCollector):
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

    def initiate_client(self) -> SerpAPI:
        """
        Initializes and returns the SerpAPI client.

        Returns:
            SerpAPI: An instance of the SerpAPI client.
        """
        client = SerpAPI()
        return client
    
    def retrieve_response(self, keyword: str, client: SerpAPI, number_of_results: int) -> Dict[str, Any]:
        """
        Makes the API call to SerpAPI to retrieve search results for the given keyword.

        Args:
            keyword (str): The search keyword.
            client (SerpAPI): The SerpAPI client instance.
            number_of_results (int): The number of search results to retrieve.

        Returns:
            Dict[str, Any]: The raw response data from the SerpAPI.
        """
        params = {
            "q": keyword,
            "tbm": "",
            "start": 0,
            "num": int(number_of_results),
            "api_key": self.context.settings.serp_api.token,
        }
        logger.info(f"Extracting URLs from SerpAPI for '{keyword}'")
        response = client.call_serpapi(params, log_name="google_regular")
        return response

    def structure_results(self, response: Dict[str, Any], client: SerpAPI, full_output: bool, keyword: str) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            response (Dict[str, Any]): The raw data returned from the API.
            client (SerpAPI): The SerpAPI client instance.
            full_output (bool): Flag indicating whether to return the full output or just the URLs.
            keyword (str): The search keyword.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The structured search results.
        """
        items = client.get_organic_results(response)

        if full_output:
            results = items
        else:
            urls = [item.get("link") for item in items]
            results = client._check_limit(urls, keyword)

        return results
    
    def store_results(self, structured_results: Union[List[str], List[Dict[str, Any]]]) -> None:
        """
        Stores the structured search results to a JSON file.

        Args:
            structured_results (Union[List[str], List[Dict[str, Any]]]): The structured search results.
        """
        write_json(self.context.output_path, self.context.serpapi_filename, structured_results)

    def apply(self, keyword: str, number_of_results: int, full_output: bool= False) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Orchestrates the entire process of data collection: client initiation, 
        response retrieval, structuring results, and storing results.

        Args:
            keyword (str): The search keyword.
            number_of_results (int): The number of search results to retrieve.
            full_output (bool): Flag indicating whether to return the full output or just the URLs.

        Returns:
            Union[List[str], List[Dict[str, Any]]]: The final structured search results.
        """

        client = self.initiate_client()
        response = self.retrieve_response(keyword, client, number_of_results)
        structured_results = self.structure_results(response, client, full_output, keyword)
        self.store_results(structured_results)
        return structured_results
