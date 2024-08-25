import logging
from typing import Any, Dict

from helpers.context import Context
from helpers.utils import write_json
from helpers.api.serp_api import SerpAPI
from helpers.decorators import timeit
from helpers import LOGGER_NAME

from nightcrawler.base import (
    ExtractSerpapiData,
    ExtractMetaData,
    ExtractResults,
    Extract,
)

logger = logging.getLogger(LOGGER_NAME)


class SerpapiExtractor(Extract):
    """
    Implements data collection using SerpAPI.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the SerpapiExtractor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        super().__init__(self._entity_name)
        self.context = context

    def initiate_client(self) -> SerpAPI:
        """
        Initializes and returns the SerpAPI client.

        Returns:
            SerpAPI: An instance of the SerpAPI client.
        """
        return SerpAPI()

    def retrieve_response(
        self, keyword: str, client: SerpAPI, number_of_results: int
    ) -> Dict[str, Any]:
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
            "num": number_of_results,
            "api_key": self.context.settings.serp_api.token,
        }
        logger.info(f"Extracting URLs from SerpAPI for '{keyword}'")
        return client.call_serpapi(params, log_name="google_regular")

    def structure_results(
        self,
        response: Dict[str, Any],
        client: SerpAPI,
        metadata: ExtractMetaData,
        keyword: str,
    ) -> ExtractResults:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            response (Dict[str, Any]): The raw data returned from the API.
            client (SerpAPI): The SerpAPI client instance.
            metadata (ExtractMetaData): Metadata about the search.
            keyword (str): The search keyword.

        Returns:
            ExtractResults: The structured search results.
        """
        items = client.get_organic_results(response)
        urls = [item.get("link") for item in items]
        filtered_urls = client._check_limit(urls, keyword)
        results = [ExtractSerpapiData(url=url).to_dict() for url in filtered_urls]
        return ExtractResults(meta=metadata.to_dict(), results=results)

    def store_results(
        self,
        structured_results: ExtractResults,
        output_dir: str,
    ) -> None:
        """
        Stores the structured search results to a JSON file.

        Args:
            structured_results (ExtractResults): The structured search results.
            output_dir (str): Path to the directory where the results will be stored.
        """
        write_json(
            output_dir, self.context.serpapi_filename, structured_results.to_dict()
        )

    def _rename_link_to_url(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Renames the 'link' key to 'url' in the result dictionary.

        Args:
            result (Dict[str, Any]): The result dictionary to modify.

        Returns:
            Dict[str, Any]: The modified result dictionary with 'url' instead of 'link'.
        """
        new_result = result.copy()
        new_result["url"] = new_result.pop("link")
        return new_result

    @timeit
    def apply(
        self,
        keyword: str,
        number_of_results: int,
        output_dir: str,
    ) -> ExtractResults:
        """
        Orchestrates the entire process of data collection: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            keyword (str): The search keyword.
            number_of_results (int): The number of search results to retrieve.
            output_dir (str): Path to the directory where the results will be stored.

        Returns:
            ExtractResults: The final structured search results.
        """
        client = self.initiate_client()
        metadata = ExtractMetaData(keyword=keyword, numberOfResults=number_of_results)
        response = self.retrieve_response(keyword, client, number_of_results)
        structured_results = self.structure_results(response, client, metadata, keyword)
        self.store_results(structured_results, output_dir)
        return structured_results
