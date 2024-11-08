import logging
import time
from typing import List, Dict, Any, Callable
from serpapi.google_search import GoogleSearch

from nightcrawler.helpers.api.requests_wrapper import (
    convert_request_to_string,
    convert_response_to_string,
)
from nightcrawler.helpers.api.api_caller import APICaller

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.context import Context

logger = logging.getLogger(LOGGER_NAME)


class SerpAPI(APICaller):
    """
    A class to handle API calls to SerpAPI with caching, retry mechanisms, and logging.

    Attributes:
        cache_name (str): The name of the cache.
        max_retries (int): The maximum number of retries for API calls.
        retry_delay (int): The delay in seconds between retry attempts.
    """

    def __init__(
        self, context: Context, cache_name: str = "serpapi", max_retries: int = 3, retry_delay: int = 2
    ):
        """
        Initializes the SerpAPI class.

        Args:
            context (Context): Context object
            cache_name (str): The name of the cache (default is "serpapi").
            max_retries (int): The maximum number of retries for API calls (default is 3).
            retry_delay (int): The delay in seconds between retry attempts (default is 2).
        """
        super().__init__(context, cache_name, max_retries, retry_delay, 18 * 60 * 60)

    def call_serpapi(
        self, params: Dict[str, Any], log_name: str, force_refresh: bool = False, callback: Callable[int, None] | None = None
    ) -> Dict[str, Any]:
        """
        Calls the SerpAPI and returns the response, with optional caching.

        Args:
            params (Dict[str, Any]): Parameters for the API call.
            log_name (str): The name used for logging.
            force_refresh (bool): Whether to bypass the cache and force a new API call (default is False).

        Returns:
            Dict[str, Any]: The JSON response from the SerpAPI.

        Raises:
            Exception: If all API call attempts fail.
        """
        data_hash = self._generate_hash(str(params))

        if not force_refresh and (cached := self._read_cache(data_hash)) is not None:
            logger.warning("Using cached response for serpapi (%s)", data_hash)
            return cached

        attempts = 0
        while attempts < self.max_retries:
            try:
                search = GoogleSearch(params)
                response = search.get_response()
                logger.debug(
                    f'{log_name}: req: {convert_request_to_string(response.request, params.get("api_key"))}'
                )
                logger.debug(
                    f"{log_name}: response: \n"
                    + convert_response_to_string(response, params.get("api_key"))
                )
                response.raise_for_status()
                self._write_cache(data_hash, response.json())
                if callback is not None:
                    callback(1)
                return response.json()
            except Exception as e:
                logger.warning(
                    f"API call failed with error: {e}. Retrying in {self.retry_delay} seconds..."
                )
                attempts += 1
                time.sleep(self.retry_delay)
        raise Exception("All API call attempts to SerpAPI failed.")

    @staticmethod
    def _check_limit(urls: List[str], query: str, limit: int = 200) -> List[str]:
        """
        Checks if the number of URLs exceeds the limit, and trims the list if necessary.

        Args:
            urls (List[str]): The list of URLs.
            query (str): The search query.
            limit (int): hight of limit

        Returns:
            List[str]: The potentially trimmed list of URLs.
        """
        if len(urls) > limit:
            urls = urls[:limit]
            logger.warning(f"Reached limit for keyword: {query}")
        return urls

    @staticmethod
    def get_organic_results(results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extracts the organic search results from the API response.

        Args:
            results (Dict[str, Any]): The JSON response from the API.

        Returns:
            List[Dict[str, Any]]: A list of organic search results.
        """
        return results.get("organic_results") or []

    @staticmethod
    def get_shopping_results(results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extracts the shopping results from the API response.

        Args:
            results (Dict[str, Any]): The JSON response from the API.

        Returns:
            List[Dict[str, Any]]: A list of shopping results.
        """
        inline_results = results.get("inline_shopping_results") or []
        results = results.get("shopping_results") or []
        return inline_results + results
