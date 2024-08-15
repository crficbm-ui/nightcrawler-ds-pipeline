import logging
import pandas as pd
from abc import ABC
from typing import List, Dict
from nightcrawler.context import Context
from nightcrawler.utils import write_json
from nightcrawler.contex import Context
from helpers.utils import write_json
from helpers.decorators import retry_on_requests_exception, log_start_and_end
from helpers.serp_api import SerpAPI
from helpers.zyte_api import ZyteAPI, DEFAULT_CONFIG

from tqdm.auto import tqdm

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

class DataCollector(ABC):
    """
    Implements the data collection via Zyte and SerpAPI.

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
        logger.info(f"Initializing data collection : {self._entity_name}")
        self.context = context

    def full_pipeline(self, keyword:str, num_of_results: int):
        """
        Performs both SerpAPI and Zyte extraction processes.

        Args:
            keyword (str): The keyword to search for.
        """
        urls = self.extract_serpapi(keyword=keyword, full_output=False, num_of_results= num_of_results)
        results = self.extract_zyte(urls=urls)
        return results

    def extract_serpapi(self, keyword: str, full_output: bool, num_of_results: int) -> List[str]:
        logger.info(f"Extracting URLs from serpapi for '{keyword}'")

        serpapi_client = SerpAPI()

        params = {
            'q': keyword,
            'tbm': '',
            'start': 0,
            'num': int(num_of_results) + 1,
            'api_key': self.context.settings.serpapi.token
        }

        response = serpapi_client.call_serpapi(params, log_name='google_regular')
        items = serpapi_client.get_organic_results(response)

        if full_output:
            results =  items
        else:
            urls = [item.get('link') for item in items]
            results = serpapi_client._check_limit(urls, keyword)

        write_json(self.context.output_path, self.context.serpapi_filename, results)
        return urls


    def extract_zyte(self, urls: List[str]) -> List[Dict[str, str]]:
        """
        Retrieves data in bulk from Zyte using the provided URL path.

        Args:
            urls (List[str]): A list of URLs to fetch data from.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing the URL and a placeholder title.
        """
        logger.info(f"Extracting product details from zyte for {len(urls)} URLs.")

        api = ZyteAPI()
        api_config = DEFAULT_CONFIG.copy()
        api_config["screenshot"] = False
        api_config["actions"] = []
        api_config["screenshotOptions"] = None
        api_config["viewport"] = None
        api_config["browserHtml"] = False
        api_config["javascript"] = False

        results = []
        with tqdm(total=len(urls)) as pbar:
            for url in urls:
                response = api.call_api(url, api_config)
                if not response:
                    continue

                product = response["product"]
                results.append(
                    {
                        "price": product.get("price", "") + product.get("currencyRaw", ""),
                        "title": product.get("name", ""),
                        "full_description": product.get("description", ""),
                        "seconds_taken": response["seconds_taken"],
                    }
                )
                pbar.update(1)

        write_json(self.context.output_path, self.context.zyte_filename, results)
        return results