import os
import time
import requests
import logging

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller

logger = logging.getLogger(LOGGER_NAME)

DEFAULT_CONFIG = {"url": "", "token": os.environ["DIFFBOT_API_TOKEN"]}


class DiffbotAPI(APICaller):
    def __init__(self, cache_name="diffbot", max_retries=3, retry_delay=2):
        super().__init__(cache_name, max_retries, retry_delay)
        self.endpoint = "https://api.diffbot.com/v3/product"
        self.headers = {"accept": "application/json"}

    def call_api(self, url, config=DEFAULT_CONFIG, force_refresh=False):
        data_hash = self._generate_hash((url, str(config)))

        if not force_refresh and self._is_cached(data_hash):
            logger.warning("Using cached response")
            return self._read_cache(data_hash)
        else:
            attempts = 0
            while attempts < self.max_retries:
                try:
                    start_time = time.time()
                    params = {"url": url, "token": config["token"]}
                    raw_response = requests.get(
                        self.endpoint, headers=self.headers, params=params, timeout=10
                    )

                    if raw_response.status_code != 200:
                        raise Exception(
                            f"API call failed with status code {raw_response.status_code} and response: {raw_response.text}"
                        )

                    end_time = time.time()

                    response = raw_response.json()
                    response["seconds_taken"] = end_time - start_time

                    self._write_cache(data_hash, response)

                    return response
                except Exception as e:
                    logger.warning(
                        f"API call failed with error: {e}. Retrying in {self.retry_delay} seconds..."
                    )
                    attempts += 1
                    time.sleep(self.retry_delay)
            raise Exception("All API call attempts failed.")
