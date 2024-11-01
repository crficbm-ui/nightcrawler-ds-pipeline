import os
import time
import requests
import logging

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller


logger = logging.getLogger(LOGGER_NAME)


DEFAULT_CONFIG = {
    "javascript": False,
    "browserHtml": False,
    "screenshot": False,
    "product": True,
    "productOptions": {"extractFrom": "httpResponseBody"},
    "httpResponseBody": True,
    "geolocation": "CH",
    "viewport": {"width": 1280, "height": 1080},
    "screenshotOptions": None,
    "actions": [],
}


class ZyteAPI(APICaller):
    def __init__(self, cache_name="zyte", max_retries=3, retry_delay=2):
        super().__init__(cache_name, max_retries, retry_delay)
        self.endpoint = "https://api.zyte.com/v1/extract"
        self.auth = (os.environ["ZYTE_API_TOKEN"], "")

    def call_api(self, prompt, config, force_refresh=False):
        data_hash = self._generate_hash((prompt, str(config)))

        if not force_refresh and self._is_cached(data_hash):
            logger.info("Using cached response for zyte")
            return self._read_cache(data_hash)
        else:
            attempts = 0
            while attempts < self.max_retries:
                try:
                    start_time = time.time()
                    raw_response = requests.post(
                        self.endpoint,
                        auth=self.auth,
                        json={
                            "url": prompt,
                            **config,
                        },
                        timeout=10,
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
