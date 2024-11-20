import os
import time
import asyncio
import aiohttp
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
    def __init__(self, context, cache_name="zyte", max_retries=3, retry_delay=10):
        # Cache data for 7 days (minus 6h) for zyte
        super().__init__(
            context, cache_name, max_retries, retry_delay, (7 * 24 - 6) * 60 * 60
        )
        self.endpoint = "https://api.zyte.com/v1/extract"
        self.auth = aiohttp.BasicAuth(os.environ["ZYTE_API_TOKEN"], "")
        self.session = None

    async def close(self):
        """
        Closes the aiohttp session.
        """
        if self.session:
            await self.session.close()

    async def call_api(self, prompt, config, force_refresh=False, callback=None):
        """
        Asynchronously calls the Zyte API with the given URL and configuration.

        Args:
            prompt (str): The URL to fetch data from.
            config (dict): The configuration dictionary for the API call.
            force_refresh (bool, optional): Whether to force a fresh API call, bypassing the cache.
            callback (callable, optional): A callback function to report progress.

        Returns:
            dict: The API response.

        Raises:
            Exception: If all retry attempts fail.
        """
        data_hash = self._generate_hash((prompt, str(config)))

        if not force_refresh and (cached := self._read_cache(data_hash)) is not None:
            logger.warning("Using cached response for zyte (%s)", data_hash)
            return cached

        attempts = 0

        if not self.session:
            self.session = aiohttp.ClientSession()

        while attempts < self.max_retries:
            try:
                start_time = time.time()
                async with self.session.post(
                    self.endpoint,
                    auth=self.auth,
                    json={
                        "url": prompt,
                        **config,
                    },
                    timeout=10,
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        raise Exception(
                            f"API call failed with status code {response.status} and response: {text}"
                        )

                    end_time = time.time()

                    response_json = await response.json()
                    response_json["seconds_taken"] = end_time - start_time

                    self._write_cache(data_hash, response_json)
                    if callback is not None:
                        callback(1)

                    return response_json
            except Exception as e:
                logger.warning(
                    f"API call failed with error: {e}. Retrying in {self.retry_delay} seconds..."
                )
                attempts += 1
                await asyncio.sleep(self.retry_delay)
        raise Exception("All API call attempts to zyte failed.")
