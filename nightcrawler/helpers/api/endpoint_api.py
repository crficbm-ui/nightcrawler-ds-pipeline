import time
import requests
from requests.auth import HTTPBasicAuth
import logging

from nightcrawler.context import Context
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller


logger = logging.getLogger(LOGGER_NAME)


class EndpointAPI(APICaller):
    def __init__(
        self,
        context: Context,
        endpoint_url: str,
        endpoint_auth_creds: tuple = None,
        endpoint_timeout: int = 10,
        cache_name: str = "endpoint",
        max_retries: int = 1,
        retry_delay: int = 10,
        cache_duration: int = 24 * 60 * 60,
    ):
        """
        Initializes the EndpointAPI class.

        Args:
            context (Context): The context object.
            endpoint (str): The endpoint to call.
            auth (tuple): The authentication tuple (username, password) (default is None).
            cache_name (str): The name of the cache (default is "endpoint").
            max_retries (int): The maximum number of retries for API calls (default is 1).
            retry_delay (int): The delay in seconds between retry attempts (default is 10).
            endpoint_timeout (int): The timeout in seconds for the API call (default is 10).
            cache_duration (int): The delay in seconds between a cache entry is considered

        Returns:
            None
        """
        super().__init__(
            context=context,
            cache_name=cache_name,
            max_retries=max_retries,
            retry_delay=retry_delay,
            cache_duration=cache_duration,
        )
        self.endpoint_url = endpoint_url
        self.endpoint_auth = (
            HTTPBasicAuth(*endpoint_auth_creds) if endpoint_auth_creds else None
        )
        self.endpoint_timeout = endpoint_timeout

    def call_api(self, playload: dict, force_refresh: bool = False) -> dict:
        """
        Call a endpoint with the given payload.

        Args:
            playload (dict):
                The payload to send. It depends on the endpoint.
            force_refresh (bool):
                Whether to bypass the cache and force a new API call (default is False).

        Returns:
            dict:
                The JSON response from the API.

        Raises:
            ValueError:
                If the status code is not 200.
        """
        result_hash = self._generate_hash(
            (
                self.endpoint_url,
                playload,
            )
        )

        if not force_refresh and (cached := self._read_cache(result_hash)) is not None:
            logger.warning("Using cached response for proxy (%s)", result_hash)
            return cached

        start_time = time.time()

        response = requests.post(
            url=self.endpoint_url,
            json=playload,
            auth=self.endpoint_auth,
            timeout=self.endpoint_timeout,
        )

        if response.status_code != 200:
            raise ValueError(
                f"Request failed with status code {response.status_code} and response: {response.text}"
            )

        result = {
            "response": response.json(),
            "seconds_taken": time.time() - start_time,
        }

        self._write_cache(result_hash, result)

        return result
