import time
import requests
import logging

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller


logger = logging.getLogger(LOGGER_NAME)


PROXY_COUNTRY_MAPPING_ISO_3166_1_ALPHA_2 = {
    "Switzerland": "CH",

    "Austria": "AT",

    "Chile": "CL",
}

class ProxyAPI(APICaller):
    def __init__(self, context, cache_name="proxy", max_retries=1, retry_delay=10, requests_timeout=10):
        # Cache data for 7 days (minus 6h) for zyte
        super().__init__(
            context, cache_name, max_retries, retry_delay, (7 * 24 - 6) * 60 * 60
        )

        self.requests_timeout = requests_timeout

    def _build_proxy_url(self, country: str) -> str:
        if not country:
            raise ValueError(f"Country '{country}' is not supported for proxy.")

        return "http://{username}:{password}@{country}.smartproxy.com:{port}".format(
            username=self.context.settings.proxy.username,
            password=self.context.settings.proxy.password,
            country=country,
            port=self.context.settings.proxy.port,
        )

    def _resolve_redirect(self, url: str, proxy_url: str) -> str:
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        response = requests.head(
            url=url,
            allow_redirects=True,
            proxies=proxies,
            timeout=self.requests_timeout
        )

        if response.status_code != 200:
            raise ValueError(f"Status code ({response.status_code}) is not 200.")

        return response.url

    def call_proxy(self, url: str, country: str, force_refresh: bool = False) -> dict:
        """
        Resolve the redirect of a given URL using a proxy for a specific country.

        Args:
            url (str): The URL to resolve.
            country (str): The country to use for the proxy.
                Format: ISO 3166-1 alpha-2 (e.g., "CH", "AT", "CL").
            force_refresh (bool, optional): Whether to force a refresh of the data.
                Defaults to False.

        Returns:
            dict: A dictionary containing the resolved URL and the time taken to resolve it.
                Example: {"resolved_url": "https://www.example.com", "seconds_taken": 0.123}

        Raises:
            ValueError: If the status code is not 200.
        """
        result_hash = self._generate_hash((url, country))

        if not force_refresh and (cached := self._read_cache(result_hash)) is not None:
            logger.warning("Using cached response for proxy (%s)", result_hash)
            return cached

        start_time = time.time()

        proxy_url = self._build_proxy_url(country)
        resolved_url = self._resolve_redirect(url, proxy_url)

        result = {
            "resolved_url": resolved_url,
            "seconds_taken": time.time() - start_time,
        }

        self._write_cache(result_hash, result)

        return result