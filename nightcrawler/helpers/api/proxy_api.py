import time
import requests
import logging

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller


logger = logging.getLogger(LOGGER_NAME)


PROXY_COUNTRY_MAPPING = {
    "CH": "ch",
    "Switzerland": "ch",

    "AT": "at",
    "Austria": "at",

    "CL": "cl",
    "Chile": "cl",
}

class ProxyAPI(APICaller):
    def __init__(self, context, cache_name="proxy", max_retries=1, retry_delay=10, requests_timeout=10):
        # Cache data for 7 days (minus 6h) for zyte
        super().__init__(
            context, cache_name, max_retries, retry_delay, (7 * 24 - 6) * 60 * 60
        )

        self.requests_timeout = requests_timeout

    def _build_proxy_url(self, country: str) -> str:
        proxy_country = PROXY_COUNTRY_MAPPING.get(country, None)

        if not proxy_country:
            raise ValueError(f"Country '{country}' is not supported for proxy.")

        return "http://{username}:{password}@{country}.smartproxy.com:{port}".format(
            username=self.context.settings.proxy.username,
            password=self.context.settings.proxy.password,
            country=proxy_country,
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

    
    def call_proxy(self, url: str, config: dict, force_refresh: bool = False, callback: callable = None) -> dict:
        """
        Resolves the redirect of the given URL.

        Args:
            url (str): The URL to resolve.

        Returns:
            str: The resolved URL.
        """
        data_hash = self._generate_hash((url, str(config)))

        if not force_refresh and (cached := self._read_cache(data_hash)) is not None:
            logger.warning("Using cached response for proxy (%s)", data_hash)
            return cached

        resolved_url = url
        start_time = time.time()
        try:
            country = config["country"]

            proxy_url = self._build_proxy_url(country)
            resolved_url = self._resolve_redirect(url, proxy_url)

            self._write_cache(data_hash, {
                "resolved_url": resolved_url,
                "seconds_taken": time.time() - start_time,
            })
            
        except Exception as e:
            logger.warning(f"Failed to resolve redirect for {url}: {e}")
        
        return {
            "resolved_url": resolved_url,
            "seconds_taken": time.time() - start_time,
        }