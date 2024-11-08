import os
import json
import hashlib
import logging
from typing import Any, Dict

from nightcrawler.helpers import CACHE_DIR, LOGGER_NAME
from nightcrawler.context import Context

logger = logging.getLogger(LOGGER_NAME)


class APICaller:
    """
    A base class to handle caching of remote API calls.
    """

    def __init__(
        self, context: Context, cache_name: str = "default", max_retries: int = 3, retry_delay: int = 2, cache_duration: int = 24*60*60
    ):
        """
        Initializes the base class APICaller class.

        Args:
            context (Context): Context object
            cache_name (str): The name of the cache (default is "serpapi").
            max_retries (int): The maximum number of retries for API calls (default is 3).
            retry_delay (int): The delay in seconds between retry attempts (default is 2).
            cache_duration (int): The delay in seconds between a cache entry is considered obsolete.
        """

        self.context = context

        if self.context.settings.use_file_storage:
            self.cache_dir = os.path.join(CACHE_DIR, cache_name)
            os.makedirs(self.cache_dir, exist_ok=True)

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.cache_name = cache_name
        self.cache_duration = cache_duration

    @staticmethod
    def _generate_hash(data: Any) -> str:
        data_str = str(data)
        return hashlib.sha256(data_str.encode("utf-8")).hexdigest()

    def _cache_path(self, data_hash: str) -> str:
        if not self.context.settings.use_file_storage:
            return os.path.join(self.cache_name, f"{data_hash}.cache")

        return os.path.join(self.cache_dir, f"{data_hash}.cache")

    def _is_cached(self, data_hash: str) -> bool:
        return os.path.exists(self._cache_path(data_hash))

    def _write_cache(self, data_hash: str, response: Dict[str, Any]) -> None:
        path = self._cache_path(data_hash)
        logger.warning("Writing to cache: %s", path)

        if not self.context.settings.use_file_storage:
            self.context.blob_client.cache(path, response)
            return

        with open(path, "w") as cache_file:
            json.dump(response, cache_file)

    def _read_cache(self, data_hash: str) -> Dict[str, Any] | None:
        path = self._cache_path(data_hash)

        if not self.context.settings.use_file_storage:
            return self.context.blob_client.get_cached(path, self.cache_duration)

        if not self._is_cached(data_hash):
            return None

        with open(path, "r") as cache_file:
            return json.load(cache_file)
