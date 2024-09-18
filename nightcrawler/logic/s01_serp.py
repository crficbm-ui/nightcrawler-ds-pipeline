import os
from typing import Any, Dict

from helpers.api import serp_api

from nightcrawler.logic.s00_base import BaseLogic


class SerpLogic(BaseLogic):
    def __init__(self, *args, **kwargs):
        self.client = self._setup_client()
        self.api_token = kwargs.get("api_token", os.environ.get("SERP_API_TOKEN", None))
        self.api_config = kwargs.get("api_config", {})

    def _setup_client(self) -> serp_api.SerpAPI:
        return serp_api.SerpAPI(**self.api_config)
    
    def apply_one(self, keyword: str, number_of_results: int, full_output: bool) -> Dict[str, Any]:
        params = {
            "q": keyword,
            "tbm": "",
            "start": 0,
            "num": int(number_of_results),
            "api_key": self.api_token,
        }

        raw_result = self.client.call_serpapi(params, log_name="google_regular")
        if "organic_results" not in raw_result:
            raise ValueError("No organic results found in the response")
        
        results = raw_result.get("organic_results", [])

        if not full_output:
            results = [item.get("link") for item in results]
            if len(results) > 200:
                results = results[:200]

        return results
