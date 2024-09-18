
import base64
from typing import List, Dict, Any, Tuple

from nightcrawler.logic.s00_base import BaseLogic

from helpers.api.zyte_api import ZyteAPI, DEFAULT_CONFIG


class ZyteLogic(BaseLogic):
    DEFAULT_CONFIG = DEFAULT_CONFIG

    def __init__(self, *args, **kwargs):
        self.config = kwargs.get("config", self.DEFAULT_CONFIG)
        self.api_config = kwargs.get("api_config", {})
        self.client = self._setup_client()

    def _setup_client(self) -> ZyteAPI:
        return ZyteAPI(**self.api_config)

    def _get_html_from_response(self, response: Dict) -> str:
        if "browserHtml" in response:
            return response["browserHtml"]
        elif "httpResponseBody" in response:
            return base64.b64decode(response["httpResponseBody"]).decode()
        return None
    
    def apply_one(self, item: Dict) -> Dict:
        print(self.client)
        try:
            response = self.client.call_api(item["url"], self.config)
        except Exception as e:
            raise ValueError(f"Failed to collect product from {item['url']}") from e
        
        html = self._get_html_from_response(response)
        product = response.get("product", {})
        metadata = product.get("metadata", {})

        result = {
            "url": item["url"],
            "zyte_probability": metadata.get("probability", None),
            "price": product.get("price", "") + product.get("currencyRaw", ""),
            "title": product.get("name", ""),
            "full_description": product.get("description", ""),
            "seconds_taken": str(response.get("seconds_taken", 0)),
            "html": html,
            "raw_response": response,
        }

        return result
