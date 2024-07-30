import logging
import json
import pandas as pd
from abc import ABC

from typing import List
from nightcrawler.contex import Context
from nightcrawler.utils import write_json


class DataCollector(ABC):
    """Implements the data collection via DiffBot and SerpAPI"""

    _entity_name = __qualname__  # type: ignore

    def __init__(self):
        logging.info(f"Initializing data collection : {self._entity_name}")
        self.context = Context()

    def get_diffbot_bulk(self, urlpath: str = "") -> List[dict]:
        # TODO: implement diffbot bulk calls

        if not urlpath:
            urlpath = self.context.diffbot_output_path

        with open(urlpath, "r") as file:
            urls = file.read()
        results = [{"url": url, "title": "xxx"} for url in urls]
        write_json(self.context.output_path, self.context.diffbot_filename, results)
        return results

    def get_urls_from_serpapi(self, keywords: List[str]) -> List[str]:
        # TODO: implement serpapi calls
        results = [f"www.{keyword}.ch" for keyword in keywords]  # toy
        write_json(self.context.output_path, self.context.serpapi_filename, results)
        return results
