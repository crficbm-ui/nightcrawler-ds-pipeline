
import logging

from typing import List
from nightcrawler.contex import Context

class DataProcessor:
    ### TODO: Alex had a ABC class here.. needed?
    """Implements the data processing"""

    _entity_name = __qualname__  # type: ignore
    ### ASK ALEX what's the deal?

    def __init__(self):
        logging.info(f"Initializing data collection : {self._entity_name}")
        self.context = Context()

        processed_urls = dp_client.process_urls_from_datacollector(country=args.country)

    def process_urls_from_datacollector(self, urlpath: str = "") -> List[dict]:
        # TODO: implement data proessing

        if not urlpath:
            urlpath = self.context.processing_country_filter_output_path

        with open(urlpath, "r") as file:
            urls = file.read()
        results = [{"url": url, "title": "xxx"} for url in urls]
        write_json(self.context.output_path, self.context.diffbot_filename, results)
        return results
