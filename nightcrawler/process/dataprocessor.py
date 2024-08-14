
import logging

from typing import List
from nightcrawler.contex import Context

class DataProcessor:
    """
    Implements the data processing

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name = __qualname__  # type: ignore

    def __init__(self):
        """
        Initializes the DataProcessor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        logging.info(f"Initializing data collection : {self._entity_name}")
        self.context = Context()

        processed_urls = dp_client.process_urls_from_datacollector(country=args.country)

    def full_pipeline(self, urls:List[str], num_of_results: int):
        """
        Performs data processing.

        Args:
            keyword (str): The keyword to search for.
        """
        urls = self.extract_serpapi(keyword=keyword, full_output=False, num_of_results= num_of_results)
        results = self.extract_zyte(urls=urls)
        return results

    def process_urls_from_datacollector(self, urlpath: str = "") -> List[dict]:
        # TODO: implement data proessing
        print('testing here')
        if not urlpath:
            urlpath = self.context.processing_country_filter_output_path

        with open(urlpath, "r") as file:
            urls = file.read()
        results = [{"url": url, "title": "xxx"} for url in urls]
        write_json(self.context.output_path, self.context.diffbot_filename, results)
        return results
