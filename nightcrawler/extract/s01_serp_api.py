import logging
import re
from typing import Any, Dict, List, Callable
from nightcrawler.context import Context
from nightcrawler.helpers.api.serp_api import SerpAPI
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.utils import remove_tracking_parameters

from nightcrawler.base import (
    ExtractSerpapiData,
    MetaData,
    PipelineResult,
    Extract,
    CounterCallback,
    Organization,
    Marketplace,
)


logger = logging.getLogger(LOGGER_NAME)


class SerpapiExtractor(Extract):
    """
    Implements data collection using SerpAPI for various search engines, including Google and eBay.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(
        self,
        context: Context,
        organization: Organization,
        prevent_auto_correct: bool = True,
    ) -> None:
        """
        Initializes the SerpapiExtractor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        super().__init__(self._entity_name)
        self.context = context

        self.organization = organization
        self.country = organization.countries[0]
        self._google_params = {
            "engine": "google",
            "location_requested": self.country,
            "location_used": self.country,
            "google_domain": f"google.{organization.country_codes[0].lower()}",
            "tbs": f"ctr:{organization.country_codes[0].upper()}&cr=country{organization.country_codes[0].upper()}",
            "gl": organization.country_codes[0].lower(),
        }

        self._ebay_params = {
            "engine": "ebay",
            "ebay_domain": f"ebay.{organization.country_codes[0].lower()}",
            "_blrs": "spell_auto_correct" if prevent_auto_correct else "",
        }

        self.google_site_marketplaces = [
            Marketplace(**data)
            for data in organization.settings["default_marketplaces"]
        ] + [
            Marketplace(**data)
            for data in organization.settings["google_site_marketplaces"]
        ]

    def initiate_client(self) -> SerpAPI:
        """
        Initializes and returns the SerpAPI client.

        Returns:
            SerpAPI: An instance of the SerpAPI client.
        """
        return SerpAPI(self.context)

    def retrieve_response(
        self,
        keyword: str,
        client: SerpAPI,
        custom_params: Dict[str, Any] = {},
        offer_root: str = "DEFAULT",
        callback: Callable[int, None] | None = None,
    ) -> List[ExtractSerpapiData]:
        """
        Makes the API call to SerpAPI to retrieve search results for the given keyword.

        Args:
            client (SerpAPI): The SerpAPI client instance.
           custom_params (dict): parameters that are diferent to the default ones below. these are then added to the params dict.
           offer_root (str): the name of the offer
        Returns:
            List[ExtractSerpapiData]: The raw response data from the SerpAPI.
        """
        params = {
            "q": keyword,
            "start": 0,
            "api_key": self.context.settings.serp_api.token,
            **(custom_params),
        }
        logger.info(f"Extracting URLs from SerpAPI for '{keyword}' from '{offer_root}'")
        return client.call_serpapi(params, log_name="google_regular", callback=callback)

    def structure_results(
        self,
        keyword: str,
        response: Dict[str, Any],
        client: SerpAPI,
        offer_root: str = "DEFAULT",
        max_number_of_results: int = 0,
        check_limit: int = 200,
    ) -> List[ExtractSerpapiData]:
        """
        Processes and structures the raw API response data into the desired format.

        Args:
            response (Dict[str, Any]): The raw data returned from the API.
            client (SerpAPI): The SerpAPI client instance.
            offer_root (str): The source of the search results (e.g., "GOOGLE", "EBAY").

        Returns:
            List[Dict[str, Any]]: The structured search results.
        """

        if offer_root == "GOOGLE_SHOPPING":
            items = client.get_shopping_results(response)
        else:
            items = client.get_organic_results(response)

        urls = [item.get("link") for item in items]
        logger.debug(f"For {offer_root} retrieved {len(urls)}.")

        if offer_root == "GOOGLE_SITE":
            urls = self.filter_product_page_urls(urls, self.google_site_marketplaces)

        # get the urls and manually truncate them to number_of_results because ebay and shopping serpapi endpoints only know the '_ipg' argument that takes 25, 50 (default), 100 and 200
        urls = urls[:max_number_of_results]
        logger.debug(f"After manual truncation the length is {len(urls)}.")

        filtered_urls = client._check_limit(urls, keyword, check_limit)
        results = [
            ExtractSerpapiData(
                offerRoot=offer_root, url=remove_tracking_parameters(url)
            )
            for url in filtered_urls
        ]
        return results

    def results_from_marketplaces(
        self,
        client: SerpAPI,
        keyword: str,
        max_number_of_results: int,
        callback: Callable[int, None],
    ) -> List[ExtractSerpapiData]:
        # Define parameters and labels for different sources
        sources = [
            {
                "params": {**self._google_params, "num": 50},
                "label": "GOOGLE",
            },  # set "nume":50 only for the plain google search "label"
            {
                "params": {**self._google_params, "tbm": "shop"},
                "label": "GOOGLE_SHOPPING",
            },
            {
                "params": {
                    **self._google_params,
                    "q": f"{keyword} site:"
                    + " OR site:".join(
                        [m.root_domain_name for m in self.google_site_marketplaces]
                    ),
                },
                "label": "GOOGLE_SITE",
            },
            {
                "params": {
                    **self._ebay_params,
                    "_nkw": keyword,
                    "_ipg": max_number_of_results,
                },
                "label": "EBAY",
            },
        ]

        # Only if number of results is set, define the "num" parameter
        if max_number_of_results > 0:
            for elem in sources:
                elem["params"]["num"] = max_number_of_results

        logger.debug(f"SerpAPI configs: {sources}")

        # Collecting and structuring all results
        all_results = []
        for source in sources:
            response = self.retrieve_response(
                keyword=keyword,
                client=client,
                custom_params=source["params"],
                offer_root=source["label"],
                callback=callback,
            )
            structured_results = self.structure_results(
                keyword, response, client, source["label"], max_number_of_results
            )
            all_results.extend(structured_results)

        logger.debug(f"A total of {len(all_results)} serpapi results were stored.")
        return all_results

    @staticmethod
    def filter_product_page_urls(
        urls: list[str], marketplaces: List[Marketplace]
    ) -> list[str]:
        accepted_urls = []
        for url in urls:
            # Use a generator expression instead of a list comprehension
            if any(
                re.match(marketplace.product_page_url_pattern, url)
                for marketplace in marketplaces
            ):
                accepted_urls.append(url)

        logger.debug(
            f"Removed {len(urls) - len(accepted_urls)}/{len(urls)} URLs that did not match the Marketplace product pattern."
        )
        return accepted_urls

    def apply_step(self, keyword: str, max_number_of_results: int) -> PipelineResult:
        """
        Orchestrates the entire process of data collection: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            keyword (str): The search keyword.
            max_number_of_results (int): The number of search results to retrieve.

        Returns:
            PipelineResult: The final structured search results.
        """
        counter = CounterCallback()
        client = self.initiate_client()
        structured_results_from_marketplaces = self.results_from_marketplaces(
            client=client,
            keyword=keyword,
            max_number_of_results=max_number_of_results,
            callback=counter,
        )

        # Generate the metadata
        metadata = MetaData(
            keyword=keyword,
            numberOfResults=max_number_of_results,
            numberOfResultsAfterStage=len(structured_results_from_marketplaces),
        )

        # Combining all structured results
        structured_results_from_marketplaces = PipelineResult(
            meta=metadata,
            results=structured_results_from_marketplaces,
            usage={"serpapi": counter.value},
        )

        self.store_results(
            structured_results_from_marketplaces,
            self.context.output_dir,
            self.context.serpapi_filename,
        )

        return structured_results_from_marketplaces
