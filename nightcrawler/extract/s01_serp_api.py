import logging
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
    GOOGLE_SITE_MARKETPLACES,
    CounterCallback,
)

import libnightcrawler.objects as lo

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
        organization: lo.Organization,
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
        self.country = (
            "Austria" if organization.countries[0].upper() == "AT" else "Switzerland"
        )
        self._google_params = {
            "engine": "google",
            "location_requested": self.country,
            "location_used": self.country,
            "google_domain": f"google.{organization.countries[0].lower()}",
            "tbs": f"ctr:{organization.countries[0].upper()}&cr=country{organization.countries[0].upper()}",
            "gl": organization.countries[0].lower(),
        }

        self._ebay_params = {
            "engine": "ebay",
            "ebay_domain": f"ebay.{organization.countries[0].lower()}",
            "_blrs": "spell_auto_correct" if prevent_auto_correct else "",
        }

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
        number_of_results: int = 50,
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
            "num": number_of_results,
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
        number_of_results: int = 50,
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

        # get the urls and manually truncate them to number_of_results because ebay and shopping serpapi endpoints only know the '_ipg' argument that takes 25, 50 (default), 100 and 200
        urls = [item.get("link") for item in items]
        logger.debug(f"For {offer_root} retrieved {len(urls)}.")
        urls = urls[:number_of_results]
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
        number_of_results: int,
        callback: Callable[int, None],
    ) -> List[ExtractSerpapiData]:
        # Define parameters and labels for different sources
        sources = [
            {"params": self._google_params, "label": "GOOGLE"},
            {
                "params": {**self._google_params, "tbm": "shop"},
                "label": "GOOGLE_SHOPPING",
            },
            {
                "params": {
                    **self._google_params,
                    "q": f"{keyword} site:"
                    + " OR site:".join(
                        [
                            m.root_domain_name
                            for m in GOOGLE_SITE_MARKETPLACES
                            if self.organization.unit in m.affected_unit
                        ]
                    ),
                },
                "label": "GOOGLE_SITE",
            },
            {
                "params": {
                    **self._ebay_params,
                    "_nkw": keyword,
                    "_ipg": number_of_results,
                },
                "label": "EBAY",
            },
        ]

        logger.debug(f"SerpAPI configs: {sources}")

        # Collecting and structuring all results
        all_results = []
        for source in sources:
            response = self.retrieve_response(
                keyword=keyword,
                client=client,
                custom_params=source["params"],
                offer_root=source["label"],
                number_of_results=number_of_results,
                callback=callback,
            )
            structured_results = self.structure_results(
                keyword, response, client, source["label"], number_of_results
            )
            all_results.extend(structured_results)

        logger.debug(f"A total of {len(all_results)} serpapi results were stored.")
        return all_results

    def apply_step(self, keyword: str, number_of_results: int) -> PipelineResult:
        """
        Orchestrates the entire process of data collection: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            keyword (str): The search keyword.
            number_of_results (int): The number of search results to retrieve.

        Returns:
            PipelineResult: The final structured search results.
        """
        counter = CounterCallback()
        client = self.initiate_client()
        structured_results_from_marketplaces = self.results_from_marketplaces(
            client=client,
            keyword=keyword,
            number_of_results=number_of_results,
            callback=counter,
        )

        # Generate the metadata
        metadata = MetaData(
            keyword=keyword,
            numberOfResults=number_of_results,
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
