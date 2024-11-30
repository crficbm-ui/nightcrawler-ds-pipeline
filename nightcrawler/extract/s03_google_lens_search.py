from typing import List, Tuple, Dict, Callable

import logging
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.context import Context
from nightcrawler.helpers.api.serp_api import SerpAPI

from nightcrawler.base import (
    ExtractSerpapiData,
    MetaData,
    PipelineResult,
    BaseStep,
    CounterCallback,
)

logger = logging.getLogger(LOGGER_NAME)


class GoogleLensApi(BaseStep):
    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        super().__init__(self._entity_name)

        self.context = context

        # Controls how many pages of google lens search results should be returned
        self._num_result_pages: int = 4

    def _run_google_lens_search(
        self,
        image_url: str,
        page_number: int,
        country: str = "ch",
        callback: Callable[int, None] | None = None,
    ) -> List[Tuple[str, str]]:
        """Runs google lens search to retrieve images and pages containing the image. See
        https://serpapi.com/https://serpapi.com/google-lens-api-api for more information.

        Args:
            image_url (str): Url of an image for which google lens search should be run.
            page_number (int): Which page of Google should be retrieved, starting with 1.

        Returns:
            List[Tuple[str, str]]: List of pairs (matched page URL, matched image URL).
        """
        params: Dict[str, str] = {
            "api_key": self.context.settings.serp_api.token,
            "engine": "google_lens",
            "country": country,
            "url": image_url,
        }
        response = SerpAPI(self.context).call_serpapi(
            params, log_name="google_lens", callback=callback
        )

        response_urls: List[Tuple[str, str]] = self._extract_urls_from_response(
            response
        )

        # If searching with Google, often the first result is a so-called inline-image. An inline-image is an image that appears directly within the search results, embedded alongside the text snippets for quick visual reference.
        # See here for full refenrece: https://serpapi.com/google-inline-images
        if page_number == 1:
            response_urls += self._extract_inline_urls_from_response(response)

        return response_urls

    @staticmethod
    def _extract_urls_from_response(response: dict) -> List[Tuple[str, str]]:
        """Extract URLs from the SerpAPI response for google lens search.

        Args:
            response (dict): Response data from SerpAPI.

        Returns:
            List[Tuple[str, str]]: List of tuples containing the page URL and image thumbnail URL.
        """
        # See: https://serpapi.com/google-google-lens for an example of the response

        # No results found
        if "visual_matches" not in response:
            return []

        # Results were found
        image_results: List[dict] = response["visual_matches"]
        urls: List[Tuple[str, str]] = []

        # Iterate through results
        for image_result in image_results:
            # See first two example shown here: https://serpapi.com/google-google-lens to get an understanding of the image_results object returned by serpapi.
            urls.append((image_result["link"], image_result.get("thumbnail", None)))

        return urls

    def _extract_inline_urls_from_response(
        self, response: dict
    ) -> List[Tuple[str, str]]:
        """Extract inline image URLs from the SerpAPI response.

        Args:
            response (dict): Response data from SerpAPI.

        Returns:
            List[Tuple[str, str]]: List of tuples containing the page URL and image thumbnail URL.
        """
        try:
            inline_urls: List[dict] = response["visual_matches"]
        except KeyError:
            logger.warning(
                "visual_matches field was not found in 1st page of response. Returning empty list."
            )
            return []

        urls: List[Tuple[str, str]] = []
        results_to_log: Dict[str, List[dict]] = {"visual_matches_collected": []}
        for image_result in inline_urls:
            # Here we are checking the entry is indeed a product, and that it also has a link field
            if "source" in image_result:
                urls.append(
                    (image_result["source"], image_result.get("thumbnail", None))
                )
                results_to_log["visual_matches_collected"].append(image_result)
            else:
                logger.warning(f'No "source" found for image result: {image_result}')

        logger.info(f"{len(urls)} URLs were extracted from inline_images: {urls}\n")
        return urls

    def apply_step(
        self, image_url: str, country: str, number_of_results: int
    ) -> PipelineResult:
        """Perform google lens search on multiple URLs and return structured results.

        Args:
            image_url (str): A URL to search for.
            number_of_results (int): Maximum number of results to return.

        Returns:
            PipelineResult: Structured result data including metadata and extracted information.
        """
        results: List[ExtractSerpapiData] = []
        logger.info(f"Performing google lens search on the following URL: {image_url}")
        # Run SerpApi multiple times - once for each page of results
        counter = CounterCallback()
        for page_number in range(self._num_result_pages):
            google_lens_urls = self._run_google_lens_search(
                image_url, page_number + 1, country=country, callback=counter
            )

            # No results found - there will be no results on the next page as well
            if len(google_lens_urls) == 0:
                break

            for image in google_lens_urls:
                results.append(
                    ExtractSerpapiData(
                        url=image[0],
                        imageUrl=image[1],
                        offerRoot="google_lens_search",
                    )
                )

        # TODO: force to only have the number of results specified in the CLI - not "nice" but the google_lens_api does not provide this parameter
        # either we keep it or we do not control the number of stored results from the pipeline - however this might lead to unintentional high zyte api costs as there can be easily produced a few dozen results by the google lens search
        # also, if we have a hard cut off, we remove the webpages where the imageUrl is empty
        skipped = [item for item in results if item.imageUrl is None]
        if skipped:
            logger.warning(
                "Skipping %d results: %s", len(skipped), [x.url for x in skipped]
            )

        results = [item for item in results if item.imageUrl is not None]
        results = results[:number_of_results]

        metadata = MetaData(
            keyword="Google lense search, no keyword provided.",
            numberOfResultsManuallySet=number_of_results,
        )

        # Combining all structured results
        image_search_results = PipelineResult(
            meta=metadata, relevant_results=results, usage={"serpapi": counter.value}
        )

        return image_search_results
