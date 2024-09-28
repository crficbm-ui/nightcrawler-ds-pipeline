from typing import List, Tuple, Dict
from urllib.parse import quote_plus

import logging
from helpers import LOGGER_NAME
from helpers.context import Context
from helpers.api.serp_api import SerpAPI

from nightcrawler.base import ExtractSerpapiData, MetaData, PipelineResult, BaseStep

logger = logging.getLogger(LOGGER_NAME)


class GoogleReverseImageApi(BaseStep):
    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        super().__init__(self._entity_name)

        self.context = context

        # Controls how many pages of reverse image search results should be returned
        self._num_result_pages: int = 4

    def _run_reverse_image_search(
        self, image_url: str, page_number: int
    ) -> List[Tuple[str, str]]:
        """Runs reverse image search to retrieve images and pages containing the image. See
        https://serpapi.com/google-reverse-image for more information.

        Args:
            image_url (str): Url of an image for which reverse image search should be run.
            page_number (int): Which page of Google should be retrieved, starting with 1.

        Returns:
            List[Tuple[str, str]]: List of pairs (matched page URL, matched image URL).
        """
        # TODO make this country / organization dependent
        params: Dict[str, str] = {
            "location": "Switzerland",
            "google_domain": "google.ch",
            "gl": "ch",
            "hl": "de",
            "lr": "lang_de|lang_fr",
            "api_key": self.context.settings.serp_api.token,
            "engine": "google_reverse_image",
            # need to double-urlencode the URL to make it work with SerpAPI and Google
            "image_url": quote_plus(quote_plus(image_url)),
            "start": str((page_number - 1) * 10),
        }
        response = SerpAPI().call_serpapi(params, log_name="google_reverse_image")

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
        """Extract URLs from the SerpAPI response for reverse image search.

        Args:
            response (dict): Response data from SerpAPI.

        Returns:
            List[Tuple[str, str]]: List of tuples containing the page URL and image thumbnail URL.
        """
        # See: https://serpapi.com/google-reverse-image for an example of the response

        # No results found
        if "image_results" not in response:
            return []

        # Results were found
        image_results: List[dict] = response["image_results"]
        urls: List[Tuple[str, str]] = []

        # Iterate through results
        for image_result in image_results:
            # See first two example shown here: https://serpapi.com/google-reverse-image to get an understanding of the image_results object returned by serpapi.
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
            inline_urls: List[dict] = response["inline_images"]
        except KeyError:
            logger.warning(
                "inline_images field was not found in 1st page of response. Returning empty list."
            )
            return []

        urls: List[Tuple[str, str]] = []
        results_to_log: Dict[str, List[dict]] = {"inline_results_collected": []}
        for image_result in inline_urls:
            # Here we are checking the entry is indeed a product, and that it also has a link field
            if "source" in image_result:
                urls.append(
                    (image_result["source"], image_result.get("thumbnail", None))
                )
                results_to_log["inline_results_collected"].append(image_result)
            else:
                logger.warning(f'No "source" found for image result: {image_result}')

        logger.info(f"{len(urls)} URLs were extracted from inline_images: {urls}\n")
        return urls

    def apply(self, image_url: str, number_of_results: int) -> PipelineResult:
        """Perform reverse image search on multiple URLs and return structured results.

        Args:
            image_url (str): List of image URLs to search for.
            number_of_results (int): Maximum number of results to return.

        Returns:
            PipelineResult: Structured result data including metadata and extracted information.
        """
        # TODO: where do we get the urls from that are public?
        results: List[ExtractSerpapiData] = []
        logger.info(
            f"Performing reverse image search on the following URL: {image_url}"
        )
        # Run SerpApi multiple times - once for each page of results
        for page_number in range(self._num_result_pages):
            reverse_image_urls = self._run_reverse_image_search(
                image_url, page_number + 1
            )

            # No results found - there will be no results on the next page as well
            if len(reverse_image_urls) == 0:
                break

            for image in reverse_image_urls:
                results.append(
                    ExtractSerpapiData(
                        url=image[0],
                        imageUrl=image[1],
                        offerRoot="REVERSE_IMAGE_SEARCH",
                    )
                )

        # TODO: force to only have the number of results specified in the CLI - not "nice" but the reverse_image_api does not provide this parameter
        # either we keep it or we do not control the number of stored results from the pipeline - however this might lead to unintentional high zyte api costs as there can be easily produced a few dozen results by the reverse image search
        # also, if we have a hard cut off, we remove the webpages where the imageUrl is empty
        results = [item for item in results if item.imageUrl is not None]
        results = results[:number_of_results]

        metadata = MetaData(
            keyword="Reverse image search, no keyword provided.",
            numberOfResults=number_of_results,
            numberOfResultsAfterStage=len(results),
        )

        # Combining all structured results
        structured_results_from_marketplaces = PipelineResult(
            meta=metadata, results=results
        )

        self.store_results(
            structured_results_from_marketplaces,
            self.context.output_dir,
            self.context.serpapi_filename_reverse_image_search,
        )

        return structured_results_from_marketplaces
