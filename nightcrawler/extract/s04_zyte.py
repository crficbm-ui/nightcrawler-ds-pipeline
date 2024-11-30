import logging
import base64
from typing import List, Dict, Tuple, Any, Callable
from tqdm.auto import tqdm

from nightcrawler.context import Context
from nightcrawler.helpers.api.zyte_api import ZyteAPI, DEFAULT_CONFIG
from nightcrawler.helpers import LOGGER_NAME

from charset_normalizer import detect


from nightcrawler.base import (
    ExtractZyteData,
    PipelineResult,
    Extract,
    CounterCallback,
)

logger = logging.getLogger(LOGGER_NAME)


class ZyteExtractor(Extract):
    """
    Implements the data collection via Zyte.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the ZyteExtractor with the given context.

        Args:
            context (Context): The context object containing configuration and settings.
        """
        super().__init__(self._entity_name)
        self.context = context

    def initiate_client(self) -> Tuple[ZyteAPI, Dict[str, Any]]:
        """
        Initializes and returns the ZyteAPI client and its configuration.

        Returns:
            Tuple[ZyteAPI, Dict[str, Any]]: The ZyteAPI client instance and its configuration.
        """
        client = ZyteAPI(self.context)
        return client, DEFAULT_CONFIG

    def retrieve_response(
        self,
        client: ZyteAPI,
        serpapi_results: PipelineResult,
        api_config: Dict[str, Any],
        callback: Callable[int, None] | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Makes the API calls to ZyteAPI to retrieve data from the provided URLs.

        Args:
            client (ZyteAPI): The ZyteAPI client instance.
            serpapi_results PipelineResult: The list of results from Serpapi containing URLs to process.
            api_config (Dict[str, Any]): The configuration settings for the ZyteAPI.

        Returns:
            List[Dict[str, Any]]: The list of responses from ZyteAPI.
        """
        urls = [item.get("url") for item in serpapi_results.relevant_results]
        responses = []

        with tqdm(total=len(urls)) as pbar:
            for url in urls:
                if len(url) < 3:
                    logger.error("Skipping invalid url '%s' !", url)
                    responses.append(
                        {
                            "error": True,
                            "error_message": f"Error during {self.current_step_name}: zyte received invalid url: {url}",
                        }
                    )
                    continue
                logger.warning("Zyte processing url %s", url)
                try:
                    response = client.call_api(url, api_config, callback=callback)
                except Exception as e:
                    logger.critical("Failed to call zyte for url %s", url)
                    logger.debug(e, exc_info=True)
                    response = {
                        "error": True,
                        "error_message": f"Error during {self.current_step_name}: zyte extraction: {e}",
                    }
                if not response:
                    logger.error(f"Failed to collect product from {url}")
                    response = {
                        "error": True,
                        "error_message": f"Error during {self.current_step_name}: zyte failed to collect product from {url}",
                    }
                responses.append(response)
                pbar.update(1)
        return responses

    def structure_results(
        self,
        responses: List[Dict[str, Any]],
        serpapi_results: PipelineResult,
    ) -> PipelineResult:
        """
        Processes and structures the raw API responses into the desired format.

        Args:
            responses (List[Dict[str, Any]]): The raw data returned from the Zyte API.
            serpapi_results (PipelineResult): The initial Serpapi results to be enhanced with Zyte data. It contains data of ExtractSerpapiData within "results"

        Returns:
            PipelineResult: The updated Serpapi results with added Zyte data.
        """
        results = []
        for index, response in enumerate(responses):
            product = response.get("product", {})
            serpapi_result = serpapi_results.relevant_results[index]
            try:
                html = self._get_html_from_response(response)
            except Exception as e:
                logger.error("Failed to extract html from response")
                response["error_message"] = e
                logger.warning(e, exc_info=True)
                html = ""
            metadata = product.get("metadata", {})
            price = f"{product.get('price', '')} {product.get('currencyRaw', '')}"  # returns " " if both fields were empty
            price = (
                price if len(price.strip()) > 1 else ""
            )  # hence remove the whitespace

            images = list()
            main_image = product.get("mainImage", None)
            if main_image:
                images.append(main_image["url"])
            for image in product.get("images") or []:
                images.append(image["url"])

            # Keep first 10 images and make sure first image stays first
            if images:
                uniques = set(images)
                uniques.remove(images[0])
                images = ([images[0]] + list(uniques))[:10]

            # Update serpapi_result to include the error_messages
            error_message = response.get("error_message")
            if error_message:
                if not hasattr(serpapi_result, "error_messages"):
                    serpapi_result.error_messages = []
                serpapi_result.error_messages.append(error_message)

            if serpapi_result.url:
                # Extract Zyte data
                zyte_result = ExtractZyteData(
                    **serpapi_result,
                    price=price,
                    title=product.get("name", ""),
                    fullDescription=product.get("description", ""),
                    zyteExecutionTime=response.get("seconds_taken", 0),
                    zyteProbability=metadata.get("probability", None),
                    html=html,
                    images=list(images),
                )

                results.append(zyte_result)

        return results

    def _get_html_from_response(self, response: Dict) -> str:
        if "browserHtml" in response:
            return response["browserHtml"]
        elif "httpResponseBody" in response:
            decoded_data = base64.b64decode(response["httpResponseBody"])
            detected_encoding = detect(decoded_data).get("encoding", "utf-8")
            return decoded_data.decode(detected_encoding or "utf-8", errors="replace")
        return ""

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        """
        Orchestrates the entire data collection process: client initiation,
        response retrieval, structuring results, and storing results.

        Args:
            previous_step_results (PipelineResult): The list of results from Serpapi containing URLs to process.

        Returns:
            PipelineResult: The final structured results.
        """
        counter = CounterCallback()
        client, api_config = self.initiate_client()
        responses = self.retrieve_response(
            client, previous_step_results, api_config, callback=counter
        )
        structured_results = self.structure_results(responses, previous_step_results)

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        zyte_results = self.add_pipeline_steps_to_results(
            currentStepResults=structured_results,
            pipelineResults=previous_step_results,
            usage={"zyte": counter.value} if counter.value else None,
        )

        return zyte_results
