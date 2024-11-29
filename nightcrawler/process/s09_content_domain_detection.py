import logging
from typing import List, Dict, Any

from nightcrawler.context import Context
from nightcrawler.base import (
    DomainLabels,
    PipelineResult,
    ExtractZyteData,
    BaseStep,
    Organization,
)
from nightcrawler.helpers.api import endpoint_api
from nightcrawler.helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ContentDomainDetector(BaseStep):
    """Implementation of the content domain detection (step 8)"""

    def __init__(
        self,
        context: Context,
        organization: Organization,
    ) -> None:
        """
        Initialize the ContentDomainDetector class.

        Args:
            context (Context):
                The context object.
            organization (Organization):
                TODO: We could use different models for different organizations.

        Returns:
            None
        """
        super().__init__(context)
        self.context = context
        self.organization = organization

        self.api = self._api_initialization()

    def _api_initialization(self):
        return endpoint_api.EndpointAPI(
            context=self.context,
            endpoint_url=self.context.settings.content_domain.endpoint,
            endpoint_auth_creds=(
                self.context.settings.content_domain.username,
                self.context.settings.content_domain.password,
            ),
            cache_name="content_domain_detection",
        )

    def _process_one(self, item: ExtractZyteData) -> Dict[str, Any]:
        """
        Predict the content domain of the given item.

        Args:
            item (ExtractZyteData):
                The item to predict the content domain for.

        Returns:
            Dict[str, Any]:
                A dictionary containing the domain and the probability.
                Check ContentDomainData class for more details.
        """
        logger.debug(f"Calling domain detection API for url: `{item.url}`")
        title = item.title if item.title else ""
        full_description = item.fullDescription if item.fullDescription else ""

        if not title and not full_description:
            logger.warning(f"Item does not contain any text content. url: `{item.url}`")
            return {
                "content_domain_label": DomainLabels.UNKNOWN.value,
                "content_domain_probability": 1.0,
            }

        api_response = self.api.call_api(
            playload={"text": title + " " + full_description}
        )

        prediction = api_response["response"]["prediction"]

        # TODO: this logic should be moved into corresponding API service
        probability = (
            prediction["score"]
            if prediction["label"] == "LABEL_1"
            else 1 - prediction["score"]
        )

        domain = DomainLabels.MEDICAL if probability > 0.5 else DomainLabels.OTHER

        logger.debug(
            f"Predicted domain: `{domain}` with probability: `{probability:.2f}`"
        )

        return {
            "content_domain_label": domain.value,
            "content_domain_probability": probability,
        }

    def _process_prev_results(
        self, previous_step_result: PipelineResult
    ) -> List[Dict[str, Any]]:
        """
        Process data using Method 1.

        Args:
            previous_step_result (PipelineResult):
                The result from the previous pipeline step.

        Returns:
            List[Dict[str, Any]]:
                A list of processed results.
        """
        results: List[Dict[str, Any]] = []

        prev_results: List[ExtractZyteData] = previous_step_result.results
        for item in prev_results:
            result = self._process_one(item)

            processed_data = {
                **item.to_dict(),
                **result,
            }
            results.append(processed_data)  # TODO: Wrap into ContentDomainData

        return results

    def apply_step(
        self,
        previous_step_results: PipelineResult,
    ) -> PipelineResult:
        """
        Apply the processing step to the previous results.

        Args:
            previous_step_results (PipelineResult):
                The result from the previous pipeline step.

        Returns:
            PipelineResult:
                Updated PipelineResult after processing.
        """
        results = self._process_prev_results(previous_step_results)

        # Update the PipelineResults Object
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results, pipelineResults=previous_step_results
        )

        self.store_results(
            pipeline_results,
            self.context.output_dir,
            self.context.processing_filename_content_domain_detection,
        )
        return pipeline_results
