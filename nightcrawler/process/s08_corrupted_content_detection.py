import logging
from typing import List, Dict, Any

from nightcrawler.context import Context
from nightcrawler.base import (
    PipelineResult,
    ExtractZyteData,
    BaseStep,
    Organization,
    CorruptedContentData,
)
from nightcrawler.helpers.api import endpoint_api
from nightcrawler.helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class CorruptedContentDetector(BaseStep):
    """Implementation of the content domain detection (step 8)"""

    DEFAULT_THRESHOLD = 0.3

    def __init__(
        self,
        context: Context,
        organization: Organization,
    ) -> None:
        """
        Initialize the CorruptedContentDetector class.

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
            endpoint_url=self.context.settings.corrupted_content.endpoint,
            endpoint_auth_creds=(
                self.context.settings.corrupted_content.username,
                self.context.settings.corrupted_content.password,
            ),
            cache_name="corrupted_content_detection",
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
        full_description = item.fullDescription if item.fullDescription else ""

        if not full_description:
            logger.warning(f"Item does not contain any text content. url: `{item.url}`")
            return {
                "is_corrupted_content": True,
                "corrupted_content_probability": 1.0,
            }

        #---------------------------------------------------------------------
        #
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        # THIS HAS BEEN DEACTIVATED BY NICO FOR CHILE's DEPLOYMENT
        #
        #---------------------------------------------------------------------
        print('CORRUPTED CONTENT FILTER DEACTIVATED FOR NOW')
        # api_response = self.api.call_api(playload={"text": full_description})
        # prediction = api_response["response"]["prediction"]
        prediction = {"score":0.0,"label":"LABEL_1"}

        # TODO: this logic should be moved into corresponding API service
        probability = (
            prediction["score"]
            if prediction["label"] == "LABEL_1"
            else 1 - prediction["score"]
        )

        is_corrupted_content = True if probability > self.DEFAULT_THRESHOLD else False

        logger.debug(
            f"Predicted: `{is_corrupted_content=}` with probability: `{probability:.2f}`"
        )

        return {
            "is_corrupted_content": is_corrupted_content,
            "corrupted_content_probability": probability,
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
        bypassed_results: List[Dict[str, Any]] = []

        prev_results: List[ExtractZyteData] = previous_step_result.relevant_results
        for item in prev_results:
            result = self._process_one(item)

            processed_data = {
                **item.to_dict(),
                **result,
            }
            if processed_data["is_corrupted_content"]:
                bypassed_results.append(CorruptedContentData(**processed_data))
            else:
                results.append(CorruptedContentData(**processed_data))

        return results, bypassed_results

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
        results, bypassed_results = self._process_prev_results(previous_step_results)

        # Update the PipelineResults Object
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results,
            pipelineResults=previous_step_results,
            currentStepBypassedtResults=bypassed_results,
        )
        return pipeline_results
