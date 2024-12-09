import logging
from typing import List

from nightcrawler.base import (
    PageTypeData,
    PipelineResult,
    BaseStep,
    PageTypes,
    ProcessingSteps,
)

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.context import Context

logger = logging.getLogger(LOGGER_NAME)


class PageTypeDetector(BaseStep):
    """Implementation of a binary classifier, specifing if a website is likely to be a product page or not (step 7)."""

    _entity_name: str = __qualname__

    THRESHOLD_ZYTE_PROB = 0.4

    def __init__(self, context: Context, *args, **kwargs):
        """
        Initialize the PageTypeDetector.

        :param context: Context object containing relevant configurations and data.
        :param args: Additional positional arguments.
        :param kwargs: Additional keyword arguments.
        """
        super().__init__(self._entity_name)
        self.context = context
        self.threshold: float = kwargs.get("threshold", self.THRESHOLD_ZYTE_PROB)

    def _get_pagetype_from_zyte(
        self, previous_step_result: PipelineResult
    ) -> List[PageTypeData]:
        """
        Determine the page type based on Zyte probability from the previous step results.

        :param previous_step_result: The result from the previous pipeline step.
        :return: A list of PageTypeData objects with updated page types.
        :raises ValueError: If Zyte probability is not present in the input data.
        """
        results: List[PageTypeData] = []
        irrelevant_results: List[PageTypeData] = []
        for deliver_policy_object in previous_step_result.relevant_results:
            error_message = None
            zyte_probability = deliver_policy_object.get("zyteProbability", None)

            if not zyte_probability:
                current_step_name = ProcessingSteps.__dict__.get(
                    f"STEP_{BaseStep._step_counter + 1}", "Unknown step"
                )
                error_message = f"Error during {current_step_name}: Item does not contain Zyte probability, setting it to 0."
                logger.error(error_message)
                zyte_probability = 0

            if error_message:
                if not hasattr(deliver_policy_object, "error_messages"):
                    deliver_policy_object["error_messages"] = []
                deliver_policy_object["error_messages"].append(error_message)

            page_type = PageTypes.OTHER
            if zyte_probability > self.threshold:
                page_type = PageTypes.ECOMMERCE_PRODUCT
                results.append(
                    PageTypeData(**deliver_policy_object, pageType=page_type)
                )
            else:
                irrelevant_results.append(
                    PageTypeData(**deliver_policy_object, pageType=page_type)
                )

        return results, irrelevant_results

    def _get_pagetype_from_binary_endpoint(
        self, previous_step_result: PipelineResult
    ) -> List[PageTypeData]:
        """
        Determine the page type based on a custom inference endpoint...
        """
        results: List[PageTypeData] = []
        irrelevant_results: List[PageTypeData] = []
        for deliver_policy_object in previous_step_result.relevant_results:
            html = deliver_policy_object.get("html", None)

            # Update results to include the concatenated error_message
            error_message = None

            if not html:
                error_message = (
                    "Error during step 7: page type detection: html is missing"
                )
                raise ValueError(error_message)

            page_type = PageTypes.OTHER

            logger.warning(
                "you used the inference, this is currenlty only dummy-implemented"
            )
            # TODO implement this on GPU and then make an endpoint call, something like:
            # proba = get_proba_from_endpoint(html)
            proba = 0.5

            # Update deliver_policy_object to include the concatenated error_message
            if error_message:
                if not hasattr(deliver_policy_object, "error_message"):
                    deliver_policy_object.error_message = []
                deliver_policy_object.error_message.append(error_message)

            if proba > self.threshold:
                page_type = PageTypes.ECOMMERCE_PRODUCT
                results.append(
                    PageTypeData(**deliver_policy_object, pageType=page_type)
                )
            else:
                irrelevant_results.append(
                    PageTypeData(**deliver_policy_object, pageType=page_type)
                )

        return results, irrelevant_results

    def apply_step(
        self, previous_step_results: PipelineResult, page_type_detection_method: str
    ) -> PipelineResult:
        """
        Apply the page type detection step to the previous results.

        :param previous_step_results: The result from the previous pipeline step.
        :param page_type_detection_method: Type of detection ("zyte" or "infer"). Defaults to "zyte".
        :return: Updated PipelineResult after page type detection.
        :raises RuntimeError: If detection type is not implemented.
        """
        if page_type_detection_method == "zyte":
            logger.info(
                "Using the a probability calculated by Zyte to determine, if the the page_type is likely to be ecommerce related."
            )
            results, irrelevant_results = self._get_pagetype_from_zyte(
                previous_step_results
            )
        else:
            logger.info("Using the binary inference to calculate the page type.")
            results, irrelevant_results = self._get_pagetype_from_binary_endpoint(
                previous_step_results
            )

        # Updating the PipelineResults Object (append the results to the results list and update the number of results after this stage)
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results,
            pipelineResults=previous_step_results,
            currentStepIrrelevantResults=irrelevant_results,
        )

        return pipeline_results
