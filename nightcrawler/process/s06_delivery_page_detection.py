import logging

from typing import List
from nightcrawler.base import DeliveryPolicyData, PipelineResult, BaseStep

from nightcrawler.helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class DeliveryPolicyDetector(BaseStep):
    """Implementation of the delivery policy detection (step 5)"""

    def dummy_results(
        self, previous_steps_results: PipelineResult
    ) -> List[DeliveryPolicyData]:
        return previous_steps_results.relevant_results

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic

        results = self.dummy_results(previous_step_results)

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results,
            pipelineResults=previous_step_results,
            currentStepIrrelevantResults=[],  # TODO, add irrelevant results
        )

        return pipeline_results
