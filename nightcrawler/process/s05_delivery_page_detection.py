import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class DeliveryPolicyDetector(BaseStep):
    """Implementation of the delivery policy detection (step 5)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.processing_filename_delivery_policy,
        )
        return pipelineResults
