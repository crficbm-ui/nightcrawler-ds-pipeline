import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class SuspiciousnessClassifier(BaseStep):
    """Implementation of the classifier for suspiciousness(step 9)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.processing_filename_suspiciousness_classifier,
        )
        return pipelineResults
