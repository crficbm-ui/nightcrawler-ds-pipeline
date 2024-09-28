import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class BlockedContentDetector(BaseStep):
    """Implementation of the blocked content detection (step 7)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.processing_filename_blocked_content_detection,
        )
        return pipelineResults
