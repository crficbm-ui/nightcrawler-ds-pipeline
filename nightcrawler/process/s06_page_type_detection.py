import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class PageTypeDetector(BaseStep):
    """Implementation of the page type detection (step 6)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.processing_filename_page_type_detection,
        )
        return pipelineResults
