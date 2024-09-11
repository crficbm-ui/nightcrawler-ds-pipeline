import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ContentDomainDetector(BaseStep):
    """Implementation of the content domain detection (step 8)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.processing_filename_content_domain_detection,
        )
        return pipelineResults
