import logging
from typing import List

from nightcrawler.base import ContentDomainData, PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ContentDomainDetector(BaseStep):
    """Implementation of the content domain detection (step 8)"""

    def dummy_results(
        self, previous_steps_results: PipelineResult
    ) -> List[ContentDomainData]:
        return previous_steps_results.results

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic

        results = self.dummy_results(previous_step_results)

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results, pipelineResults=previous_step_results
        )

        self.store_results(
            pipeline_results,
            self.context.output_dir,
            self.context.processing_filename_content_domain_detection,
        )
        return pipeline_results
