import logging
from typing import List

from nightcrawler.base import CrawlResultData, PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ResultRanker(BaseStep):
    """Implementation of reranking of results(step 10)"""

    def dummy_results(
        self, previous_steps_results: PipelineResult
    ) -> List[CrawlResultData]:
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
            self.context.filename_final_results,
        )

        return pipeline_results
