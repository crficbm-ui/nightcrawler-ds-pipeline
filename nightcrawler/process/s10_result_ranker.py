import logging

from nightcrawler.base import PipelineResult, BaseStep

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


class ResultRanker(BaseStep):
    """Implementation of reranking of results(step 10)"""

    def apply(self, pipelineResults: PipelineResult) -> PipelineResult:
        # TODO implement logic
        self.store_results(
            pipelineResults,
            self.context.output_dir,
            self.context.filename_final_results,
        )
        return pipelineResults
