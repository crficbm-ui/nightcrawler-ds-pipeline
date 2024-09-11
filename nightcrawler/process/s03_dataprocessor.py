import logging
from helpers.decorators import timeit
from helpers import LOGGER_NAME

from nightcrawler.process.s04_filter_swiss_result import filter_per_country_results
from nightcrawler.base import PipelineResult, ProcessData, BaseStep

logger = logging.getLogger(LOGGER_NAME)


class DataProcessor(BaseStep):
    """
    Implements the data processing.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    def step_country_filtering(
        self, pipeline_result: PipelineResult, country: str = ""
    ) -> ProcessData:
        """
        Filters results based on the specified country and returns the filtered data. This method filters results
        according to the provided country and URL path. If no URL path is provided, it defaults to test data from the
        repository.

        Args:
            country (str, optional): The country code used to filter results. Defaults to an empty string.
            urlpath (str, optional): The path to the JSON file containing the raw data to be processed. If not provided,
                                     the method uses `self.context.zyte_filename`. Defaults to an empty string.

        Returns:
            ProcessData: a process data object representing the filtered results for the specified country.
        """
        country_filtered_results = filter_per_country_results(
            self.context, country, pipeline_result
        )

        if not country_filtered_results.results:
            logger.warning(
                "After filtering per country variable, no results move further in the pipeline."
            )

        return country_filtered_results

    @timeit
    def apply(
        self, pipeline_results: PipelineResult = None, country: str = "CH"
    ) -> PipelineResult:
        """
        Placeholder for the full data processing pipeline.

        Args:
            urls (List[Dict[str, str]], optional): A list of URLs to process. Defaults to an empty list.

        Raises:
            NotImplementedError: Raised as the full pipeline is not yet implemented.
        """

        # TODO this will be replaced by ML-based filtering implemented by Nico W.
        return self.step_country_filtering(
            country=country, pipeline_result=pipeline_results
        )
