import logging

from typing import Dict, List, Any

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.utils import evaluate_not_na
from nightcrawler.context import Context

from nightcrawler.base import (
    ProcessData,
    PipelineResult,
    ExtractZyteData,
    BaseStep,
)

logger = logging.getLogger(LOGGER_NAME)


class DataProcessor(BaseStep):
    """
    Implements the data processing.

    Attributes:
        context (Context): The context object containing configuration and settings.
    """

    def filter_per_country_results(
        self, context: Context, country: str, pipeline_result: PipelineResult
    ) -> PipelineResult:
        """
        Filters and processes results based on the specified country and writes the filtered results to a designated output path.

        This function reads a JSON file from a given URL path, applies filtering to retain only those items that meet the
        criteria for being sold in a given country [e.g., Switzerland ("result_sold_CH" is True)], and writes the filtered
        results to the specified output path.

        Args:
            context (Context): The context object containing paths for input and output processing.
            country (str): The country code to filter results by.

        Returns:
            PipelineResult: Filtered results based on the chosen country.
        """
        # TODO: Add the KEYWORD FILTER - input is NOT ZYTE - it is keyword filter.

        # TODO: ask Nico why we are reading in the previous step file and if it is okay, that I removed it.

        # Process the list of URLs
        zyte_results = pipeline_result.relevant_results
        raw_results = self._add_individual_features_swiss_url(zyte_results)

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        raw_results_object = self.add_pipeline_steps_to_results(
            currentStepResults=raw_results, pipelineResults=pipeline_result
        )

        # Update pipeline_result with processed results
        for index in range(len(zyte_results)):
            zyte_results[index] = raw_results[index]

        # Filter results based on country
        if country == "CH":
            relevant_results = [item for item in raw_results if item["result_sold_CH"]]
            irrelevant_results = [
                item for item in raw_results if not item["result_sold_CH"]
            ]

            # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
            country_results_object = self.add_pipeline_steps_to_results(
                currentStepResults=relevant_results,
                pipelineResults=pipeline_result,
                currentStepIrrelevantResults=irrelevant_results,
            )

            return raw_results_object, country_results_object

        return raw_results_object, []

    @staticmethod
    def _add_individual_features_swiss_url(
        raw_json_urls: List[ExtractZyteData],
    ) -> List[ProcessData]:
        """
        Verifies relevant features that characterize a product sold in CH and evaluates whether the product is actually sold in CH.

        Args:
            raw_json_urls (List[ExtractZyteData]): Original JSON containing URLs with additional metadata.

        Returns:
            List[ProcessData]: The processed JSON list, now with a 'result_sold_CH' key that determines if the product is sold in the Swiss market.
        """
        languages = ["ch-de", "/ch/", "swiss", "/CH/", "/fr"]
        shops = [
            "anastore",
            "ayurveda101",
            "biovea",
            "bodysport",
            "brack",
            "brain-effect",
            "ebay",
            "gesund-gekauft",
            "kanela",
            "myfairtrade",
            "nurnatur",
            "nu3",
            "plantavis",
            "shop-apotheke",
            "herbano",
            "onebioshop",
            "puravita",
            "sembrador",
            "vitaminexpress",
            "wish",
        ]
        web_extensions = [".ch", "ch."]
        price_swiss_francs = ["CHF", "SFr"]

        CH_processed_json = [
            {
                **url_item,
                "ch_de_in_url": DataProcessor._is_substring_in_column(
                    url_item["url"], languages
                ),
                "swisscompany_in_url": DataProcessor._is_substring_in_column(
                    url_item["url"], shops
                ),
                "web_extension_in_url": DataProcessor._is_substring_in_column(
                    url_item["url"], web_extensions
                ),
                "francs_in_url": DataProcessor._is_substring_in_column(
                    url_item.get("price", ""), price_swiss_francs
                ),
            }
            for url_item in raw_json_urls
        ]

        # Add the 'result_sold_CH' key to each item
        features_to_check = [
            "ch-de_in_url",
            "swisscompany_in_url",
            "web_extension_in_url",
            "francs_in_url",
        ]
        CH_processed_json = [
            ProcessData(
                **url_item,
                result_sold_CH=DataProcessor._has_at_least_one_feature(
                    url_item, features_to_check
                ),
            )
            for url_item in CH_processed_json
        ]

        return CH_processed_json

    @staticmethod
    def _has_at_least_one_feature(
        item_json: Dict[str, Any], features_to_check: List[str]
    ) -> bool:
        """
        Determines whether at least one specified feature is present in the given item dictionary.

        Args:
            item_json (Dict[str, Any]): A dictionary with fields corresponding to metadata per URL.
            features_to_check (List[str]): A list of feature names to check within the `item_json` dictionary.

        Returns:
            bool: True if the item has at least one of the specified features, False otherwise.
        """
        return any(item_json.get(feature, False) for feature in features_to_check)

    @staticmethod
    def _is_substring_in_column(_input: str, substrings: List[str]) -> bool:
        """
        Checks if any of the specified substrings are present in the given input string. Returns True if at least one substring is found; otherwise, returns False.

        Args:
            _input (str): The input string to search within.
            substrings (List[str]): A list of substrings to check for within the `_input`.

        Returns:
            bool: True if any substring in `substrings` is found within `_input`, False otherwise.
        """
        return evaluate_not_na(_input) and any(
            substring in _input for substring in substrings
        )

    def apply_step(
        self, previous_step_results: PipelineResult = None, country: str = "CH"
    ) -> PipelineResult:
        """
        Placeholder for the full data processing pipeline.

        Args:
            urls (List[Dict[str, str]], optional): A list of URLs to process. Defaults to an empty list.

        Raises:
            NotImplementedError: Raised as the full pipeline is not yet implemented.
        """

        # TODO this will be replaced by ML-based filtering implemented by Nico W.

        all_results, country_filtered_results = self.filter_per_country_results(
            self.context, country, previous_step_results
        )

        if isinstance(country_filtered_results, PipelineResult):
            if len(country_filtered_results.relevant_results) == 0:
                logger.warning(
                    "After filtering per country variable, no results move further in the pipeline."
                )
            return country_filtered_results
        return all_results
