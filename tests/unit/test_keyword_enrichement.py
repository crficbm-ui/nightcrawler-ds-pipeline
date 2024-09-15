from unittest.mock import MagicMock, patch

from helpers.api.serp_api import SerpAPI
from helpers.api.dataforseo_api import DataforSeoAPI

from nightcrawler.base import (
    ExtractSerpapiData,
    PipelineResult,
    CrawlResultData,
    MetaData,
)
from nightcrawler.extract.s02_enriched_keywords import KeyWordEnricher


def test_keyword_enricher_apply():
    """
    Test the apply method of KeyWordEnricher class.

    Input parameters:
    - keyword: 'test keyword'
    - serpapi: a mocked SerpAPI instance
    - number_of_results: 10
    - locations: ['United States']
    - languages: ['English']

    Expected results:
    - The result should be a PipelineResult instance.
    - result.meta should have keyword='test keyword', numberOfResults=10, numberOfResultsAfterStage matching the number of results.
    - result.results should be a list of ExtractSerpapiData instances with correct attributes.
    """

    # Set up a mock Context
    context = MagicMock()

    # Set up the nested settings attribute
    context.settings = MagicMock()
    context.settings.data_for_seo = MagicMock()
    context.settings.data_for_seo.api_params = {
        "US": {"location": "United States", "language": "English"},
        "CH": {"location": "Switzerland", "language": "German"},
    }
    context.output_dir = "/tmp"
    context.serpapi_filename_keyword_enrichement = "test_output.json"

    # Mock SerpAPI client
    serpapi_mock = MagicMock(spec=SerpAPI)
    serpapi_mock.initiate_client = MagicMock()
    serpapi_client_mock = MagicMock()
    serpapi_mock.initiate_client.return_value = serpapi_client_mock
    serpapi_mock.retrieve_response = MagicMock()
    serpapi_mock.retrieve_response.return_value = {
        "organic_results": [{"url": "https://example.com"}]
    }
    serpapi_mock.get_organic_results.return_value = [{"url": "https://example.com"}]
    serpapi_mock._check_limit.return_value = [{"url": "https://example.com"}]

    # Mock DataforSeoAPI methods
    dataforseo_api_mock = MagicMock(spec=DataforSeoAPI)
    dataforseo_api_mock.get_keyword_suggestions.return_value = [
        {
            "keywordEnriched": "test keyword suggestion",
            "keywordLocation": "United States",
            "keywordLanguage": "English",
            "keywordVolume": 1000,
            "offerRoot": "KEYWORD_SUGGESTION",
        }
    ]
    dataforseo_api_mock.get_related_keywords.return_value = [
        {
            "keywordEnriched": "test related keyword",
            "keywordLocation": "United States",
            "keywordLanguage": "English",
            "keywordVolume": 500,
            "offerRoot": "RELATED_KEYWORD",
        }
    ]

    # Patch DataforSeoAPI in the module where KeyWordEnricher is defined
    with patch(
        "nightcrawler.extract.s02_enriched_keywords.DataforSeoAPI",
        return_value=dataforseo_api_mock,
    ):
        # Mock the keywords_selection functions
        with patch(
            "helpers.analytics.keywords_selection.filter_keywords",
            side_effect=lambda x: x,
        ) as aggregate_keywords_mock, patch(
            "helpers.analytics.keywords_selection.estimate_volume_per_url"
        ) as estimate_volume_per_url_mock, patch(
            "helpers.analytics.keywords_selection.aggregate_urls"
        ) as aggregate_urls_mock:
            # Mock return values for aggregate_keywords
            aggregate_keywords_mock.return_value.to_dict.return_value = [
                {
                    "keywordEnriched": "test keyword suggestion",
                    "keywordLocation": "United States",
                    "keywordLanguage": "English",
                    "keywordVolume": 1000,
                    "offerRoot": "KEYWORD_SUGGESTION",
                },
                {
                    "keywordEnriched": "test related keyword",
                    "keywordLocation": "United States",
                    "keywordLanguage": "English",
                    "keywordVolume": 500,
                    "offerRoot": "RELATED_KEYWORD",
                },
            ]

            # Mock return values for estimate_volume_per_url
            estimate_volume_per_url_mock.return_value = [
                {"url": "https://example.com", "keywordVolume": 1000}
            ]

            # Mock return values for aggregate_urls
            aggregate_urls_mock.return_value = [
                {"url": "https://example.com", "keywordVolume": 1000}
            ]

            # Patch from_dict function
            with patch(
                "helpers.utils.from_dict", side_effect=lambda cls, data: cls(**data)
            ):
                # Patch store_results method to avoid file I/O
                with patch.object(KeyWordEnricher, "store_results", return_value=None):
                    # Instantiate KeyWordEnricher
                    keyword_enricher = KeyWordEnricher(context)

                    # Input parameters
                    keyword = "test keyword"
                    number_of_keywords = 10
                    locations = ["United States"]
                    languages = ["English"]

                    # Invoke the apply method
                    result = keyword_enricher.apply(
                        keyword=keyword,
                        serpapi=serpapi_mock,
                        number_of_keywords=number_of_keywords,
                        locations=locations,
                        languages=languages,
                        previous_step_results=PipelineResult(
                            meta=MetaData(keyword=keyword),
                            results=[
                                CrawlResultData(offerRoot="", url="https://example.com")
                            ],
                        ),
                    )

                    # Assertions
                    # Check that the result is a PipelineResult instance
                    assert isinstance(
                        result, PipelineResult
                    ), "Result is not a PipelineResult instance."

                    # Check metadata
                    assert (
                        result.meta.keyword == keyword
                    ), f"Expected keyword '{keyword}', got '{result.meta.keyword}'."
                    assert result.meta.numberOfResultsAfterStage == len(
                        result.results
                    ), "numberOfResultsAfterStage does not match the number of results."

                    # Check results
                    extract_data = result.results[-1]
                    assert isinstance(
                        extract_data, ExtractSerpapiData
                    ), "Result item is not an ExtractSerpapiData instance."
                    assert (
                        extract_data.url == "https://example.com"
                    ), f"Expected link 'https://example.com', got '{extract_data.url}'."
                    assert (
                        extract_data.keywordVolume == 1000
                    ), f"Expected keywordVolume '1000', got '{extract_data.keywordVolume}'."
