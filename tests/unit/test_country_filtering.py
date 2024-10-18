import pytest
from unittest.mock import MagicMock

from nightcrawler.base import PipelineResult, CrawlResultData
from helpers.context import Context
from nightcrawler.process.s05_country_filterer import CountryFilterer


@pytest.fixture
def mock_context():
    """Fixture to create a mock context object."""
    context = MagicMock(spec=Context)
    context.output_dir = "/mock/output/dir"
    context.processing_filename_country_filtering = "process_country_filtering.json"
    return context


@pytest.fixture
def country_filterer(mock_context):
    """Fixture to create a PageTypeDetector instance."""
    return CountryFilterer(context=mock_context)


@pytest.fixture
def sample_previous_pipeline_result():
    """Fixture to create a sample PipelineResult object with mock results."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                                 url="u1.ch"),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.com")]
    )


@pytest.fixture
def sample_pipeline_result():
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                                 url="u1.ch",
                                 domain="u1.ch",
                                 filtererName="url",
                                 DeliveringtoCountry=1),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.com",
                                domain="u2.com",
                                filtererName="unknown",
                                DeliveringtoCountry=0)]
    )


def test_apply_step_country_filterer(country_filterer, sample_previous_pipeline_result, sample_pipeline_result):
    """Test the apply_step method for Zyte detection."""
    # Mock the `add_pipeline_steps_to_results` and `store_results` methods
    country_filterer.add_pipeline_steps_to_results = MagicMock(
        return_value=sample_pipeline_result
    )
    country_filterer.store_results = MagicMock()

    # Apply country filtering
    result = country_filterer.apply_step(
        previous_step_results=sample_previous_pipeline_result
    )

    # Check that results are added and stored correctly
    country_filterer.add_pipeline_steps_to_results.assert_called_once()
    country_filterer.store_results.assert_called_once()

    # Ensure that the returned result is a PipelineResult
    assert isinstance(result, PipelineResult)
