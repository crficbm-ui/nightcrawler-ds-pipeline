import pytest
from unittest.mock import patch, MagicMock

from nightcrawler.base import PipelineResult, CrawlResultData
from helpers.context import Context
from nightcrawler.process.s06_delivery_page_detection import DeliveryPolicyExtractor


# @pytest.fixture
# def mock_context():
#     """Fixture to create a mock context object."""
#     context = MagicMock(spec=Context)
#     context.output_dir = "/mock/output/dir"
#     context.processing_filename_delivery_policy = "process_delivery_policy.json"
#     return context


# @pytest.fixture(params=["CH", "AT"])
# def delivery_policy_extractor(mock_context, request):
#     """Fixture to create a Delivery Policy Extractor instance for different countries."""
#     return DeliveryPolicyExtractor(context=mock_context, country=request.param)


@pytest.fixture
@patch('nightcrawler.process.s06_delivery_page_detection.DeliveryPolicyExtractor._setup_zyte_client_product_page')  # Patch _setup_zyte_client_product_page in MasterCountryFilterer
def master_country_filterer(mock_setup_zyte_client_product_page):
    mock_client = MagicMock()
    mock_setup_zyte_client_product_page.return_value = {}

    return DeliveryPolicyExtractor(context=Context(), country="CH")


@patch.object(DeliveryPolicyExtractor, "._setup_zyte_client_product_page")  # Patch _setup_zyte_client_product_page in DeliveryPolicyExtractor
def DeliveryPolicyExtractor(mock_setup_zyte_client_product_page, mock_setup_zyte_client_policy_page):
    mock_client = MagicMock()
    mock_setup_zyte_client_product_page.return_value = {}
    mock_setup_zyte_client_policy_page.return_value = {}

    # return MasterCountryFilterer()


@pytest.fixture
def sample_previous_pipeline_result():
    """Fixture to create a sample PipelineResult object with mock results."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                                 url="u1.ch",
                                 domain="u1.ch",
                                 filtererName="url",
                                 deliveringToCountry=1),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.com",
                                filtererName="unknown",
                                deliveringToCountry=0)]
    )


@pytest.fixture
def sample_pipeline_result():
    """Fixture to create a sample PipelineResult object with mock results."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                                 url="u1.ch",
                                 domain="u1.ch",
                                 filtererName="url",
                                 deliveringToCountry=1,
                                 labelJustif=""),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.com",
                                domain="u2.com",
                                filtererName="unknown",
                                deliveringToCountry=0,
                                labelJustif="")]
    )


def test_apply_step_delivery_policy_extractor(delivery_policy_extractor, sample_previous_pipeline_result, sample_pipeline_result):
    """Test the apply_step method for Zyte detection."""
    # Mock the `add_pipeline_steps_to_results` and `store_results` methods
    delivery_policy_extractor.add_pipeline_steps_to_results = MagicMock(
        return_value=sample_pipeline_result
    )
    delivery_policy_extractor.store_results = MagicMock()

    # Apply delivery policy extraction
    result = delivery_policy_extractor.apply_step(
        previous_step_results=sample_previous_pipeline_result
    )

    # Check that results are added and stored correctly
    delivery_policy_extractor.add_pipeline_steps_to_results.assert_called_once()
    delivery_policy_extractor.store_results.assert_called_once()

    # Ensure that the returned result is a PipelineResult
    assert isinstance(result, PipelineResult)
