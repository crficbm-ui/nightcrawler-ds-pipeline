import pytest
from unittest.mock import patch, MagicMock

from nightcrawler.base import PipelineResult, CrawlResultData, DeliveryPolicyData
from helpers.context import Context
from nightcrawler.process.s06_delivery_page_detection import DeliveryPolicyExtractor, BaseShippingPolicyFilterer


# ---------------------------------------------------
# Tests for Switzerland
# ---------------------------------------------------


@pytest.fixture
def sample_previous_pipeline_result_ch():
    """Fixture to create a sample PipelineResult object of previous step (step 5 country filtering) with mock results, for Switzerland."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                           url="u1.aa",
                           domain="u1.aa",
                           filtererName="unknown",
                           deliveringToCountry=0)]
    )


@pytest.fixture
def sample_delivery_policy_extraction_result_ch():
    """Fixture to create a sample Delivery policy extraction result, for Switzerland."""
    return [DeliveryPolicyData(offerRoot="GOOGLE",
                           url="u1.aa",
                           domain="u1.aa",
                           filtererName="shipping_policy",
                           deliveringToCountry=1,
                           labelJustif="the text stricly mentions the fact that the website ships to Switzerland")
    ]


@patch.object(BaseShippingPolicyFilterer, "save_new_known_domains") # Patch save_new_known_domains in BaseShippingPolicyFilterer
@patch.object(DeliveryPolicyExtractor, "_setup_mistral_client") # Patch _setup_mistral_client in DeliveryPolicyExtractor
@patch.object(DeliveryPolicyExtractor, "_setup_zyte_client_policy_page") # Patch _setup_zyte_client_policy_page in DeliveryPolicyExtractor
@patch.object(DeliveryPolicyExtractor, "_setup_zyte_client_product_page") # Patch _setup_zyte_client_product_page in DeliveryPolicyExtractor
def test_get_step_results_delivery_policy_extractor_ch(mock__setup_zyte_client_product_page, 
                                                       mock__setup_zyte_client_policy_page,
                                                       mock__setup_mistral_client,
                                                       mock_save_new_known_domains,
                                                       sample_previous_pipeline_result_ch, 
                                                       sample_delivery_policy_extraction_result_ch):
    # Mock Zyte product page API
    mock_zyte_product_page_api = MagicMock()
    mock_zyte_product_page_api.call_api.return_value = {
            "browserHtml": "<html> <body>test content</body> <footer><a href='https://www.example.com' target='_blank'>Home</a> <a href='https://www.example.com/delivery' target='_blank'>Delivery conditions</a> </footer></html>"
        }
    mock__setup_zyte_client_product_page.return_value = mock_zyte_product_page_api

    # Mock Zyte policy page API
    mock_zyte_policy_page_api = MagicMock()
    mock_zyte_policy_page_api.call_api.return_value = {
            "browserHtml": "<html><body>Delivery to Switzerland is available</body></html>"
        }
    mock__setup_zyte_client_policy_page.return_value = mock_zyte_policy_page_api
    
    # Mock Mistral API
    mock_mistral_api = MagicMock()
    mock_mistral_api.call_api.return_value = {"content": 
            """{"is_shipping_ch_answer": "yes", 
            "is_shipping_ch_justification": "the text stricly mentions the fact that the website ships to Switzerland"}"""
    }
    mock__setup_mistral_client.return_value = mock_mistral_api

    # Mock save_new_known_domains
    mock_save_new_known_domains.return_value = None

    # Initialize DeliveryPolicyExtractor
    delivery_policy_extractor = DeliveryPolicyExtractor(context=Context(), country="CH")
    
    # Apply delivery policy extraction
    delivery_policy_extractor_result_ch = delivery_policy_extractor.get_step_results(previous_steps_results=sample_previous_pipeline_result_ch)
    print(delivery_policy_extractor_result_ch)
    print(sample_delivery_policy_extraction_result_ch)

    # Check delivery policy extraction worked
    assert delivery_policy_extractor_result_ch == sample_delivery_policy_extraction_result_ch


# ---------------------------------------------------
# Tests for Austria
# ---------------------------------------------------

@pytest.fixture
def sample_previous_pipeline_result_at():
    """Fixture to create a sample PipelineResult object of previous step (step 5 country filtering) with mock results, for Austria."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                           url="u1.aa",
                           domain="u1.aa",
                           filtererName="unknown",
                           deliveringToCountry=0)]
    )


@pytest.fixture
def sample_delivery_policy_extraction_result_at():
    """Fixture to create a sample Delivery policy extraction result, for Austria."""
    return [DeliveryPolicyData(offerRoot="GOOGLE",
                           url="u1.aa",
                           domain="u1.aa",
                           filtererName="shipping_policy",
                           deliveringToCountry=1,
                           labelJustif="the text stricly mentions the fact that the website ships to Austria")
    ]


@patch.object(BaseShippingPolicyFilterer, "save_new_known_domains") # Patch save_new_known_domains in BaseShippingPolicyFilterer
@patch.object(DeliveryPolicyExtractor, "_setup_mistral_client") # Patch _setup_mistral_client in DeliveryPolicyExtractor
@patch.object(DeliveryPolicyExtractor, "_setup_zyte_client_policy_page") # Patch _setup_zyte_client_policy_page in DeliveryPolicyExtractor
@patch.object(DeliveryPolicyExtractor, "_setup_zyte_client_product_page") # Patch _setup_zyte_client_product_page in DeliveryPolicyExtractor
def test_get_step_results_delivery_policy_extractor_at(mock__setup_zyte_client_product_page, 
                                                       mock__setup_zyte_client_policy_page,
                                                       mock__setup_mistral_client,
                                                       mock_save_new_known_domains,
                                                       sample_previous_pipeline_result_at, 
                                                       sample_delivery_policy_extraction_result_at):
    # Mock Zyte product page API
    mock_zyte_product_page_api = MagicMock()
    mock_zyte_product_page_api.call_api.return_value = {
            "browserHtml": "<html> <body>test content</body> <footer><a href='https://www.example.com' target='_blank'>Home</a> <a href='https://www.example.com/delivery' target='_blank'>Delivery conditions</a> </footer></html>"
        }
    mock__setup_zyte_client_product_page.return_value = mock_zyte_product_page_api

    # Mock Zyte policy page API
    mock_zyte_policy_page_api = MagicMock()
    mock_zyte_policy_page_api.call_api.return_value = {
            "browserHtml": "<html><body>Delivery to Austria is available</body></html>"
        }
    mock__setup_zyte_client_policy_page.return_value = mock_zyte_policy_page_api
    
    # Mock Mistral API
    mock_mistral_api = MagicMock()
    mock_mistral_api.call_api.return_value = {"content": 
            """{"is_shipping_at_answer": "yes", 
            "is_shipping_at_justification": "the text stricly mentions the fact that the website ships to Austria"}"""
    }
    mock__setup_mistral_client.return_value = mock_mistral_api

    # Mock save_new_known_domains
    mock_save_new_known_domains.return_value = None

    # Initialize DeliveryPolicyExtractor
    delivery_policy_extractor = DeliveryPolicyExtractor(context=Context(), country="AT")
    
    # Apply delivery policy extraction
    delivery_policy_extractor_result_at = delivery_policy_extractor.get_step_results(previous_steps_results=sample_previous_pipeline_result_at)
    print(delivery_policy_extractor_result_at)
    print(sample_delivery_policy_extraction_result_at)

    # Check delivery policy extraction worked
    assert delivery_policy_extractor_result_at == sample_delivery_policy_extraction_result_at
