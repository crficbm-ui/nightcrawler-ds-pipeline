import base64
from unittest import TestCase
import pytest
from unittest.mock import MagicMock, patch

from nightcrawler.extract.zyte import ZyteExtractor


@pytest.fixture
@patch('nightcrawler.logic.s01_zyte.ZyteLogic._setup_client')  # Patch _setup_client in ZyteLogic
def zyte_extractor(mock_setup_client):
    mock_client = MagicMock()
    mock_setup_client.return_value = mock_client

    context = MagicMock()
    context.zyte_filename = "dummy_zyte_filename.json"

    return ZyteExtractor(context)


@patch.object(ZyteExtractor, "store_results")
def test_zyte_extractor_empty_(mock_store_results, zyte_extractor):
    # Test inputs
    true_inputs = {"urls": ["http://example.com/product1"], "output_dir": "/tmp"}

    # Scenario 1: Valid product information from the API
    zyte_extractor.logic.client.call_api.return_value = {
        "product": {"price": "100", "currencyRaw": "USD", "name": "Sample Product", "description": "Sample Description"},
        "metadata": {"probability": 0.95},
        "seconds_taken": 1,
        "browserHtml": "<html></html>"
    }
    
    true_result = {
        "url": "http://example.com/product1",
        "price": "100USD",
        "title": "Sample Product",
        "full_description": "Sample Description",
        "seconds_taken": "1",
        "html": "<html></html>",
        "zyte_probability": 0.95,
    }

    result = zyte_extractor.apply(**true_inputs)
    assert result == [true_result]

    # Scenario 2: Missing product data from the API
    zyte_extractor.logic.client.call_api.return_value = {
        "product": {},  # No product info
        "metadata": {"probability": 0.7},
        "seconds_taken": 2,
        "httpResponseBody": base64.b64encode(b"<html>no data</html>").decode(),
    }

    true_result_missing_data = {
        "url": "http://example.com/product1",
        "price": "",  # No price
        "title": "",  # No title
        "full_description": "",  # No description
        "seconds_taken": "2",
        "html": "<html>no data</html>",  # HTML is present
        "zyte_probability": 0.7,  # Probability from metadata
    }

    result_missing_data = zyte_extractor.apply(**true_inputs)
    assert result_missing_data == [true_result_missing_data]

    # Ensure store_results was called both times
    assert mock_store_results.call_count == 2
