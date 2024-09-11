import pytest
from unittest.mock import MagicMock, patch
from nightcrawler.extract.serp_api import SerpapiExtractor


@pytest.fixture
@patch('nightcrawler.logic.s01_serp.SerpLogic._setup_client')  # Patch _setup_client in SerpLogic
def serpapi_extractor(mock_setup_client):
    mock_client = MagicMock()
    mock_setup_client.return_value = mock_client

    context = MagicMock()
    context.serpapi_filename = "dummy_serpapi_filename.json"

    return SerpapiExtractor(context)


@patch.object(SerpapiExtractor, "store_results")
def test_serpapi_extractor(mock_store_results, serpapi_extractor):
    serpapi_extractor.client.call_serpapi.return_value = {"organic_results": [{"link": "http://example.com/product1"}]}
    
    results = serpapi_extractor.apply("aspirin", 3, "/tmp", full_output=False)

    assert results == ["http://example.com/product1"]
    assert mock_store_results.call_count == 1
