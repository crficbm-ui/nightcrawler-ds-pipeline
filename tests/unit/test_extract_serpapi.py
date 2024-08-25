import pytest
from unittest.mock import MagicMock, patch
from helpers.api.serp_api import SerpAPI
from nightcrawler.extract.serp_api import SerpapiExtractor


@pytest.fixture
def serpapi_extractor():
    context = MagicMock()
    context.settings.serp_api.token = "dummy_api_key"
    context.serpapi_filename = "dummy_filename.json"
    return SerpapiExtractor(context)


def test_initiate_client(serpapi_extractor):
    client = serpapi_extractor.initiate_client()
    assert isinstance(client, SerpAPI)


@patch.object(SerpAPI, "call_serpapi")
def test_retrieve_response(mock_call_serpapi, serpapi_extractor):
    mock_call_serpapi.return_value = {"mock_key": "mock_value"}
    response = serpapi_extractor.retrieve_response(
        "aspirin", serpapi_extractor.initiate_client(), 3
    )
    mock_call_serpapi.assert_called_once_with(
        {"q": "aspirin", "tbm": "", "start": 0, "num": 3, "api_key": "dummy_api_key"},
        log_name="google_regular",
    )
    assert response == {"mock_key": "mock_value"}


@patch.object(SerpAPI, "get_organic_results")
@patch.object(SerpAPI, "_check_limit")
def test_structure_results(
    mock_check_limit, mock_get_organic_results, serpapi_extractor
):
    mock_get_organic_results.return_value = [{"link": "http://example.com"}]
    mock_check_limit.return_value = ["http://example.com"]
    response = {"mock_key": "mock_value"}
    assert serpapi_extractor.structure_results(
        response, serpapi_extractor.initiate_client(), True, "aspirin"
    ) == [{"link": "http://example.com"}]
    assert serpapi_extractor.structure_results(
        response, serpapi_extractor.initiate_client(), False, "aspirin"
    ) == ["http://example.com"]


@patch("nightcrawler.extract.serp_api.write_json")
def test_store_results(mock_write_json, serpapi_extractor):
    serpapi_extractor.store_results(["http://example.com"], "/tmp")
    mock_write_json.assert_called_once_with(
        "/tmp", "dummy_filename.json", ["http://example.com"]
    )


@patch.object(SerpapiExtractor, "store_results")
@patch.object(SerpapiExtractor, "structure_results")
@patch.object(SerpapiExtractor, "retrieve_response")
@patch.object(SerpapiExtractor, "initiate_client")
def test_apply(
    mock_initiate_client,
    mock_retrieve_response,
    mock_structure_results,
    mock_store_results,
    serpapi_extractor,
):
    mock_initiate_client.return_value = MagicMock(spec=SerpAPI)
    mock_retrieve_response.return_value = {"mock_key": "mock_value"}
    mock_structure_results.return_value = ["http://example.com"]
    results = serpapi_extractor.apply("aspirin", 3, "/tmp", full_output=False)
    mock_initiate_client.assert_called_once()
    mock_retrieve_response.assert_called_once_with(
        "aspirin", mock_initiate_client.return_value, 3
    )
    mock_structure_results.assert_called_once_with(
        {"mock_key": "mock_value"}, mock_initiate_client.return_value, False, "aspirin"
    )
    mock_store_results.assert_called_once_with(["http://example.com"], "/tmp")
    assert results == ["http://example.com"]
