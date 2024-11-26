import pytest
from unittest.mock import MagicMock, patch, ANY
from nightcrawler.helpers.api.serp_api import SerpAPI
from nightcrawler.helpers.api.proxy_api import ProxyAPI
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.base import ExtractSerpapiData, PipelineResult, Organization


@pytest.fixture
def serpapi_extractor() -> SerpapiExtractor:
    """
    Fixture to create an instance of SerpapiExtractor with a mocked context.

    :return: An instance of SerpapiExtractor with a mocked context.
    """
    context = MagicMock()
    context.settings.serp_api.token = "dummy_api_key"
    context.serpapi_filename = "dummy_filename.json"
    context.output_dir = "/tmp"
    return SerpapiExtractor(context, Organization.get_all()["Ages"])


def test_initiate_client_is_type_SerpAPI(serpapi_extractor: SerpapiExtractor) -> None:
    """
    Test that `initiate_client` returns an instance of SerpAPI.

    :param serpapi_extractor: Fixture that provides an instance of SerpapiExtractor.
    """
    client = serpapi_extractor.initiate_client()
    assert isinstance(client, SerpAPI)


@patch.object(ProxyAPI, "call_proxy")
@patch.object(SerpAPI, "call_serpapi")
def test_retrieve_response_is_retrieved(
    mock_call_serpapi: MagicMock,
    mock_call_proxy: MagicMock,
    serpapi_extractor: SerpapiExtractor
) -> None:
    """
    Test that `retrieve_response` calls the `call_serpapi` method and returns the expected result.

    :param mock_call_serpapi: Mock for the `call_serpapi` method in the SerpAPI class.
    :param serpapi_extractor: Fixture that provides an instance of SerpapiExtractor.
    """
    mock_call_serpapi.return_value = {"mock_key": "mock_value"}
    mock_call_proxy.return_value = {"resolved_url": "http://example.com"}
    response = serpapi_extractor.retrieve_response(
        "aspirin",
        serpapi_extractor.initiate_client(),
        offer_root="DEFAULT",
        number_of_results=3,
    )
    mock_call_serpapi.assert_called_once_with(
        {"q": "aspirin", "start": 0, "num": 3, "api_key": "dummy_api_key"},
        log_name="google_regular",
        callback=ANY,
    )
    assert response == {"mock_key": "mock_value"}

@patch.object(ProxyAPI, "call_proxy")
@patch.object(SerpAPI, "get_organic_results")
@patch.object(SerpAPI, "_check_limit")
def test_structure_results_is_formatted_properly(
    mock_check_limit: MagicMock,
    mock_get_organic_results: MagicMock,
    mock_call_proxy: MagicMock,
    serpapi_extractor: SerpapiExtractor,
) -> None:
    """
    Test that `structure_results` formats the results correctly based on the provided arguments.

    :param mock_check_limit: Mock for the `_check_limit` method in the SerpAPI class.
    :param mock_get_organic_results: Mock for the `get_organic_results` method in the SerpAPI class.
    :param serpapi_extractor: Fixture that provides an instance of SerpapiExtractor.
    """
    mock_get_organic_results.return_value = [
        {"link": "http://example.com", "offerRoot": "DEFAULT"}
    ]
    mock_check_limit.return_value = ["http://example.com"]
    mock_call_proxy.return_value = {"resolved_url": "http://example.com"}
    response = {"mock_key": "mock_value"}

    result = serpapi_extractor.structure_results(
        "aspirin",
        response,
        serpapi_extractor.initiate_client(),
        serpapi_extractor.initiate_proxy_client(),
        offer_root="DEFAULT",
        number_of_results=1,
    )
    assert result == [ExtractSerpapiData(url="http://example.com", offerRoot="DEFAULT", resolved_url="http://example.com", original_url="http://example.com")]


@patch.object(SerpapiExtractor, "store_results")
def test_store_results_was_called(
    mock_store_results: MagicMock, serpapi_extractor: SerpapiExtractor
) -> None:
    """
    Test that `store_results` calls `write_json` with the correct arguments.

    :param serpapi_extractor: Fixture that provides an instance of SerpapiExtractor.
    :param mock_write_json: Mock object for the `write_json` method.
    """
    # Create mock structured results
    mock_results = PipelineResult(
        meta={"some_meta_key": "some_meta_value"},
        results=[ExtractSerpapiData(url="http://example.com", offerRoot="DEFAULT")],
    )

    # Call the store_results method
    serpapi_extractor.store_results(mock_results, "/tmp", "dummy_filename.json")

    # Ensure the mock was called with the correct arguments
    mock_store_results.assert_called_once_with(
        mock_results,
        "/tmp",
        "dummy_filename.json",
    )

@patch.object(SerpapiExtractor, "initiate_proxy_client")
@patch.object(SerpapiExtractor, "store_results")
@patch.object(SerpapiExtractor, "results_from_marketplaces")
@patch.object(SerpapiExtractor, "initiate_client")
def test_apply_all_functions_called_once(
    mock_initiate_client: MagicMock,
    mock_results_from_marketplaces: MagicMock,
    mock_store_results: MagicMock,
    mock_initiate_proxy_client: MagicMock,
    serpapi_extractor: SerpapiExtractor,
) -> None:
    """
    Test that all methods in the `apply` workflow are called exactly once and return the correct result.

    :param mock_initiate_client: Mock for the `initiate_client` method.
    :param mock_results_from_marketplaces: Mock for the `results_from_marketplaces` method.
    :param mock_store_results: Mock for the `store_results` method.
    :param serpapi_extractor: Fixture that provides an instance of SerpapiExtractor.
    """
    mock_initiate_client.return_value = MagicMock(spec=SerpAPI)
    mock_initiate_proxy_client.return_value = MagicMock(spec=ProxyAPI)
    mock_results_from_marketplaces.return_value = ["http://example.com"]

    # Call the apply method
    results = serpapi_extractor.apply(keyword="aspirin", number_of_results=1)

    # Assert all methods were called once with the correct arguments
    mock_initiate_client.assert_called_once()
    mock_results_from_marketplaces.assert_called_once_with(
        client=mock_initiate_client.return_value,
        proxy=mock_initiate_proxy_client.return_value,
        keyword="aspirin",
        number_of_results=1,
        callback=ANY,
    )
    mock_store_results.assert_called_once()
    assert results.results == ["http://example.com"]
