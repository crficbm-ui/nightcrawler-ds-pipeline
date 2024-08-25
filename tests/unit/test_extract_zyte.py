import pytest
from unittest.mock import MagicMock, patch
from helpers.api.zyte_api import ZyteAPI
from nightcrawler.extract.zyte import ZyteExtractor
from nightcrawler.base import (
    ExtractResults,
    ExtractMetaData,
    ExtractSerpapiData,
)


@pytest.fixture
def zyte_extractor():
    context = MagicMock()
    context.zyte_filename = "dummy_zyte_filename.json"
    return ZyteExtractor(context)


@pytest.fixture
def serpapi_report():
    return ExtractResults(
        meta=ExtractMetaData(
            keyword="test_keyword", numberOfResults=2, fullOutput=False
        ),
        results=[ExtractSerpapiData(url="http://example.com/product1")],
    )


def test_initiate_client(zyte_extractor):
    client, api_config = zyte_extractor.initiate_client()
    assert isinstance(client, ZyteAPI)
    assert isinstance(api_config, dict)


@patch.object(ZyteAPI, "call_api")
def test_retrieve_response(mock_call_api, zyte_extractor, serpapi_report):
    mock_call_api.return_value = {"product": {"name": "Product Name"}}

    responses = zyte_extractor.retrieve_response(ZyteAPI(), serpapi_report, {})

    assert len(responses) == 1
    mock_call_api.assert_called_once_with("http://example.com/product1", {})
    assert responses == [{"product": {"name": "Product Name"}}]


def test_structure_results(zyte_extractor, serpapi_report):
    responses = [
        {
            "product": {
                "url": "http://example.com/product1",
                "price": "100",
                "currencyRaw": "USD",
                "name": "Product 1",
                "description": "Description 1",
            },
            "seconds_taken": 2,
        }
    ]
    structured_results = zyte_extractor.structure_results(responses, serpapi_report)
    assert structured_results["results"][0]["url"] == "http://example.com/product1"
    assert structured_results["results"][0]["title"] == "Product 1"


@patch("nightcrawler.extract.zyte.write_json")
def test_store_results(mock_write_json, zyte_extractor, serpapi_report):
    zyte_extractor.store_results(serpapi_report.to_dict(), "/tmp")
    mock_write_json.assert_called_once_with(
        "/tmp", "dummy_zyte_filename.json", serpapi_report.to_dict()
    )


@patch.object(ZyteExtractor, "store_results")
@patch.object(ZyteExtractor, "structure_results")
@patch.object(ZyteExtractor, "retrieve_response")
@patch.object(ZyteExtractor, "initiate_client")
def test_apply(
    mock_initiate_client,
    mock_retrieve_response,
    mock_structure_results,
    mock_store_results,
    zyte_extractor,
    serpapi_report,
):
    # Ensure the mock for initiate_client returns a ZyteAPI instance and a config dict
    mock_initiate_client.return_value = (MagicMock(spec=ZyteAPI), {})

    # Mock the response from the retrieve_response method
    mock_retrieve_response.return_value = [{"product": {"name": "Product Name"}}]

    # Mock the response from the structure_results method
    mock_structure_results.return_value = serpapi_report.to_dict()

    # Call the apply method
    results = zyte_extractor.apply(serpapi_report, "/tmp")

    # Assertions to ensure that each step was called correctly
    mock_initiate_client.assert_called_once()
    mock_retrieve_response.assert_called_once_with(
        mock_initiate_client.return_value[0], serpapi_report, {}
    )
    mock_structure_results.assert_called_once_with(
        [{"product": {"name": "Product Name"}}], serpapi_report
    )
    mock_store_results.assert_called_once_with(serpapi_report.to_dict(), "/tmp")

    # Final assertion to check the returned results
    assert results == serpapi_report.to_dict()
