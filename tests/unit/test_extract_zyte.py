import pytest
from unittest.mock import MagicMock, patch
from helpers.api.zyte_api import ZyteAPI
from nightcrawler.extract.zyte import ZyteExtractor


@pytest.fixture
def zyte_extractor():
    context = MagicMock()
    context.zyte_filename = "dummy_zyte_filename.json"
    return ZyteExtractor(context)


def test_initiate_client(zyte_extractor):
    client, api_config = zyte_extractor.initiate_client()
    assert isinstance(client, ZyteAPI)
    assert isinstance(api_config, dict)


@patch.object(ZyteAPI, "call_api")
def test_retrieve_response(mock_call_api, zyte_extractor):
    mock_call_api.return_value = {"product": {"name": "Product Name"}}
    responses = zyte_extractor.retrieve_response(
        ZyteAPI(), ["http://example.com/product1", "http://example.com/product2"], {}
    )
    assert len(responses) == 2
    mock_call_api.assert_any_call("http://example.com/product1", {})
    mock_call_api.assert_any_call("http://example.com/product2", {})
    assert responses == [
        {"product": {"name": "Product Name"}},
        {"product": {"name": "Product Name"}},
    ]


@patch.object(ZyteAPI, "call_api")
@patch("nightcrawler.extract.zyte.logger")
def test_retrieve_response_with_failure(mock_logger, mock_call_api, zyte_extractor):
    mock_call_api.side_effect = [None, {"product": {"name": "Product Name"}}]
    responses = zyte_extractor.retrieve_response(
        ZyteAPI(), ["http://example.com/product1", "http://example.com/product2"], {}
    )
    assert len(responses) == 1
    mock_logger.error.assert_called_once_with(
        "Failed to collect product from http://example.com/product1"
    )
    assert responses == [{"product": {"name": "Product Name"}}]


def test_structure_results(zyte_extractor):
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
    structured_results = zyte_extractor.structure_results(responses)
    assert structured_results == [
        {
            "url": "http://example.com/product1",
            "price": "100USD",
            "title": "Product 1",
            "full_description": "Description 1",
            "seconds_taken": "2",
        }
    ]


@patch("nightcrawler.extract.zyte.write_json")
def test_store_results(mock_write_json, zyte_extractor):
    zyte_extractor.store_results([{"url": "http://example.com/product1"}], "/tmp")
    mock_write_json.assert_called_once_with(
        "/tmp", "dummy_zyte_filename.json", [{"url": "http://example.com/product1"}]
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
):
    # Ensure the mock for initiate_client returns a ZyteAPI instance and a config dict
    mock_initiate_client.return_value = (MagicMock(spec=ZyteAPI), {})

    # Mock the response from the retrieve_response method
    mock_retrieve_response.return_value = [{"product": {"name": "Product Name"}}]

    # Mock the response from the structure_results method
    mock_structure_results.return_value = [{"url": "http://example.com/product1"}]

    # Call the apply method
    results = zyte_extractor.apply(["http://example.com/product1"], "/tmp")

    # Assertions to ensure that each step was called correctly
    mock_initiate_client.assert_called_once()
    mock_retrieve_response.assert_called_once_with(
        mock_initiate_client.return_value[0], ["http://example.com/product1"], {}
    )
    mock_structure_results.assert_called_once_with(
        [{"product": {"name": "Product Name"}}]
    )
    mock_store_results.assert_called_once_with(
        [{"url": "http://example.com/product1"}], "/tmp"
    )

    # Final assertion to check the returned results
    assert results == [{"url": "http://example.com/product1"}]
