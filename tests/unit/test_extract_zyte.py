import pytest
from unittest.mock import MagicMock, patch
from helpers.api.zyte_api import ZyteAPI
from nightcrawler.extract.s02_zyte import ZyteExtractor
from nightcrawler.base import PipelineResult, ExtractZyteData, MetaData


@pytest.fixture
def zyte_extractor():
    context = MagicMock()
    context.zyte_filename = "dummy_zyte_filename.json"
    context.output_dir = "/tmp"
    return ZyteExtractor(context)


@patch.object(ZyteExtractor, "store_results")
@patch.object(ZyteExtractor, "structure_results")
@patch.object(ZyteExtractor, "retrieve_response")
@patch.object(ZyteExtractor, "initiate_client")
def test_apply_all_functions_called_once(
    mock_initiate_client,
    mock_retrieve_response,
    mock_structure_results,
    mock_store_results,
    zyte_extractor,
):
    # Create the MetaData object to replace the meta dictionary
    meta_data = MetaData(keyword="test_keyword", numberOfResults=1)
    zyte_data = [
        ExtractZyteData(
            url="http://example.com/product1",
            price="100USD",
            title="Product 1",
            fullDescription="Description 1",
            zyteExecuctionTime=2,
            offerRoot="GOOGLE",
        ).to_dict()
    ]
    pipeline_result = PipelineResult(meta=meta_data, results=zyte_data)

    # Ensure the mock for initiate_client returns a ZyteAPI instance and a config dict
    mock_initiate_client.return_value = (MagicMock(spec=ZyteAPI), {})

    # Mock the response from the retrieve_response method
    mock_retrieve_response.return_value = [{"product": {"name": "Product Name"}}]

    # Use ExtractZyteData to mock the structured results
    mock_structure_results.return_value = pipeline_result

    # Call the apply method with serpapi_results as the only argument
    results = zyte_extractor.apply(
        PipelineResult(results=[{"url": "http://example.com/product1"}], meta=meta_data)
    )

    # Assertions to ensure that each step was called correctly
    mock_initiate_client.assert_called_once()

    # Check the call to retrieve_response with the updated meta
    mock_retrieve_response.assert_called_once_with(
        mock_initiate_client.return_value[0],
        PipelineResult(
            results=[{"url": "http://example.com/product1"}], meta=meta_data
        ),
        {},
    )

    # Check the structure_results call with expected ExtractZyteData and updated MetaData
    mock_structure_results.assert_called_once_with(
        [{"product": {"name": "Product Name"}}],
        PipelineResult(
            results=[{"url": "http://example.com/product1"}], meta=meta_data
        ),
    )

    # Final assertion to check the returned results
    assert results == pipeline_result

    # Ensure MetaData has been updated correctly
    assert results.meta.numberOfResultsAfterStage == 1
