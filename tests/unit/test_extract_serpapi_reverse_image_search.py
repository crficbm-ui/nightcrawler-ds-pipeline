from unittest.mock import patch
from helpers.context import Context
from nightcrawler.base import PipelineResult, MetaData
from nightcrawler.extract.s03_reverse_image_search import GoogleReverseImageApi


# Test for _run_reverse_image_search method
@patch("helpers.api.serp_api.SerpAPI.call_serpapi")
def test_run_reverse_image_search(mock_call_serpapi):
    # Mock response from SerpAPI
    mock_response = {
        "image_results": [
            {
                "link": "https://example.com/page1",
                "thumbnail": "https://example.com/thumb1.jpg",
            },
            {
                "link": "https://example.com/page2",
                "thumbnail": "https://example.com/thumb2.jpg",
            },
        ]
    }
    mock_call_serpapi.return_value = mock_response

    context = Context()
    api = GoogleReverseImageApi(context)

    # Call the method
    result = api._run_reverse_image_search("https://example.com/sample.jpg", 1)

    # Expected result
    expected_result = [
        ("https://example.com/page1", "https://example.com/thumb1.jpg"),
        ("https://example.com/page2", "https://example.com/thumb2.jpg"),
    ]

    # Assert
    assert result == expected_result


# Test for _extract_urls_from_response method
def test_extract_urls_from_response():
    # Sample response
    response = {
        "image_results": [
            {
                "link": "https://example.com/page1",
                "thumbnail": "https://example.com/thumb1.jpg",
            },
            {
                "link": "https://example.com/page2",
                "thumbnail": "https://example.com/thumb2.jpg",
            },
        ]
    }

    # Expected result
    expected_result = [
        ("https://example.com/page1", "https://example.com/thumb1.jpg"),
        ("https://example.com/page2", "https://example.com/thumb2.jpg"),
    ]

    # Call the method
    result = GoogleReverseImageApi._extract_urls_from_response(response)

    # Assert
    assert result == expected_result


# Test for _extract_inline_urls_from_response method
@patch("logging.Logger.warning")
def test_extract_inline_urls_from_response(mock_warning):
    # Sample response with inline images
    response = {
        "inline_images": [
            {
                "source": "https://example.com/source1",
                "thumbnail": "https://example.com/thumb1.jpg",
            },
            {
                "source": "https://example.com/source2",
                "thumbnail": "https://example.com/thumb2.jpg",
            },
        ]
    }

    # Expected result
    expected_result = [
        ("https://example.com/source1", "https://example.com/thumb1.jpg"),
        ("https://example.com/source2", "https://example.com/thumb2.jpg"),
    ]

    # Create an instance of the class and call the method
    context = Context()
    api = GoogleReverseImageApi(context)
    result = api._extract_inline_urls_from_response(response)

    # Assert
    assert result == expected_result


# Test for apply method
@patch.object(GoogleReverseImageApi, "_run_reverse_image_search")
@patch.object(GoogleReverseImageApi, "store_results")
def test_apply(mock_store_results, mock_run_reverse_image_search):
    # Mock reverse image search results
    mock_run_reverse_image_search.return_value = [
        ("https://example.com/page1", "https://example.com/thumb1.jpg"),
        ("https://example.com/page2", "https://example.com/thumb2.jpg"),
    ]

    context = Context()
    context.output_dir = "/tmp/"
    api = GoogleReverseImageApi(context)

    # Input parameters
    image_urls = ["https://example.com/sample1.jpg"]
    number_of_results = 2

    # Call the apply method
    result = api.apply(
        image_urls=image_urls,
        number_of_results=number_of_results
    )

    # Assert the type of result and check that the number of results match
    assert isinstance(result, PipelineResult)
    assert result.meta.numberOfResultsAfterStage == number_of_results
