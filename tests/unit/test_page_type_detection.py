import pytest
from unittest.mock import MagicMock
from typing import List
import logging

from nightcrawler.base import PipelineResult, PageTypes
from helpers.context import Context
from nightcrawler.process.s07_page_type_detection import PageTypeDetector


@pytest.fixture
def mock_context():
    """Fixture to create a mock context object."""
    context = MagicMock(spec=Context)
    context.output_dir = "/mock/output/dir"
    context.processing_filename_page_type_detection = "test_page_type_detection.json"
    return context


@pytest.fixture
def page_type_detector(mock_context):
    """Fixture to create a PageTypeDetector instance."""
    return PageTypeDetector(context=mock_context)


@pytest.fixture
def sample_pipeline_result():
    """Fixture to create a sample PipelineResult object with mock results."""
    return PipelineResult(
        meta=MagicMock(),
        results=[
            {
                "offerRoot": "",
                "url": "",
                "zyteProbability": 0.5,
                "html": "<html></html>",
            },  # valid Zyte probability
            {
                "offerRoot": "",
                "url": "",
                "zyteProbability": 0.3,
                "html": "<html></html>",
            },  # valid Zyte probability
        ],
    )


@pytest.fixture
def invalid_pipeline_result():
    """Fixture to create an invalid PipelineResult object missing zyteProbability."""
    return PipelineResult(
        meta=MagicMock(),
        results=[
            {
                "offerRoot": "",
                "url": "",
                "html": "<html></html>",
            }  # Missing zyteProbability
        ],
    )


def test_get_pagetype_from_zyte_valid(page_type_detector, sample_pipeline_result):
    """Test the Zyte page type detection logic with valid inputs."""
    results = page_type_detector._get_pagetype_from_zyte(sample_pipeline_result)

    # Verify that the results are of type List[PageTypeData]
    assert isinstance(results, List)
    assert len(results) == 2

    # The first result should be classified as an ECOMMERCE_PRODUCT (zyteProbability > 0.4)
    assert results[0].pageType == PageTypes.ECOMMERCE_PRODUCT
    # The second result should be classified as OTHER (zyteProbability <= 0.4)
    assert results[1].pageType == PageTypes.OTHER


def test_get_pagetype_from_zyte_invalid(
    page_type_detector, invalid_pipeline_result, caplog
):
    """Test the Zyte page type detection logic with invalid inputs."""
    with caplog.at_level(logging.ERROR):
        results = page_type_detector._get_pagetype_from_zyte(invalid_pipeline_result)
        assert "Item does not contain Zyte probability" in caplog.text
        assert results[0].pageType == PageTypes.OTHER


def test_apply_step_zyte(page_type_detector, sample_pipeline_result):
    """Test the apply_step method for Zyte detection."""
    # Mock the `add_pipeline_steps_to_results` and `store_results` methods
    page_type_detector.add_pipeline_steps_to_results = MagicMock(
        return_value=sample_pipeline_result
    )
    page_type_detector.store_results = MagicMock()

    # Apply the Zyte detection
    result = page_type_detector.apply_step(
        previous_step_results=sample_pipeline_result, page_type_detection_method="zyte"
    )

    # Check that results are added and stored correctly
    page_type_detector.add_pipeline_steps_to_results.assert_called_once()
    page_type_detector.store_results.assert_called_once()

    # Ensure that the returned result is a PipelineResult
    assert isinstance(result, PipelineResult)


def test_get_pagetype_from_binary_endpoint_valid(
    page_type_detector, sample_pipeline_result
):
    """Test the binary endpoint page type detection logic with valid inputs."""
    # Mock the probability returned from the endpoint (currently hardcoded as 0.5 in the code)
    results = page_type_detector._get_pagetype_from_binary_endpoint(
        sample_pipeline_result
    )

    # Verify that the results are of type List[PageTypeData]
    assert isinstance(results, List)
    assert len(results) == 2

    # Since the probability is hardcoded as 0.5 in the method, both results should be classified as ECOMMERCE_PRODUCT
    assert results[0].pageType == PageTypes.ECOMMERCE_PRODUCT
    assert results[1].pageType == PageTypes.ECOMMERCE_PRODUCT
