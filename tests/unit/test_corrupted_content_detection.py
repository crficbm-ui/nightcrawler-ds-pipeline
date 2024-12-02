import pytest
from unittest.mock import patch
from unittest.mock import MagicMock
import logging

from nightcrawler.base import PipelineResult, Organization
from nightcrawler.context import Context
from nightcrawler.process.s08_corrupted_content_detection import (
    CorruptedContentDetector,
)
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.base import MetaData, ExtractZyteData

logger = logging.getLogger(LOGGER_NAME)


@pytest.fixture
def mock_context():
    context = MagicMock(spec=Context)
    context.output_dir = "/mock/output/dir"
    context.processing_filename_corrupted_content_detection = (
        "test_corrupted_content_detection.json"
    )
    return context


@pytest.fixture
def corrupted_content_detector(mock_context):
    mock_endpoint_api = MagicMock()
    mock_endpoint_api.call_api.return_value = {
        "response": {"prediction": {"label": "LABEL_1", "score": 0.65}},
        "seconds_taken": 0.11,
    }

    with patch.object(
        CorruptedContentDetector, "_api_initialization", return_value=mock_endpoint_api
    ):
        return CorruptedContentDetector(
            context=mock_context, organization=Organization.get_all()["Ages"]
        )


def test_simple(corrupted_content_detector):
    """Simple test to check if the CorruptedContentDetector class can be instantiated."""

    assert isinstance(corrupted_content_detector, CorruptedContentDetector)


def test_apply_corrupted_content_detection(corrupted_content_detector):
    """Test the apply_corrupted_content_detection method."""

    input_pipeline_result = PipelineResult(
        meta=MetaData(keyword="test_keyword", numberOfResultsManuallySet=1),
        relevant_results=[
            ExtractZyteData(
                url="http://example.com/product1",
                price="100USD",
                title="Product 1",
                fullDescription="Description 1",
                zyteExecutionTime=2,
                offerRoot="GOOGLE",
            )
        ],
    )

    # Apply the content domain detection
    step_output = corrupted_content_detector.apply_step(input_pipeline_result)

    result = step_output.bypassed_results[0]

    print(step_output)

    # Check that results are added and stored correctly
    # Ensure that the returned result is a PipelineResult
    assert result["is_corrupted_content"]
