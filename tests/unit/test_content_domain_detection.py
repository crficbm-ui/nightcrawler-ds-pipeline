import pytest
from unittest.mock import patch
from unittest.mock import MagicMock
import logging

from nightcrawler.base import PipelineResult, Organization
from nightcrawler.context import Context
from nightcrawler.process.s09_content_domain_detection import ContentDomainDetector
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.base import MetaData, ExtractZyteData, DomainLabels

logger = logging.getLogger(LOGGER_NAME)


@pytest.fixture
def mock_context():
    context = MagicMock(spec=Context)
    context.output_dir = "/mock/output/dir"
    context.processing_filename_content_domain_detection = (
        "test_content_domain_detection.json"
    )
    return context


@pytest.fixture
def content_domain_detector(mock_context):
    mock_endpoint_api = MagicMock()
    mock_endpoint_api.call_api.return_value = {
        "response": {"prediction": {"label": "LABEL_1", "score": 0.65}},
        "seconds_taken": 0.11,
    }

    with patch.object(
        ContentDomainDetector, "_api_initialization", return_value=mock_endpoint_api
    ):
        return ContentDomainDetector(
            context=mock_context, organization=Organization.get_all()["Ages"]
        )


def test_simple(content_domain_detector):
    """Simple test to check if the ContentDomainDetector class can be instantiated."""

    assert isinstance(content_domain_detector, ContentDomainDetector)


def test_apply_content_domain_detection(content_domain_detector):
    """Test the apply_content_domain_detection method."""

    input_pipeline_result = PipelineResult(
        meta=MetaData(keyword="test_keyword", numberOfResults=1),
        results=[
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

    # Mock the `add_pipeline_steps_to_results` and `store_results` methods
    content_domain_detector.store_results = MagicMock()

    # Apply the content domain detection
    step_output = content_domain_detector.apply_step(input_pipeline_result)

    result = step_output.results[0]

    print(step_output)

    # Check that results are added and stored correctly
    content_domain_detector.store_results.assert_called_once()
    print(result)
    # Ensure that the returned result is a PipelineResult
    assert result["content_domain_label"] == DomainLabels.MEDICAL.value
