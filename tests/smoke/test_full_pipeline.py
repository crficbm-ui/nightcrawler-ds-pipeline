import re
import os
import logging
from io import StringIO

from nightcrawler.cli.main import run

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def test_full_pipeline_end_to_end():
    # Configure logging to write to string
    stream = StringIO()
    logger.addHandler(logging.StreamHandler(stream=stream))

    # Start the pipeline command
    run(["fullrun", "aspirin", "-n=1", "--country=CH"])

    # Combine stdout and stderr for log checking
    combined_output = stream.getvalue()

    # print the stdout
    logger.info("\nStdout generated during smoke tests:")
    logger.info(combined_output)

    # Check that the logs contain messages of starting / completing of the tasks
    assert (
        "Initializing data collection: SerpapiExtractor" in combined_output
    ), "SerpapiExtractor initialization not found in output."
    assert (
        "Initializing data collection: ZyteExtractor" in combined_output
    ), "ZyteExtractor initialization not found in output."
    assert (
        "Initializing: DataProcessor" in combined_output
    ), "DataProcessor initialization not found in output."
    assert (
        "Pipeline execution finished" in combined_output
    ), "Pipeline completion message not found in output."

    # Check that the logs contain messages of starting / completing of the tasks
    pattern = r"\.\/data\/output\/\d{8}_\d{6}_aspirin_defaultuser"
    paths = re.findall(pattern, combined_output)

    if paths:
        output_directory = paths[0]

        # Count the number of JSON files in the output directory
        json_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]
        json_file_reference = ["01_extract_serpapi.json", "02_extract_zyte.json", "03_process_raw.json", "04_process_filtered_CH.json"]

        # Assert that each file in json_file_reference exists in json_files
        for reference_file in json_file_reference:
            assert reference_file in json_files, f"File {reference_file} is missing from json_files"
    else:
        raise AssertionError("Output directory path not found in logs.")
