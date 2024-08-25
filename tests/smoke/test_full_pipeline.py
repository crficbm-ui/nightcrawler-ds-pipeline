import subprocess
import time
import re
import os
import logging

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def test_full_pipeline_end_to_end():
    # Start the pipeline command
    result = subprocess.Popen(
        ["python", "-m", "nightcrawler", "fullrun", "aspirin", "-n=1", "--country=CH"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Polling for the pipeline to finish
    timeout = 120  # seconds
    poll_interval = 5  # seconds
    elapsed_time = 0

    while elapsed_time < timeout:
        # Check if the process has completed
        if result.poll() is not None:
            break
        time.sleep(poll_interval)
        elapsed_time += poll_interval

    # Final output check
    stdout, stderr = result.communicate()

    # Combine stdout and stderr for log checking
    combined_output = stdout + stderr

    # print the stdout
    logger.info("\nStdout generated during smoke tests:")
    logger.info(combined_output)

    # Check that the logs contain messages of starting / completing of the tasks
    assert result.returncode == 0, f"Pipeline failed with error: {stderr}"
    assert (
        "Initializing step: SerpapiExtractor" in combined_output
    ), "SerpapiExtractor initialization not found in output."
    assert (
        "Initializing step: ZyteExtractor" in combined_output
    ), "ZyteExtractor initialization not found in output."
    assert (
        "Initializing step: DataProcessor" in combined_output
    ), "DataProcessor initialization not found in output."
    assert (
        "Run full pipeline" in combined_output
    ), "Pipeline completion message not found in output."

    # Check that the logs contain messages of starting / completing of the tasks
    logger.info(combined_output)
    pattern = (
        r"\.\/data\/output\/\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_aspirin_defaultuser"
    )
    paths = re.findall(pattern, combined_output)

    if paths:
        output_directory = paths[0]

        # Count the number of JSON files in the output directory
        json_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]
        json_file_reference = [
            "01_extract_serpapi.json",
            "02_extract_zyte.json",
            "03_process_raw.json",
            "04_process_filtered_CH.json",
        ]

        # Assert that each file in json_file_reference exists in json_files
        for reference_file in json_file_reference:
            assert (
                reference_file in json_files
            ), f"File {reference_file} is missing from json_files"
    else:
        raise AssertionError("Output directory path not found in logs.")
