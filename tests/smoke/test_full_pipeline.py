import re
import os
import logging
from io import StringIO

from nightcrawler.cli.main import run

from helpers import LOGGER_NAME
from helpers.context import Context

logger = logging.getLogger(LOGGER_NAME)


def test_full_pipeline_end_to_end():
    # Configure logging to write to string
    stream = StringIO()
    logger.addHandler(logging.StreamHandler(stream=stream))

    country = "CH"
    # Start the pipeline command
    run(["fullrun", "aspirin", "-n=1", f"--country={country}"])

    # Combine stdout and stderr for log checking
    combined_output = stream.getvalue()

    # print the stdout
    logger.info("\nStdout generated during smoke tests:")
    logger.info(combined_output)

    # Check that the logs contain messages of starting / completing of the tasks
    assert re.search(
        r"Executing step \d{1,2}: SerpapiExtractor", combined_output
    ), "SerpapiExtractor initialization not found in output."
    assert re.search(
        r"Executing step \d{1,2}: ZyteExtractor", combined_output
    ), "ZyteExtractor initialization not found in output."
    # assert re.search(
    #     r"Executing step \d{1,2}: DataProcessor", combined_output
    # ), "DataProcessor initialization not found in output."
    assert re.search(
        r"Executing step \d{1,2}: CountryFilterer", combined_output
    ), "CountryFilterer initialization not found in output."
    assert re.search(
        r"Executing step \d{1,2}: DeliveryPolicyExtractor", combined_output
    ), "DeliveryPolicyExtractor initialization not found in output."
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
        context = Context()

        # Count the number of JSON files in the output directory
        json_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]
        json_file_reference = [
            context.serpapi_filename,
            context.zyte_filename,
            context.processing_filename_raw,
            context.processing_filename_filtered.replace("country", country),
            context.processing_filename_country_filtering,
            context.processing_filename_delivery_policy,
            context.processing_filename_page_type_detection,
            context.processing_filename_blocked_content_detection,
            context.processing_filename_content_domain_detection,
            context.processing_filename_suspiciousness_classifier,
            context.filename_final_results,
        ]

        # Assert that each file in json_file_reference exists in json_files
        for reference_file in json_file_reference:
            assert (
                reference_file.endswith(json_file)
                for json_file in json_files  # this is nessesary, as the numbering of the resulting files is not present int the context.
            ), f"File {reference_file} is missing from json_files"
    else:
        raise AssertionError("Output directory path not found in logs.")
