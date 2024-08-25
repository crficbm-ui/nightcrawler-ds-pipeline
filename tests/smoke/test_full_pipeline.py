import subprocess
import time
import re
import os


def test_full_pipeline_run():
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
    print("\nStdout generated during smoke tests:")
    print(combined_output)

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
    print(combined_output)
    pattern = (
        r"\.\/data\/output\/\d{2}-\d{2}-\d{4}_\d{2}-\d{2}-\d{2}_aspirin_defaultuser"
    )
    paths = re.findall(pattern, combined_output)

    if paths:
        output_directory = paths[0]

        # Count the number of JSON files in the output directory
        json_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]
        json_file_count = len(json_files)

        # Check if the count matches the expected number
        assert (
            json_file_count == 4
        ), f"Expected 4 JSON files, but found {json_file_count}."
    else:
        raise AssertionError("Output directory path not found in logs.")
