import json
from helpers.utils import from_dict
from nightcrawler.base import MetaData, PipelineResult, ProcessData


def merge_pipeline_steps_results(
    previousStep: PipelineResult, currentStep: PipelineResult
) -> PipelineResult:
    updatedResults = PipelineResult(
        meta=previousStep.meta, results=previousStep.results + currentStep.results
    )

    return updatedResults


def get_object_from_file(
    dir: str, filename: str, processing_object: ProcessData
) -> PipelineResult:
    """
    Reads a JSON file, processes its content, and returns a PipelineResult object along with the output directory.
    Since this function uses the datamodel, it cannot be added to the helpers repo (where it would typically belong).

    Args:
        dir (str): The directory path where the file is located.
        filename (str): The name of the file to be read.
        processing_object (ProcessData): A class reference to be used for processing the 'results' in the JSON file.

    Returns:
        PipelineResult: The processed PipelineResult object.
    """

    dir_and_filename = f"{dir}/{filename}"
    with open(dir_and_filename, "r") as file:
        json_input = json.loads(file.read())

    # Convert the meta part to MetaData
    json_input["meta"] = from_dict(MetaData, json_input["meta"])

    # Convert each item in the results list to ProcessData
    json_input["results"] = [
        from_dict(processing_object, item) for item in json_input["results"]
    ]

    # Finally, convert the entire dictionary to a PipelineResult
    pipeline_result = from_dict(PipelineResult, json_input)

    return pipeline_result
