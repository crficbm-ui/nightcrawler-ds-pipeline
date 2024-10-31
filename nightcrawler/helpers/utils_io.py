import yaml
import os
import pandas as pd
import json

from . import utils_path
from nightcrawler.base import MetaData, PipelineResult, ProcessData
from typing import Type


# io
def create_directory(directory: str) -> None:
    # Check if the directory already exists
    if not os.path.exists(directory):
        # Create the directory
        os.makedirs(directory)
        print("Directory created successfully!")


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


def from_dict(data_class: Type, data: dict):
    """Recursively convert a dictionary to a dataclass, handling fields with init=False."""
    # Prepare arguments for dataclass initialization
    field_values = {}
    post_init_fields = {}

    for field in data_class.__dataclass_fields__.values():
        if field.name in data:
            value = data[field.name]
            if isinstance(value, dict) and hasattr(field.type, "__dataclass_fields__"):
                # Recursively convert dict to dataclass
                field_value = from_dict(field.type, value)
            elif isinstance(value, list) and hasattr(
                field.type.__args__[0], "__dataclass_fields__"
            ):
                # Recursively convert list of dicts to list of dataclasses
                field_value = [
                    from_dict(field.type.__args__[0], item) for item in value
                ]
            else:
                field_value = value

            if field.init:
                field_values[field.name] = field_value
            else:
                post_init_fields[field.name] = field_value
        else:
            if field.init:
                field_values[field.name] = None

    # Create an instance of the dataclass
    instance = data_class(**field_values)

    # Manually set fields that have init=False
    for key, value in post_init_fields.items():
        setattr(instance, key, value)

    return instance


# dataset


def load_dataset(path_data: str, country: str, file_name: str) -> pd.DataFrame:
    """Load dataset from file.

    Args:
        path_data (str): path to data.
        country (str): country.
        file_name (str): name of the file.

    Returns:
        pd.DataFrame: data.
    """
    path = utils_path.compose_path_dataset_file(path_data, country, file_name)
    data = pd.read_csv(path)
    return data


def save_and_load_dataset(
    data: pd.DataFrame, path_data: str, country: str, file_name: str
) -> pd.DataFrame:
    """Save dataset from file and load it.

    Args:
        data (pd.DataFrame): data to save.
        path_data (str): path to data.
        country (str): country.
        file_name (str): name of the file.

    Returns:
        pd.DataFrame: data.
    """
    # Create directory if it does not exist
    directory = os.path.join(path_data, country)
    create_directory(directory)

    # Save data
    path = utils_path.compose_path_dataset_file(path_data, country, file_name)
    data.to_csv(path, index=False)
    # data = load_dataset(path_data, country, file_name)
    data = pd.read_csv(path)
    return data


# settings


def load_setting(path_settings: str, country: str, file_name: str) -> dict:
    """Load setting from file.

    Args:
        path_settings (str): path to settings.
        country (str): country.
        file_name (str): name of the file.

    Returns:
        dict: setting.
    """
    path = utils_path.compose_path_setting_file(path_settings, country, file_name)

    try:
        with open(path, "r") as file:
            setting = yaml.safe_load(file)
            return setting

    except FileNotFoundError:
        print(f"File not found: {path}")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def save_and_load_setting(
    setting: dict, path_settings: str, country: str, file_name: str
) -> dict:
    """Save setting from a dict.

    Args:
        setting (dict): data to save.
        path_settings (str): path to settings.
        country (str): country.
        file_name (str): name of the file.

    Returns:
        dict: setting.
    """
    # Create directory if it does not exist
    directory = os.path.join(path_settings, country)
    create_directory(directory)

    # Save setting
    path = utils_path.compose_path_setting_file(path_settings, country, file_name)

    try:
        with open(path, "w") as file:
            yaml.safe_dump(setting, file)

    except Exception as e:
        print(f"An error occurred while saving file: {e}")

    # Load
    setting = load_setting(path_settings, country, file_name)

    # Return
    return setting
