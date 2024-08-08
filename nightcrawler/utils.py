import os
import json
import logging
from typing import Any, Dict, List, Union


def write_json(target_path: str, target_file: str, data: Union[Dict[str, Any], List[Any]]) -> None:
    """
    Writes the provided data to a JSON file at the specified target path and filename.

    Args:
        target_path (str): The directory path where the JSON file will be saved.
        target_file (str): The name of the JSON file to be created.
        data (Union[Dict[str, Any], List[Any]]): The data to be written to the JSON file, 
            which can be a dictionary or a list.

    Raises:
        Exception: If there is an error creating the target directory.
    """
    filepath = os.path.join(target_path, target_file)
    if not os.path.exists(target_path):
        try:
            os.makedirs(target_path)
        except Exception as e:
            logging.error(e)
            raise

    with open(filepath, "w") as f:
        json.dump(data, f)
    
    logging.info(f"Successfully saved {target_path}/{target_file}")
