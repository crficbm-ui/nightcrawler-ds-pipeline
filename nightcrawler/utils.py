import os
import json
import logging


def write_json(target_path, target_file, data):
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
