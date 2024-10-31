import hashlib
import uuid
import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Union

from nightcrawler.helpers import LOGGER_NAME


from urllib.parse import urlparse, urlunparse, ParseResult
import re
import pandas as pd

logger = logging.getLogger(LOGGER_NAME)


def evaluate_not_na(value: str) -> bool:
    """
    Evaluates NA value

    Args:
        value (str): any expression to be evaluated

    Returns:
        bool expression
    """
    return value is not None


def read_json(target_path: str, target_file: str) -> Union[Dict[str, Any], List[Any]]:
    """
    Reads a JSON file from the specified target path and filename.

    Args:
        target_path (str): The directory path where the JSON file is located.
        target_file (str): The name of the JSON file to be read.

    Returns:
        Union[Dict[str, Any], List[Any]]: The data contained in the JSON file, which could be
            a dictionary or a list depending on the structure of the JSON.

    Raises:
        FileNotFoundError: If the JSON file does not exist at the specified path.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    filepath = os.path.join(target_path, target_file)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"No such file: '{filepath}'")

    with open(filepath, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Error decoding JSON from file {filepath}: {str(e)}", f, e.pos
            )

    return data


def write_json(
    target_path: str, target_file: str, data: Union[Dict[str, Any], List[Any]]
) -> None:
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
            logger.error(e)
            raise

    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)

    logger.info(f"Successfully saved {target_path}/{target_file}")


def create_output_dir(
    keyword: str, parent_dir: str = "./", username: str = "defaultuser"
) -> str:
    """
    Creates a directory with a unique name based on the current timestamp, keyword, and username.

    The directory name is formatted as: YYYYMMDD_HHMMSS_keyword_username. If the directory
    does not already exist, it will be created. If any error occurs during the creation of
    the directory, the exception is logged and raised.

    Args:
        parent_dir (str, optional): defines the parent directory of where the output file will be stored. Defaults to current dir
        keyword (str): The keyword to include in the directory name.
        username (str, optional): The username to include in the directory name. Defaults to "defaultuser".

    Returns:
        str: The path of the created directory.

    Raises:
        Exception: If there is an error creating the directory.
    """
    # Get the current timestamp
    today = datetime.now()
    today_ts = today.strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the directory path
    target_path = f"{parent_dir}/{today_ts}_{keyword}_{username}"

    # Create the directory if it doesn't exist
    if not os.path.exists(target_path):
        try:
            os.makedirs(target_path)
        except Exception as e:
            logger.error(e)
            raise
    else:
        raise ValueError(f"directory '{target_path}' already exists")

    return target_path


def _get_stable_hash_id(text):
    text_bytes = text.encode("utf-8")
    hasher = hashlib.sha256()
    hasher.update(text_bytes)
    hash_hex = hasher.hexdigest()
    hash_int = int(hash_hex, 16)
    return hash_int % (10**8)


def _get_uuid(*args) -> str:
    concatenated_string = "".join(str(value) for value in args)
    hashed_value = hashlib.sha1(concatenated_string.encode("utf-8")).hexdigest()
    generated_id = uuid.UUID(
        hashed_value[:32]
    )  # UUID requires a 32 character hexadecimal string

    return str(generated_id)


def _clean_short_text(text: str) -> str:
    text = text.lower()
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    text = text.replace("\t", " ")
    text = text.replace('"', "").replace("'", "")
    text = text.replace("-", " ")
    text = text.strip()
    words = [word for word in text.split(" ") if word not in ["", " "]]
    text = " ".join(words)
    return text


def count_tokens(text):
    # Split the text into tokens based on whitespace
    tokens = text.split()
    # Return the number of tokens
    return len(tokens)


def get_groupby_count_prop_cols(df, list_cols):
    df_groupby = (
        df.groupby(by=list_cols, dropna=False).size().reset_index(name="Counts")
    )
    df_groupby["Proportions"] = df_groupby["Counts"] / len(df)
    df_groupby = df_groupby.sort_values(by="Proportions", ascending=False).round(2)
    return df_groupby


def get_value_counts_col(df, col_name):
    # Get counts
    value_counts = df[col_name].value_counts()

    # Get proportions
    value_proportions = df[col_name].value_counts(normalize=True, dropna=False)  # True

    # Combine both
    df_value_counts = (
        pd.DataFrame({"Counts": value_counts, "Proportions": value_proportions})
        .sort_values(by="Proportions", ascending=False)
        .round(2)
    )

    return df_value_counts


def display_values_list_cols_each_row(df: pd.DataFrame, list_cols: list):
    for _, row in df.iterrows():
        for col in list_cols:
            print(f"{col}: {row[col]}")
        print("\n")


def get_unique_domains(df):
    print(f"Number of url: {len(df)}")

    # remove duplicate urls
    df_unique_urls = df.drop_duplicates(subset="page_url", keep="first")
    print(f"Number of unique url: {len(df_unique_urls)}")

    # recover hostname
    df_unique_urls["hostname"] = df_unique_urls["page_url"].apply(
        lambda url: urlparse(url).hostname
    )

    # remove duplicated hostname
    df_unique_domains = df_unique_urls.drop_duplicates(subset="hostname", keep="first")
    print(f"Number of unique hostname: {len(df_unique_domains)}")

    return df_unique_domains


def compare_lists(list1, list2, comp_type):
    if comp_type == "intersection":
        list_intersection = list(set(list1) & set(list2))
        print(f"Length of {comp_type} list: {len(list_intersection)}")
        return list_intersection
    elif comp_type == "list1_only":
        list_list1_only = list(set(list1) - set(list2))
        print(f"Length of {comp_type} list: {len(list_list1_only)}")
        return list_list1_only
    elif comp_type == "list2_only":
        list_list2_only = list(set(list2) - set(list1))
        print(f"Length of {comp_type} list: {len(list_list2_only)}")
        return list_list2_only
    elif comp_type == "union":
        list_union = list(set(list1) | set(list2))
        print(f"Length of {comp_type} list: {len(list_union)}")
        return list_union
    else:
        print("Error: comp_type not recognized")
        return []


def estimate_api_price(df, model_name, model_input_price, model_output_price):
    # Nb samples
    nb_samples = len(df)

    # Estimate nb total tokens input
    nb_tokens_raw_prompt = 550  # 250
    df["nb_tokens_clean_ship_page_text"] = df["clean_ship_page_text"].apply(
        lambda text: count_tokens(text)
    )
    df["nb_tokens_prompt"] = nb_tokens_raw_prompt + df["nb_tokens_clean_ship_page_text"]
    nb_total_tokens_input = df["nb_tokens_prompt"].sum()

    # Estimate nb total tokens output
    mean_nb_tokens_output = 50
    nb_total_tokens_output = mean_nb_tokens_output * nb_samples

    # Api price
    api_price = (
        nb_total_tokens_input * model_input_price
        + nb_total_tokens_output * model_output_price
    ) / 1e6
    print(f"{model_name} price: {round(api_price, 2)}")


def clean_url(url):
    # Parse the URL into its components
    parsed_url = urlparse(url)

    # Remove language extensions using regex
    cleaned_path = re.sub(r"/([a-z]{2}-[a-z]{2})/", "/", parsed_url.path)

    # Remove the query string by setting it to an empty string
    cleaned_url = ParseResult(
        scheme=parsed_url.scheme,
        netloc=parsed_url.netloc,
        path=cleaned_path,  # Cleaned path
        params=parsed_url.params,
        query="",  # Clear the query
        fragment=parsed_url.fragment,
    )

    # Reconstruct the cleaned URL
    return urlunparse(cleaned_url)


def filter_dict_keys(original_dict, keys_to_save):
    # Create the nested dictionary including the keys to save
    # Not a problem if the key does not exist in the original dictionary
    dict_filtered = {k: v for k, v in original_dict.items() if k in keys_to_save}

    return dict_filtered
