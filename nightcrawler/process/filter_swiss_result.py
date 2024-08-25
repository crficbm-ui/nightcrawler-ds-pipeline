import logging
from typing import Dict, List, Any
from helpers.utils import read_json, write_json, evaluate_not_na
from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def filter_per_country_results(
    context: Any, country: str, input_dir_name: str
) -> List[Dict[str, Any]]:
    """
    Filters and processes results based on the specified country and writes the filtered results to a designated output path.

    This function reads a JSON file from a given URL path, applies filtering to retain only those items that meet the
    criteria for being sold in a given country [e.g., Switzerland ("result_sold_CH" is True)], and writes the filtered
    results to the specified output path.

    Args:
        context (Any): The context object containing paths for input and output processing.
        country (str): The country code to filter results by.
        input_dir_name (str): The dynamically constructed name of the directory containing the raw JSON files with the URLs to be processed.

    Returns:
        List[Dict[str, Any]]: Filtered results based on the chosen country.
    """
    # TODO: Add the KEYWORD FILTER - input is NOT ZYTE - it is keyword filter.

    # Read raw JSON data
    raw_json = read_json(
        f"{context.output_path}/{input_dir_name}", context.zyte_filename
    )

    # Process the list of URLs
    raw_json_urls = raw_json.get("results", []).copy()
    raw_results = _add_individual_features_swiss_url(raw_json_urls)

    # Update raw_json_urls with processed results
    for index in range(len(raw_json_urls)):
        raw_json_urls[index] = raw_results[index]

    # Update the raw_json dictionary with the modified list
    raw_json["results"] = raw_json_urls

    # Write the updated raw JSON data
    write_json(
        f"{context.output_path}/{input_dir_name}",
        context.processing_filename_raw,
        raw_json,
    )

    # Filter results based on country
    if country == "CH":
        country_filtered_results = raw_json.copy()
        filtered_results = [item for item in raw_results if item["result_sold_CH"]]
        country_filtered_results["results"] = filtered_results

        # Write the filtered results to a file
        write_json(
            f"{context.output_path}/{input_dir_name}",
            context.processing_filename_filtered.replace("country", country),
            country_filtered_results,
        )

        return country_filtered_results

    return raw_json


def _add_individual_features_swiss_url(
    raw_json_urls: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Verifies relevant features that characterize a product sold in CH and evaluates whether the product is actually sold in CH.

    Args:
        raw_json_urls (List[Dict[str, str]]): Original JSON containing URLs with additional metadata.

    Returns:
        List[Dict[str, Any]]: The processed JSON list, now with a 'result_sold_CH' key that determines if the product is sold in the Swiss market.
    """
    languages = ["ch-de", "/ch/", "swiss", "/CH/", "/fr"]
    shops = [
        "anastore",
        "ayurveda101",
        "biovea",
        "bodysport",
        "brack",
        "brain-effect",
        "ebay",
        "gesund-gekauft",
        "kanela",
        "myfairtrade",
        "nurnatur",
        "nu3",
        "plantavis",
        "shop-apotheke",
        "herbano",
        "onebioshop",
        "puravita",
        "sembrador",
        "vitaminexpress",
        "wish",
    ]
    web_extensions = [".ch", "ch."]
    price_swiss_francs = ["CHF", "SFr"]

    CH_processed_json = [
        {
            **url_item,
            "ch-de_in_url": _is_substring_in_column(url_item["url"], languages),
            "swisscompany_in_url": _is_substring_in_column(url_item["url"], shops),
            "web_extension_in_url": _is_substring_in_column(
                url_item["url"], web_extensions
            ),
            "francs_in_url": _is_substring_in_column(
                url_item.get("price", ""), price_swiss_francs
            ),
        }
        for url_item in raw_json_urls
    ]

    # Add the 'result_sold_CH' key to each item
    features_to_check = [
        "ch-de_in_url",
        "swisscompany_in_url",
        "web_extension_in_url",
        "francs_in_url",
    ]
    CH_processed_json = [
        {
            **url_item,
            "result_sold_CH": _has_at_least_one_feature(url_item, features_to_check),
        }
        for url_item in CH_processed_json
    ]

    return CH_processed_json


def _has_at_least_one_feature(
    item_json: Dict[str, Any], features_to_check: List[str]
) -> bool:
    """
    Determines whether at least one specified feature is present in the given item dictionary.

    Args:
        item_json (Dict[str, Any]): A dictionary with fields corresponding to metadata per URL.
        features_to_check (List[str]): A list of feature names to check within the `item_json` dictionary.

    Returns:
        bool: True if the item has at least one of the specified features, False otherwise.
    """
    return any(item_json.get(feature, False) for feature in features_to_check)


def _is_substring_in_column(_input: str, substrings: List[str]) -> bool:
    """
    Checks if any of the specified substrings are present in the given input string. Returns True if at least one substring is found; otherwise, returns False.

    Args:
        _input (str): The input string to search within.
        substrings (List[str]): A list of substrings to check for within the `_input`.

    Returns:
        bool: True if any substring in `substrings` is found within `_input`, False otherwise.
    """
    return evaluate_not_na(_input) and any(
        substring in _input for substring in substrings
    )
