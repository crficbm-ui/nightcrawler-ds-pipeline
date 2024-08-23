from typing import Dict, List
from helpers.utils import read_json, write_json, evaluate_not_na

def filter_per_country_results(context, country: str, input_dir_name: str) -> List[Dict]:
    """
    Filters and processes results based on the specified country and writes the filtered results to a designated output
    path.

    This function reads a JSON file from a given URL path, applies filtering to retain only those items that meet the
    criteria for being sold in a given Country [Switzerland ("result_sold_CH" is True)], and writes the filtered
    results to the specified output path.

    Args:
        context (object): The context object containing paths for input and output processing.
        country (str): The country code to filter results by.
        input_dir_name (str): The dynamically constracted name of the directory containing the raw JSON files with the URLs to be processed.

    Returns:
        filtered_results (List[Dict]): filtered results based on the chosen country
    """
    #TODO: Add the KEYWORD FILTER - input is NOT ZYTE - it is keyword filter.
    raw_json_urls = read_json(f"{context.output_path}/{input_dir_name}", context.zyte_filename)
    raw_results = _add_individual_features_swiss_url(raw_json_urls)
    write_json(f"{context.output_path}/{input_dir_name}", context.processing_filename_raw, raw_results)

    if country == "CH":
        country_filtered_results = [item for item in raw_results if item["result_sold_CH"]]
        write_json(f"{context.output_path}/{input_dir_name}", context.processing_filename_filtered.replace("country",country), country_filtered_results)

    
    return country_filtered_results

def _add_individual_features_swiss_url(raw_json_urls: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Verifies for relevant features that characterize a product sold in CH and then evaluates a decision if this
    product is actually SOLD in CH.

    Args:
        raw_json_urls: Original json containing URLs with additional metadata

    Returns:
        expanded dataframe json file, now with a 'result_sold_swiss' key that determines if product is sold in the
        Swiss market.
    """
    languages = ['ch-de', '/ch/', 'swiss', '/CH/', '/fr']
    shops = ['anastore', 'ayurveda101', 'biovea', 'bodysport', 'brack', 'brain-effect', 'ebay', 'gesund-gekauft',
             'kanela', 'myfairtrade', 'nurnatur', 'nu3', 'plantavis', 'shop-apotheke',
             'herbano', 'onebioshop', 'puravita', 'sembrador', 'vitaminexpress', 'wish'
             ]
    web_extensions = ['.ch', 'ch.']
    price_swiss_francs = ['CHF', 'SFr']

    print(raw_json_urls)

    CH_processed_json = [{**url_item, "ch-de_in_url": _is_substring_in_column(url_item["url"], languages)}
                         for url_item in raw_json_urls]
    CH_processed_json = [{**url_item, "swisscompany_in_url": _is_substring_in_column(url_item["url"], shops)}
                         for url_item in CH_processed_json]
    CH_processed_json = [{**url_item, "web_extension_in_url": _is_substring_in_column(url_item["url"], web_extensions)}
                         for url_item in CH_processed_json]
    CH_processed_json = [{**url_item, "francs_in_url": _is_substring_in_column(url_item["price"], price_swiss_francs)}
                         for url_item in CH_processed_json]

    # Check on feature individual features for global evaluation
    features_to_check = ['ch-de_in_url', 'swisscompany_in_url', 'web_extension_in_url', 'francs_in_url']

    CH_processed_json = [{**url_item, "result_sold_CH": _has_at_least_one_feature(url_item, features_to_check)}
                         for url_item in CH_processed_json]

    return CH_processed_json


def _has_at_least_one_feature(item_json: Dict[str, str], features_to_check: List[str]) -> bool:
    """
    Determines whether at least one specified feature is present in the given item dictionary.

    Args:
        item_json (Dict[str, str]): A dictionary with fields corresponding to metadata per URL
        features_to_check (List[str]): A list of feature names to check within the `item_json` dictionary.

    Returns:
        bool: if the system has at least one of all mentioned features
    """
    counter = sum([1 if item_json[feature_to_check] else 0
                   for feature_to_check in features_to_check], 0)
    return counter >= 1


def _is_substring_in_column(_input: str, substrings: List[str]) -> bool:
    """
    Checks if any of the specified substrings are present in the given input string. The function returns True if at
    least one substring is found; otherwise, it returns False. The input is first checked to ensure it is not null
    or NaN before performing the substring search.

    Args:
        _input (str): The input string to search within.
        substrings (List[str]): A list of substrings to check for within the `_input`.

    Returns:
        bool: True if any substring in `substrings` is found within `_input`, False otherwise.
    """
    return evaluate_not_na(_input) and any([substring in str(_input) for substring in substrings])