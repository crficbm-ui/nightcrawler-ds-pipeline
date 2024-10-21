import argparse
from typing import List
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from nightcrawler.process.s05_country_filterer import MasterCountryFilterer
from helpers.settings import Settings
from nightcrawler.process.s06_delivery_page_detection import ShippingPolicyFilterer
from helpers.api.llm_apis import MistralAPI
from helpers.api.zyte_api import ZyteAPI
from helpers import utils_io


def load_df(path_to_data, country):
    if country == "CH":
        blob_2024_top_domain_raw = utils_io.load_dataset(path_data=path_to_data, 
                                                        country="ch/blob_2024_dataset", 
                                                        file_name="blob_2024_top_domain_raw")
        blob_2024_top_domain_raw.rename(columns={"pageUrl": "page_url"}, inplace=True)
        blob_2024_top_domain_raw["LABEL"] = (
            blob_2024_top_domain_raw["shipping_in_swiss"]
            .map({"yes": True, "no_info": False, "unknown": False, "no": False})
            .astype(bool)
        )
        df = blob_2024_top_domain_raw.copy()
        return df

    elif country == "AT":
        df_austria = utils_io.load_dataset(path_data=path_to_data, 
                                        country="at/nico_dataset", 
                                        file_name="initial_collection_regex_austria")
        df_austria.rename(columns={"url": "page_url"}, inplace=True)
        df_austria["austria_relevant"] = df_austria["austria_relevant"].astype(float)
        df_austria = df_austria.rename(columns={"austria_relevant": "LABEL"})
        df = df_austria.copy()
        return df


def get_confusion_matrix(y_true, y_pred_label, kwargs):
    fig = plt.figure()

    conf_matrix = pd.crosstab(
        index=y_true,
        columns=y_pred_label,
        rownames=["True"],
        colnames=["Predicted"],
    )

    sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues")

    plt.xlabel("Predicted")
    plt.ylabel("Ground Truth")
    plt.title("Confusion Matrix")
    plt.show()

    return conf_matrix


def evaluate_filterers_dataset_labeled(dataset_labeled, **kwargs):
    # Assess filtering
    ## initialize metrics
    metrics = {}
    ## compute duration metric
    metrics = {
        **metrics,
        # "duration": time_end - time_start,
    }
    ## compute distribution metrics
    metrics = {
        **metrics,
        "positives": dataset_labeled["RESULT"].eq(+1).sum(),
        "unknowns": dataset_labeled["RESULT"].eq(0).sum(),
        "negatives": dataset_labeled["RESULT"].eq(-1).sum(),
    }
    ## compute classification metrics
    if "LABEL" in dataset_labeled.columns:
        # ATTENTION: in "RESULT" column there will be no NaN whatever happens because of the astype(bool)
        dataset_labeled["RESULT"] = (
            dataset_labeled["RESULT"].map({+1: True, 0: False, -1: False}).astype(bool)
        )
        assert (
            dataset_labeled["RESULT"].isna().sum() == 0
        ), "There are NaN values in the RESULT column"

        metrics = {
            **metrics,
            "accuracy": accuracy_score(
                dataset_labeled["LABEL"], dataset_labeled["RESULT"]
            ),
            "precision": precision_score(
                dataset_labeled["LABEL"],
                dataset_labeled["RESULT"],
                zero_division=np.nan,
            ),
            "recall": recall_score(
                dataset_labeled["LABEL"], dataset_labeled["RESULT"]
            ),
            "f1": f1_score(
                dataset_labeled["LABEL"], dataset_labeled["RESULT"]
            ),
        }

        # Display and save confusion matrix
        _ = get_confusion_matrix(
            y_true=dataset_labeled["LABEL"],
            y_pred_label=dataset_labeled["RESULT"],
            kwargs=kwargs,
        )

    return metrics


def main(path_to_data, country):
    # Load the dataset
    df = load_df(path_to_data, country)

    # Country filtering
    ## Instantiate filterer
    SETTINGS = Settings().country_filtering
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_URL_FILTERER = SETTINGS.config_url_filterer

    config = DEFAULT_CONFIG.get(country)
    config_url_filterer = DEFAULT_CONFIG_URL_FILTERER.get(country)

    filterer = MasterCountryFilterer(
        filterer_name=config["FILTERER_NAME"],
        country=config["COUNTRY"],
        config=config,
        config_filterer=config_url_filterer
    )

    ## Perform filtering
    df_cf = filterer.perform_filtering(df)

    # Delivery policy detection
    ## Instantiate filterer
    SETTINGS = Settings().delivery_policy
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_FILTERER = SETTINGS.config_filterer

    config = DEFAULT_CONFIG.get(country)
    config_filterer = DEFAULT_CONFIG_FILTERER.get(country)
    
    filterer = ShippingPolicyFilterer(
        country=config["COUNTRY"],
        config=config,
        config_filterer=config_filterer,
        zyte_api_product_page=ZyteAPI(),
        zyte_api_policy_page=ZyteAPI(),
        mistral_api=MistralAPI(),
    )

    ## Perform filtering
    df_spf = filterer.perform_filtering(df_cf)

    # Evaluate
    metrics = evaluate_filterers_dataset_labeled(df_spf)
    print(metrics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path_to_data",
        type=str,
        default="data",
        help="Path to the data folder",
    )
    parser.add_argument(
        "--country", 
        type=str, default="CH",
        help="The country on which to evaluate country filtering and delivery policy models"
    )

    args = parser.parse_args()
    
    main(args.path_to_data, args.country)
