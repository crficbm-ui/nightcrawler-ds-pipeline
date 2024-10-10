import logging

from typing import List
from nightcrawler.base import DeliveryPolicyData, PipelineResult, BaseStep

from helpers import LOGGER_NAME
from helpers.api.llm_apis import MistralAPI
from helpers.api.zyte_api import ZyteAPI

from helpers.context import Context


from helpers.settings import Settings

from helpers import utils, utils_io, utils_strings

import pandas as pd
import time

logger = logging.getLogger(LOGGER_NAME)

import abc
import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


# base country filterer
class BaseCountryFilterer(abc.ABC):
    """Base class for country filterers."""

    RESULT_POSITIVE = +1
    RESULT_UNKNOWN = 0
    RESULT_NEGATIVE = -1

    def __init__(
        self,
        name: str,
        country: str | None = None,
        config: dict | None = None,
        config_filterers: dict | None = None,
    ) -> None:
        super().__init__()

        self.name = name

        if config:
            self.config = config
        else:
            self.config = {}
        
        if country and config_filterers:
            if name == "known_domains":
                self.setting = utils_io.load_setting(path_settings=config.get("PATH_CURRENT_PROJECT_SETTINGS"), country=country, file_name=name)
            else:
                self.setting = config_filterers.get(name)
        else:
            self.setting = {}

    @abc.abstractmethod
    def filter_page(self, **page: str) -> int:
        """Filter page.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """

        raise NotImplementedError

    def perform_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform filtering.

        Args:
            df (pd.DataFrame): dataFrame to filter.

        Returns:
            pd.DataFrame: filtered dataFrame.
        """

        tqdm.tqdm.pandas(desc=f"Filtering with {self.name}...", leave=False)

        # Check that the index of df is unique
        assert df.index.is_unique, "Index of df must be unique"

        # Create a list to store rows' indexes and labels
        list_pages_labeled = []

        # Recover known_domains filterer index if known_domains filterer is used
        self.index_known_domains_filterer = self.get_index_known_domains_filterer()

        def pseudo_filter_page(row):
            return row.name, self.filter_page(
                **row.dropna().to_dict()
            )  # keep .dropna()?

        # Use ThreadPoolExecutor to call zyte api in parallel for the shipping policy step
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(pseudo_filter_page, row) for _, row in df.iterrows()
            ]

            with tqdm.tqdm(total=len(futures)) as pbar:
                for future in as_completed(futures):
                    # Get page_labeled and add the index
                    row_index, page_labeled = future.result()
                    page_labeled["index"] = row_index

                    # Add it to the list
                    list_pages_labeled.append(page_labeled)

                    # If the setting is set to save new classified domains
                    if self.config["SAVE_NEW_CLASSIFIED_DOMAINS"]:
                        # Add the domain to known domains if known_domains filterer is used and domain is not already in known_domains (filterer label != "known_domains")
                        if (
                            self.index_known_domains_filterer is not None
                        ) & (page_labeled["filterer_name"] != "known_domains"):
                            
                            # Add domain to known_domains filterer
                            try:
                                self.add_domain_to_known_domains_filterer(page_labeled=page_labeled)
                            
                            except Exception as e:
                                print(f"Error: {e}")

                    pbar.update(1)

        # If the setting is set to save new classified domains
        if self.config["SAVE_NEW_CLASSIFIED_DOMAINS"]:
            # Save new known domains
            self.save_new_known_domains()

        # Create a df with the outputs
        df_labeled = pd.DataFrame(list_pages_labeled)

        return df_labeled
    
    def get_index_known_domains_filterer(self):
        # Recover index of known_domains filterer
        list_filterer_names = self.name.split("+")
        index_known_domains_filterer = list_filterer_names.index("known_domains") if "known_domains" in list_filterer_names else None

        return index_known_domains_filterer

    def add_domain_to_known_domains_filterer(self, page_labeled):
        # Recover label from page_labeled
        label, domain = page_labeled["RESULT"], page_labeled["domain"]

        # Filter page_labeled with relevant keys
        page_labeled_filtered = filter_dict_keys(original_dict=page_labeled, keys_to_save=self.config["KEYS_TO_SAVE"])

        # Extract known_domains_filterer
        known_domains_filterer = self.filterers[self.index_known_domains_filterer]

        # Add domain labeled to dict
        if label == 1:
            known_domains_filterer.domains_pos[domain] = page_labeled_filtered

        elif label == 0:
            known_domains_filterer.domains_unknwn[domain] = page_labeled_filtered
        
        elif label == -1:
            known_domains_filterer.domains_neg[domain] = page_labeled_filtered

        # Update self.filterers
        self.filterers[self.index_known_domains_filterer] = known_domains_filterer

    def save_new_known_domains(self):
        # Extract known_domains_filterer
        known_domains_filterer = self.filterers[self.index_known_domains_filterer]
        
        dict_known_domains = {"domains_pos": known_domains_filterer.domains_pos,
                              "domains_unknwn": known_domains_filterer.domains_unknwn,
                              "domains_neg": known_domains_filterer.domains_neg}
        
        _ = utils_io.save_and_load_setting(setting=dict_known_domains, 
                                           path_settings=known_domains_filterer.path_settings, 
                                           country=known_domains_filterer.country, 
                                           file_name=known_domains_filterer.name)


def filter_dict_keys(original_dict, keys_to_save):
    # Create the nested dictionary including the keys to save
    # Not a problem if the key does not exist in the original dictionary
    dict_filtered = {k: v for k, v in original_dict.items() if k in keys_to_save}

    return dict_filtered




# Master country filterer
class MasterCountryFilterer(BaseCountryFilterer):
    """Master country filterer."""

    def __init__(
        self,
        filterer_name: str,
        country: str | None = None,
        config: dict | None = None,
        config_filterers: dict | None = None,
        zyte_client_product_page: object | None = None,
        zyte_client_policy_page: object | None = None,
        mistral_client: object | None = None,
        **setting,
    ) -> None:
        super().__init__(name=filterer_name, config=config)

        self.filterers: list[BaseCountryFilterer] = []
        for filterer_name in filterer_name.split("+"):
            match filterer_name:
                case "known_domains":
                    self.filterers.append(
                        KnownDomainsFilterer(
                            **setting, config=config, config_filterers=config_filterers, country=country
                        )
                    )
                case "url":
                    self.filterers.append(
                        UrlCountryFilterer(
                            **setting, config_filterers=config_filterers, country=country
                        )
                    )
                case "shipping_policy":
                    self.filterers.append(
                        ShippingPolicyFilterer(
                            **setting, config_filterers=config_filterers, country=country, zyte_api_product_page=zyte_client_product_page, 
                            zyte_api_policy_page=zyte_client_policy_page, mistral_api=mistral_client
                        )
                    )
                case _:
                    raise ValueError(f"Unknown filterer: {filterer_name}")

    def filter_page(self, **page: str) -> int:
        """Filter page with Master.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """

        for filterer in self.filterers:
            page = filterer.filter_page(**page)
            if "RESULT" in page:
                page["filterer_name"] = filterer.name
                return page

        # Enter here if no filterer has returned a result (using filterer = medicrawl alone or filterer = medicrawl + url for examples)
        return page


import logging
import urllib.parse

# Check
# import sys
# print(sys.path)

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


def process_url(url: str) -> str:
    """Process url."""

    return url.lower()


def parse_url(url: str) -> urllib.parse.ParseResult:
    """Parse url."""

    return urllib.parse.urlparse(url)


def extract_domain(url_parsed: urllib.parse.ParseResult) -> str:
    """Extract domain from url."""

    return url_parsed.hostname or url_parsed.netloc


# Known domains filterer
class KnownDomainsFilterer(BaseCountryFilterer):
    """Known domains filterer."""

    def __init__(
        self,
        *,
        domains_pos: list[str] | None = None,
        domains_unknwn: list[str] | None = None,
        domains_neg: list[str] | None = None,
        country: str | None = None,
        config: dict | None = None,
        config_filterers: dict | None = None,
    ) -> None:
        super().__init__(
            name="known_domains", config=config, config_filterers=config_filterers, country=country
        )
        logger.error(self.setting)
        # Known domains
        self.domains_pos = domains_pos or self.setting.get("domains_pos") # or []
        self.domains_unknwn = domains_unknwn or self.setting.get("domains_unknwn") # or []
        self.domains_neg = domains_neg or self.setting.get("domains_neg") # or []

        # Keep these variables to save new classified domains
        self.path_settings = config.get("PATH_CURRENT_PROJECT_SETTINGS")
        self.country = country

    def filter_page(self, **page: str) -> int:
        """Filter page with known domains.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """
        # Recover url
        url = page.get("page_url", "")

        # Process url
        url = process_url(url)

        # Parse url
        url_parsed = parse_url(url)

        # Extract domain
        domain = extract_domain(url_parsed)

        # Store domain
        page["domain"] = domain

        # Check if the domain is already known
        if domain in self.domains_pos:
            logger.info(f"Domain {domain} already classified as positive")
            page["RESULT"] = self.RESULT_POSITIVE

        elif domain in self.domains_unknwn:
            logger.info(f"Domain {domain} already classified as unknown")
            page["RESULT"] = self.RESULT_UNKNOWN

        elif domain in self.domains_neg:
            logger.info(f"Domain {domain} already classified as negative")
            page["RESULT"] = self.RESULT_NEGATIVE

        return page
    
from bs4 import BeautifulSoup

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
# Download NLTK resources (only required for the first time)
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('punkt_tab')

import logging
from urllib.parse import urlparse
import pandas as pd
import ast

from helpers import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)
STEMMER = PorterStemmer()
LANGS = stopwords.fileids()


def extract_footers_and_links(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all footer elements
    footers = soup.find_all("footer")

    # Collect all 'a' tags within footers
    footer_links = []
    for footer in footers:
        footer_links.extend(footer.find_all("a", href=True))

    # If there are 'a' tags within any footer, return them
    if footer_links:  # i.e., if footer_links is not empty
        return True, footer_links

    else:
        # Otherwise, return all 'a' tags in the whole document
        all_links = soup.find_all("a", href=True)
        return False, all_links


def remove_stop_words(tokens):
    # Load stop words for all available languages
    stop_words = set()
    for lang in LANGS:  # stopwords.fileids()
        stop_words.update(stopwords.words(lang))

    # Filter out stop words
    filtered_tokens = [token for token in tokens if token not in stop_words]

    return filtered_tokens


def stem_tokens(tokens):
    # Stem the tokens
    stemmed_tokens = [STEMMER.stem(token) for token in tokens]

    return stemmed_tokens


def process_text(text):
    # Remove leading and trailing whitespaces
    text = text.strip()

    # Convert the link to lowercase
    text = text.lower()

    # Tokenize the text
    tokens = nltk.word_tokenize(text)
    # print(f"Tokens: {tokens}")

    # Remove the stopwords
    tokens = remove_stop_words(tokens)

    # Stem the tokens
    stemmed_tokens = stem_tokens(tokens)

    # Reconstruct the processed text
    processed_link_text = " ".join(stemmed_tokens)

    return processed_link_text


def get_domain(url):
    # Parse url
    parsed_url = urlparse(url)

    # Recover hostname
    domain = parsed_url.hostname  # .netloc

    return domain


def process_link_href(link_href, url_domain):
    # Recover the domain of the link
    url_domain_link_href = get_domain(url=link_href)

    # If url_domain_link_href is not None, then link_href is a full url (e.g., https://domain/shipping_policy) with possibly another domain than the one of the page
    if url_domain_link_href:
        return link_href

    # If link_href is a relative url (e.g., /shipping_policy), then we need to add the https and the domain of the page to it
    else:
        processed_link_href = "https://" + url_domain + link_href

    return processed_link_href


def get_df_page_links(page_url, page_hmtl_content):
    # Extract domain
    url_domain = get_domain(url=page_url)

    # Extract links from html content
    footer_agenty_bool, links = extract_footers_and_links(
        html_content=page_hmtl_content
    )

    # If links is an empty list, links_href and links_text will be empty lists
    links_href, links_text = [link["href"] for link in links], [
        link.get_text() for link in links
    ]

    # Create dataframe with 1 row = 1 link
    df_page_links = pd.DataFrame({"link_href": links_href, "link_text": links_text})
    df_page_links["processed_link_href"] = df_page_links["link_href"].apply(
        lambda link_href: process_link_href(link_href, url_domain)
    )
    df_page_links["processed_link_text"] = df_page_links["link_text"].apply(
        lambda link_text: process_text(link_text)
    )
    df_page_links["page_url"] = page_url
    df_page_links["footnote_link"] = footer_agenty_bool

    return df_page_links


def get_keywords_classification(df_page_links, keywords):
    # Check if the keyword model find a shipping policy link in the page
    df_page_links["keywords_classification"] = (
        df_page_links["processed_link_text"]
        .apply(
            lambda processed_link_text: utils_strings.check_string_contains_any_substring(
                string=str(processed_link_text), substrings=keywords
            )
        )
        .astype(int)
    )
    return df_page_links


def get_clean_full_text_from_html(html_content):
    # Extract all text
    soup = BeautifulSoup(html_content, "html.parser")
    raw_full_text = soup.get_text()

    # Clean up the text by removing superfluous spaces and unnecessary new lines
    lines = [line.strip() for line in raw_full_text.splitlines() if line.strip()]
    cleaned_full_text = "\n".join(lines)
    # cleaned_full_text = ' '.join(lines)

    return cleaned_full_text


def process_llm_response_content(llm_response_content, country):
    # Convert the string to a dictionary
    llm_response_content = (
        llm_response_content.replace("```json\n", "")
        .replace("\n```", "")
        .replace("\\", "")
    )
    dict_response_content = ast.literal_eval(llm_response_content)

    # Recover the answer and return it
    llm_response_answer = dict_response_content.get(
        f"is_shipping_{country}_answer", ""
    )
    print(f"LLM response answer: {llm_response_answer}")

    # Check llm justification
    llm_response_justification = dict_response_content.get(
        f"is_shipping_{country}_justification", ""
    )
    print(f"LLM response justification: {llm_response_justification}")

    return dict_response_content, llm_response_answer, llm_response_justification


# Shipping Policy filterer
class ShippingPolicyFilterer(BaseCountryFilterer):
    """Shipping Policy filterer."""

    def __init__(
        self,
        *,
        country: str | None = None,
        config_filterers: dict | None = None,
        keywords_shipping_policy: list[str] | None = None,
        urls_domains_shipping_pos: list[str] | None = None,
        urls_domains_shipping_unknwn: list[str] | None = None,
        urls_domains_shipping_neg: list[str] | None = None,
        llm_api_config: dict | None = None,
        llm_api_prompt: str | None = None,
        zyte_api_product_page_config: dict | None = None,
        zyte_api_product_page: object | None = None,
        zyte_api_policy_page_config: dict | None = None,
        zyte_api_policy_page: object | None = None,
        mistral_api: object | None = None
    ) -> None:
        super().__init__(
            name="shipping_policy", config_filterers=config_filterers, country=country
        )

        # Keywords
        self.keywords_shipping_policy = (
            keywords_shipping_policy
            or self.setting.get("keywords_shipping_policy")
            or []
        )

        # Url domains already classified
        self.urls_domains_shipping_pos = (
            urls_domains_shipping_pos
            or self.setting.get("urls_domains_shipping_pos")
            or []
        )
        self.urls_domains_shipping_unknwn = (
            urls_domains_shipping_unknwn
            or self.setting.get("urls_domains_shipping_unknwn")
            or []
        )
        self.urls_domains_shipping_neg = (
            urls_domains_shipping_neg
            or self.setting.get("urls_domains_shipping_neg")
            or []
        )

        # LLM API
        self.llm_api = mistral_api # MistralAPI()
        self.llm_api_prompt = llm_api_prompt or self.setting.get("llm_api_prompt") or ""
        self.llm_api_config = llm_api_config or self.setting.get("llm_api_config") or {}

        # Product page Zyte API
        self.zyte_api_product_page = zyte_api_product_page # ZyteAPI(max_retries=1)
        self.zyte_api_product_page_config = (
            zyte_api_product_page_config or self.setting.get("zyte_api_product_page_config") or {}
        )
    
        # Policy page Zyte API
        self.zyte_api_policy_page = zyte_api_policy_page # ZyteAPI(max_retries=1)
        self.zyte_api_policy_page_config = (
            zyte_api_policy_page_config or self.setting.get("zyte_api_policy_page_config") or {}
        )

        # Country
        self.country = country

    def filter_page(self, **page: str) -> int:
        # Recover page_url
        page_url = page.get("page_url", "")

        # Extract domain
        # TODO: get_domain already in get_df_page_links
        url_domain = get_domain(url=page_url)

        # Check if the domain has already been processed
        if url_domain in self.urls_domains_shipping_pos:
            logger.info(f"Domain {url_domain} labeled by hand as positive")
            label_justif = "Domain labeled by hand as positive"
            page["RESULT"], page["label_justif"] = self.RESULT_POSITIVE, label_justif
            return page

        elif url_domain in self.urls_domains_shipping_unknwn:
            logger.info(f"Domain {url_domain} labeled by hand as unknown")
            label_justif = "Domain labeled by hand as unknown"
            page["RESULT"], page["label_justif"] = self.RESULT_UNKNOWN, label_justif
            return page

        elif url_domain in self.urls_domains_shipping_neg:
            logger.info(f"Domain {url_domain} labeled by hand as negative")
            label_justif = "Domain labeled by hand as negative"
            page["RESULT"], page["label_justif"] = self.RESULT_NEGATIVE, label_justif
            return page

        # Define variables for Zyte API
        zyte_api_product_page, zyte_api_product_page_config = self.zyte_api_product_page, self.zyte_api_product_page_config
        zyte_api_policy_page, zyte_api_policy_page_config = self.zyte_api_policy_page, self.zyte_api_policy_page_config

        # Extract html content of the page
        try:
            logger.info(
                f"Extracting html content of \nthe page {page_url} \nwith domain {url_domain}"
            )
            dict_zyte_api_response_page_url = zyte_api_product_page.call_api(
                prompt=page_url, config=zyte_api_product_page_config, force_refresh=False
            )
        except Exception as e:
            # logger.info(label_justif)
            logger.info(e)
            label_justif = "All Zyte API call attempts failed for the page"
            page["RESULT"], page["label_justif"] = self.RESULT_UNKNOWN, label_justif
            return page

        page_hmtl_content = dict_zyte_api_response_page_url.get("browserHtml", "")
        # page_hmtl_content = dict_zyte_api_response_page_url.get("httpResponseBody", "")
        # page_hmtl_content = extract_html_content_with_zyte(url=page_url, url_type="product_page", zyte_api_key=ZYTE_API_KEY)
        if not page_hmtl_content:
            logger.info(
                f"Html content of \nthe page{page_url} \nwith domain {url_domain} extracted but empty"
            )
            page["RESULT"], page["label_justif"] = (
                self.RESULT_UNKNOWN,
                "Html content of page extracted but empty",
            )
            return page

        # Extract the footer's links (or all the links if no footer) of the page
        df_page_links = get_df_page_links(
            page_url=page_url, page_hmtl_content=page_hmtl_content
        )

        # Check if some links have been extracted
        if df_page_links.empty:
            logger.info(
                f"No links extracted for \nthe page {page_url} \nwith domain {url_domain}"
            )
            page["RESULT"], page["label_justif"] = (
                self.RESULT_UNKNOWN,
                "No links extracted",
            )
            return page

        # Get keywords' model classification of the links
        df_page_links = get_keywords_classification(
            df_page_links=df_page_links, keywords=self.keywords_shipping_policy
        )

        # Extract the list of shipping policy urls found by the keywords' model
        list_sp_page_urls = (
            df_page_links.loc[
                df_page_links.keywords_classification == 1, "processed_link_href"
            ]
            .unique()
            .tolist()
        )

        # Check if at least one shipping policy page url was found
        if list_sp_page_urls:
            if len(list_sp_page_urls) > 1:
                logger.info(
                    f"""Multiple shipping policy page urls found for
                            the page {page_url}
                            with domain {url_domain}
                            list of shipping policy page: {list_sp_page_urls}
                            """
                )

            # Store shipping policy pages found
            # page["urls_shipping_policy_page_found"] = list_sp_page_urls

        else:
            logger.info(
                f"No shipping policy page found with keywords model for \nthe page {page_url} \nwith domain {url_domain}"
            )
            page["RESULT"], page["label_justif"] = (
                self.RESULT_UNKNOWN,
                "No shipping policy page found with keywords model",
            )
            return page

        # Init urls_shipping_policy_page_found_analysis
        page["urls_shipping_policy_page_found_analysis"] = {}

        # Iterate over sp_page_url
        for sp_page_url in list_sp_page_urls:
            # Extract html content of the shipping policy page
            try:
                dict_zyte_api_response_sp_page_url = zyte_api_policy_page.call_api(
                    prompt=sp_page_url, config=zyte_api_policy_page_config, force_refresh=False
                )
            except Exception as e:
                logger.info(e)
                # Update urls_shipping_policy_page_found_analysis
                page["urls_shipping_policy_page_found_analysis"].update(
                    {sp_page_url: "All Zyte API call attempts failed"}
                )
                continue

            sp_page_hmtl_content = dict_zyte_api_response_sp_page_url.get(
                "browserHtml", ""
            )
            # sp_page_hmtl_content = dict_zyte_api_response_sp_page_url.get(
            #     "httpResponseBody", ""
            # )
            if not sp_page_hmtl_content:
                logger.info(
                    f"Html content of \nthe shipping policy page {sp_page_url} \nwith domain {url_domain} \nextracted but empty"
                )
                # Update urls_shipping_policy_page_found_analysis
                page["urls_shipping_policy_page_found_analysis"].update(
                    {sp_page_url: "Html content extracted but empty"}
                )
                continue

            # Get and clean the text of the shipping policy page
            sp_page_cleaned_full_text = get_clean_full_text_from_html(
                html_content=sp_page_hmtl_content
            )

            # Define variables for LLM API
            llm_api, llm_api_prompt, llm_api_config = (
                self.llm_api,
                self.llm_api_prompt,
                self.llm_api_config,
            )

            # Adding the text of the shipping policy page at the end of the prompt
            llm_api_prompt = llm_api_prompt + sp_page_cleaned_full_text

            # Call the LLM API and recover the response
            try:
                llm_response = llm_api.call_api(
                    prompt=llm_api_prompt, config=llm_api_config, force_refresh=False
                )
                llm_response_content = llm_response.get("content", "")
            except Exception as e:
                logger.info(e)
                # Update urls_shipping_policy_page_found_analysis
                page["urls_shipping_policy_page_found_analysis"].update(
                    {sp_page_url: "All LLM API call attempts failed"}
                )
                continue

            # Process the response returned by the LLM
            try:
                # Process the LLM response content
                dict_response_content, llm_response_answer, llm_response_justification = process_llm_response_content(
                    llm_response_content=llm_response_content, 
                    country=self.country
                )  # yes/unknown/no

            except Exception:
                logger.info(
                    f"""Unexpected response format from llm: {llm_response_content}
                                    for the page {page_url}
                                    with domain {url_domain}
                                    with shipping policy page {sp_page_url}"""
                )
                # Update urls_shipping_policy_page_found_analysis
                page["urls_shipping_policy_page_found_analysis"].update(
                    {sp_page_url: "Unexpected response format from llm"}
                )
                continue

            # Extract the label returned by the LLM
            try:
                # Return the label of the page
                if llm_response_answer == "yes":
                    # Label justification
                    label_justif = llm_response_justification # "LLM response: yes"

                    # Update urls_shipping_policy_page_found_analysis
                    page["urls_shipping_policy_page_found_analysis"].update(
                        {sp_page_url: dict_response_content}
                    )

                    # Return results
                    (
                        page["RESULT"],
                        page["label_justif"],
                        page["url_shipping_policy_page_kept"],
                    ) = (self.RESULT_POSITIVE, label_justif, sp_page_url)

                    return page

                elif llm_response_answer == "not_clear":  # unknown
                    # Update urls_shipping_policy_page_found_analysis
                    page["urls_shipping_policy_page_found_analysis"].update(
                        {sp_page_url: dict_response_content}
                    )
                    continue

                elif llm_response_answer == "no":
                    # Label justification
                    label_justif = llm_response_justification # "LLM response: no"

                    # Update urls_shipping_policy_page_found_analysis
                    page["urls_shipping_policy_page_found_analysis"].update(
                        {sp_page_url: dict_response_content}
                    )

                    # Return results
                    (
                        page["RESULT"],
                        page["label_justif"],
                        page["url_shipping_policy_page_kept"],
                    ) = (self.RESULT_NEGATIVE, label_justif, sp_page_url)

                    return page

                else:
                    raise Exception(
                        f"""Unexpected value in llm response format: {llm_response_content}
                                    for the page {page_url}
                                    with domain {url_domain}
                                    with shipping policy page {sp_page_url}"""
                    )

            except Exception as e:
                logger.info(e)
                # Update urls_shipping_policy_page_found_analysis
                page["urls_shipping_policy_page_found_analysis"].update(
                    {sp_page_url: "Unexpected value in llm response format"}
                )
                continue

        # If at least one shipping policy page was extracted with LLM not_clear label
        # if any("not_clear" == value for dict_response_content in page["urls_shipping_policy_page_found_analysis"].values() for value in dict_response_content.values()):
        if any(isinstance(dict_response_content, dict) and "not_clear" in dict_response_content.values() for dict_response_content in page["urls_shipping_policy_page_found_analysis"].values()):
            # Update page["url_shipping_policy_page_kept"]? with which shipping policy page if multiples?
            page["RESULT"], page["label_justif"] = (
                self.RESULT_UNKNOWN,
                "LLM response: not_clear",
            )
            return page

        # If no shipping policy page was extracted or all LLM api calls failed or a mix of the 2
        page["RESULT"], page["label_justif"] = (
            self.RESULT_UNKNOWN,
            "Zyte and or LLM api calls failed for all shipping policy pages found",
        )

        return page

import re
import urllib.parse


def process_url(url: str) -> str:
    """Process url."""

    return url.lower()


def parse_url(url: str) -> urllib.parse.ParseResult:
    """Parse url."""

    return urllib.parse.urlparse(url)


def extract_domain(url_parsed: urllib.parse.ParseResult) -> str:
    """Extract domain from url."""

    return url_parsed.hostname or url_parsed.netloc


def extract_top_level_domain(domain: str) -> str:
    """Extract top-level domain from domain."""

    return domain.split(".")[-1]


def extract_sub_level_domains(domain: str) -> list[str]:
    """Extract sub-level domains from domain."""

    return domain.split(".")[:-1]


def extract_path_directories(url_parsed: urllib.parse.ParseResult) -> list[str]:
    """Extract path directories from url."""

    return url_parsed.path.split("/")


def extract_query_values(url_parsed: urllib.parse.ParseResult) -> list[str]:
    """Extract query values from url."""

    return [
        subvalue
        for values in urllib.parse.parse_qs(url_parsed.query).values()
        for value in values
        for subvalue in re.split(r"\W+", value)
    ]


# Url country filterer
class UrlCountryFilterer(BaseCountryFilterer):
    """Url country filterer."""

    def __init__(
        self,
        *,
        countries: list[str] | None = None,
        top_level_domains: list[str] | None = None,
        sub_level_domains: list[str] | None = None,
        languages: list[str] | None = None,
        currencies: list[str] | None = None,
        country: str | None = None,
        config_filterers: dict | None = None,
    ) -> None:
        super().__init__(name="url", config_filterers=config_filterers, country=country)

        self.countries = countries or self.setting.get("countries") or []
        self.top_level_domains = (
            top_level_domains or self.setting.get("top_level_domains") or []
        )
        self.sub_level_domains = (
            sub_level_domains or self.setting.get("sub_level_domains") or []
        )
        self.languages = languages or self.setting.get("languages") or []
        self.currencies = currencies or self.setting.get("currencies") or []

    def filter_page(self, **page: str) -> int:
        """Filter page with Url.

        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """
        # Recover url
        url = page.get("page_url", "")

        # Process url
        url = process_url(url)

        # Parse url
        url_parsed = parse_url(url)

        # Extract domain
        domain = extract_domain(url_parsed)

        # Check top-level domain
        top_level_domain = extract_top_level_domain(domain)
        # print(f"Top level domain: {top_level_domain}")

        if utils_strings.check_string_equals_any_substring(
            top_level_domain, self.top_level_domains
        ):
            page["RESULT"] = self.RESULT_POSITIVE
            # page["REASON"] = "top_level_domain"

        # Check sub-level domains
        sub_level_domains = extract_sub_level_domains(domain)
        # print(f"Sub level domain: {sub_level_domains}")

        if utils_strings.check_any_string_equals_any_substring(
            sub_level_domains, self.top_level_domains + self.sub_level_domains
        ):
            page["RESULT"] = self.RESULT_POSITIVE
            # page["REASON"] = "sub_level_domains"

        # Check path directories
        path_directories = extract_path_directories(url_parsed)
        # print(f"Path directories: {path_directories}")

        if utils_strings.check_any_string_equals_any_substring(
            path_directories, self.countries + self.languages + self.currencies
        ):
            page["RESULT"] = self.RESULT_POSITIVE
            # page["REASON"] = "path_directories"

        # Check query parameters
        query_values = extract_query_values(url_parsed)
        # print(f"Queries: {query_values}")

        if utils_strings.check_any_string_equals_any_substring(
            query_values, self.countries + self.languages + self.currencies
        ):
            page["RESULT"] = self.RESULT_POSITIVE
            # page["REASON"] = "query_values"

        return page
    
class DeliveryPolicyDetector(BaseStep):
    """Implementation of the delivery policy detection (step 5)"""

    _entity_name: str = __qualname__


    SETTINGS = Settings().country_filtering
    logger.warning(SETTINGS)
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_FILTERERS = SETTINGS.config_filterers

    def __init__(self, context: Context,*args, **kwargs):
        self.config = kwargs.get("config", self.DEFAULT_CONFIG)
        self.config_filterers = kwargs.get("config_filterers", self.DEFAULT_CONFIG_FILTERERS)
        self.zyte_client_product_page = self._setup_zyte_client_product_page()
        self.zyte_client_policy_page = self._setup_zyte_client_policy_page()
        self.mistral_client = self._setup_mistral_client()
        super().__init__(self._entity_name)
        self.context = context

    def _setup_zyte_client_product_page(self):
        return ZyteAPI()

    def _setup_zyte_client_policy_page(self):
        return ZyteAPI()

    def _setup_mistral_client(self):
        return MistralAPI()



    def get_step_results(
        self, previous_steps_results: PipelineResult
    ) -> List[DeliveryPolicyData]:
        

        dataset = pd.DataFrame({"page_url": [e.url for e in previous_steps_results.results]})

        # Instantiate filterer
        filterer = MasterCountryFilterer(
            filterer_name=self.config["FILTERER_NAME"],
            country=self.config["COUNTRY"],
            config=self.config,
            config_filterers=self.config_filterers,
            zyte_client_product_page=self.zyte_client_product_page,
            zyte_client_policy_page=self.zyte_client_policy_page,
            mistral_client=self.mistral_client
        )

        # Perform filtering
        time_start = time.time()
        dataset = filterer.perform_filtering(dataset)
        time_end = time.time()

        # Compute time elapsed
        dataset["time_elapsed"] = time_end - time_start

        # Transform dataset to a dictionary
        dataset_to_dict = dataset.to_dict(orient="records") # dict, list, records
        
        stage_results = []
        for element in previous_steps_results.results:
            if element.url in dataset["page_url"].values:
                entry = next((item for item in dataset_to_dict if item['page_url'] == element.url), None)
                stage_results.append(
                    DeliveryPolicyData(
                        domain=entry.get("domain"),
                        result=entry.get("result"),
                        filtererName=entry.get("filterer_name"),
                        **element
                    )
                )

        return stage_results



    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic

        results = self.get_step_results(previous_step_results)


        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        pipeline_results = self.add_pipeline_steps_to_results(
            currentStepResults=results, pipelineResults=previous_step_results
        )

        self.store_results(
            pipeline_results,
            self.context.output_dir,
            self.context.processing_filename_delivery_policy,
        )
        return pipeline_results
