import logging
import ast
import time
from typing import List
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import pandas as pd
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

from nightcrawler.base import DeliveryPolicyData, BaseShippingPolicyFilterer, PipelineResult, BaseStep
from helpers.api.llm_apis import MistralAPI
from helpers.api.zyte_api import ZyteAPI
from helpers.context import Context
from helpers.settings import Settings
from helpers import LOGGER_NAME
from helpers import utils_strings

logger = logging.getLogger(LOGGER_NAME)


# Download NLTK resources (only required for the first time)
nltk.download("punkt")
nltk.download("stopwords")
nltk.download("punkt_tab")

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
    for lang in LANGS:
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
    links_href, links_text = (
        [link["href"] for link in links],
        [link.get_text() for link in links],
    )

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
    llm_response_answer = dict_response_content.get(f"is_shipping_{country}_answer", "")

    # Check llm justification
    llm_response_justification = dict_response_content.get(
        f"is_shipping_{country}_justification", ""
    )

    return dict_response_content, llm_response_answer, llm_response_justification


# Shipping Policy filterer
class ShippingPolicyFilterer(BaseShippingPolicyFilterer):
    """Delivery Policy filterer."""

    def __init__(
        self,
        *,
        country: str | None = None,
        config: dict | None = None,
        config_filterer: dict | None = None,
        zyte_api_product_page: object | None = None,
        zyte_api_policy_page: object | None = None,
        mistral_api: object | None = None,
    ) -> None:
        super().__init__(
            name="shipping_policy", config=config, config_filterer=config_filterer, country=country,
        )

        # Country
        self.country = country

        # LLM API
        self.llm_api = mistral_api

        # Product page Zyte API
        self.zyte_api_product_page = zyte_api_product_page

        # Policy page Zyte API
        self.zyte_api_policy_page = zyte_api_policy_page

    def filter_page(self, **page: str) -> int:
        """Filter page.
        Executes the Shipping policy filter only for urls which have not yet been classified by the offline 
        filterers (i.e., known_domains and url): it extracts the product page, then identifies in its footer 
        the shipping policy page then extracts it and sends it to an LLM asking it to say if the site delivers 
        or not or does not know in the country of interest.
        Each new classified domain is added to the domain registry.
        The extraction of the product and shipping policy pages for all urls is done in parallel using the 
        Zyte API in order to go faster.
        
        Args:
            **page (str): page.

        Returns:
            int: result of filtering.
        """

        # Recover page_url
        page_url = page.get("page_url")

        # Extract domain
        url_domain = page.get("domain")

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
        zyte_api_product_page, zyte_api_product_page_config = (
            self.zyte_api_product_page,
            self.zyte_api_product_page_config,
        )
        zyte_api_policy_page, zyte_api_policy_page_config = (
            self.zyte_api_policy_page,
            self.zyte_api_policy_page_config,
        )

        # Extract html content of the page
        try:
            logger.info(
                f"Extracting html content of \nthe page {page_url} \nwith domain {url_domain}"
            )
            dict_zyte_api_response_page_url = zyte_api_product_page.call_api(
                prompt=page_url,
                config=zyte_api_product_page_config,
                force_refresh=False,
            )
        except Exception as e:
            # logger.info(label_justif)
            logger.info(e)
            label_justif = "All Zyte API call attempts failed for the page"
            page["RESULT"], page["label_justif"] = self.RESULT_UNKNOWN, label_justif
            return page

        page_hmtl_content = dict_zyte_api_response_page_url.get("browserHtml", "")
        # page_hmtl_content = dict_zyte_api_response_page_url.get("httpResponseBody", "")
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
                    prompt=sp_page_url,
                    config=zyte_api_policy_page_config,
                    force_refresh=False,
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
                (
                    dict_response_content,
                    llm_response_answer,
                    llm_response_justification,
                ) = process_llm_response_content(
                    llm_response_content=llm_response_content, country=self.country
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
                    label_justif = llm_response_justification

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
                    label_justif = llm_response_justification

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
        if any(
            isinstance(dict_response_content, dict)
            and "not_clear" in dict_response_content.values()
            for dict_response_content in page[
                "urls_shipping_policy_page_found_analysis"
            ].values()
        ):
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


class DeliveryPolicyExtractor(BaseStep):
    """Implementation of the country filterer (step 5)"""

    _entity_name: str = __qualname__

    SETTINGS = Settings().delivery_policy
    DEFAULT_CONFIG = SETTINGS.config
    DEFAULT_CONFIG_FILTERER = SETTINGS.config_filterer

    def __init__(self, context: Context, *args, **kwargs):
        self.config = kwargs.get("config", self.DEFAULT_CONFIG)
        self.config_filterer = kwargs.get(
            "config_filterer", self.DEFAULT_CONFIG_FILTERER
        )
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
        dataset = pd.DataFrame(
            {"page_url": [e.url for e in previous_steps_results.results],
             "domain": [e.domain for e in previous_steps_results.results],
             "filterer_name": [e.filtererName for e in previous_steps_results.results],
             "RESULT": [e.deliveringToCountry for e in previous_steps_results.results],}
        )

        # Instantiate filterer
        filterer = ShippingPolicyFilterer(
            country=self.config["COUNTRY"],
            config=self.config,
            config_filterer=self.config_filterer,
            zyte_api_product_page=self.zyte_client_product_page,
            zyte_api_policy_page=self.zyte_client_policy_page,
            mistral_api=self.mistral_client,
        )

        # Perform filtering
        dataset = filterer.perform_filtering(dataset)

        # Fillna for label_justif col if delivery policy extraction was used for one url
        if "label_justif" in dataset.columns:
            dataset["label_justif"] = dataset["label_justif"].fillna("")

        # Transform dataset to a dictionary
        dataset_to_dict = dataset.to_dict(orient="records") # dict, list, records

        stage_results = []
        for element in previous_steps_results.results:
            if element.url in dataset["page_url"].values:
                entry = next(
                    (
                        item
                        for item in dataset_to_dict
                        if item["page_url"] == element.url
                    ),
                    None,
                )
                stage_results.append(
                    DeliveryPolicyData(
                        domain=entry.get("domain"),
                        filtererName=entry.get("filterer_name"),
                        deliveringToCountry=entry.get("RESULT"),
                        labelJustif=entry.get("label_justif"),
                        **{key: value for key, value in element.items() if key not in ["domain", "filtererName", "deliveringToCountry"]},
                    )
                )

        return stage_results

    def apply_step(self, previous_step_results: PipelineResult) -> PipelineResult:
        # TODO implement logic
        
        time_start = time.time()
        results = self.get_step_results(previous_step_results)
        time_end = time.time()
        previous_step_results.meta.time_delivery_policy_extractor = time_end - time_start

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
