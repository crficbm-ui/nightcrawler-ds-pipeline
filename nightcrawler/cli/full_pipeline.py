import argparse
import logging
from typing import List
from nightcrawler.process.s05_dataprocessor import DataProcessor
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.extract.s02_enriched_keywords import KeywordEnricher
from nightcrawler.extract.s04_zyte import ZyteExtractor
from nightcrawler.extract.s03_reverse_image_search import GoogleReverseImageApi
from nightcrawler.process.s07_page_type_detection import PageTypeDetector
from nightcrawler.process.s08_blocket_content_detection import BlockedContentDetector
from nightcrawler.process.s09_content_domain_detection import ContentDomainDetector
from nightcrawler.process.s10_suspiciousness_classifier import SuspiciousnessClassifier
from nightcrawler.process.s11_result_ranker import ResultRanker
from nightcrawler.base import BaseStep

from helpers import LOGGER_NAME
from helpers.context import Context
from helpers.utils import create_output_dir
from helpers.decorators import timeit

logger = logging.getLogger(LOGGER_NAME)


def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'fullrun'.
    """
    return "fullrun"


def add_parser(
    subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]
) -> argparse.ArgumentParser:
    """
    Adds the 'fullrun' parser to the collection of subparsers for the command-line interface.

    This parser is specifically designed to handle the 'fullrun' command, which executes the entire
    data pipeline end-to-end. The subparsers collection typically contains different parsers for various
    commands that can be executed through the CLI, each corresponding to a different aspect of the pipeline
    or a distinct operation within the application.

    Args:
        subparsers (argparse._SubParsersAction): The collection of subparsers associated with the
            primary argument parser. This collection organizes the different commands available in
            the CLI, allowing the user to select and execute specific functionalities.
        parents_ (List[argparse.ArgumentParser]): A list of parent parsers that provide common
            arguments and configurations shared across multiple subparsers, ensuring consistency
            and reusability in argument parsing.

    Returns:
        argparse.ArgumentParser: The parser associated with the 'fullrun' command, equipped with
        arguments and help descriptions specific to executing the full pipeline.
    """
    parser = subparsers.add_parser(
        parser_name(),
        help="Run the full pipeline from extraction to processing",
        parents=parents_,
    )

    return parser


from nightcrawler.process.s05_country_filterer import CountryFilterer
from nightcrawler.process.s06_delivery_page_detection import DeliveryPolicyExtractor

@timeit
def apply(args: argparse.Namespace) -> None:
    """
    Applies the full pipeline, combining extraction and processing.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()

    if not args.reverse_image_search:
        # Step 0: create the results directory with searchitem = keyword
        context.output_dir = create_output_dir(args.searchitem, context.output_path)

        # Step 1 Extract URLs using Serpapi based on a searchitem (=keyword) provided by the users
        serpapi_results = SerpapiExtractor(context).apply(
            keyword=args.searchitem, number_of_results=args.number_of_results
        )

        # Step 2: Enricht query by adding additional keywords if `-e` argument was set
        if args.enrich_keyword:
            # load dataForSeo configs based on the country information, if none provided, default to CH
            country = args.country if args.country else "CH"
            api_config_for_country = context.settings.data_for_seo.api_params.get(
                country
            )
            serpapi_results = KeywordEnricher(context).apply(
                keyword=args.searchitem,
                serpapi=SerpapiExtractor(context),
                number_of_keywords=3,
                location=api_config_for_country.get("location"),
                language=api_config_for_country.get("language"),
                previous_step_results=serpapi_results,
            )
        else:
            logger.warning(
                "Skipping keyword enrichment as option `-e` was not specified"
            )
            BaseStep._step_counter += 1  # doing this, so that the the output files still match the step count specified in the README.md. However, this will lead to gaps in the numbering of the output files (3 will be missing).

    else:
        # Step 0: create the results directory with searchitem = url, so just name it 'reverse_image_search'.
        context.output_dir = create_output_dir(
            "reverse_image_search", context.output_path
        )

        # Step 3 Extract URLs using Serpapi - Perform reverse image search if image-urls were provided
        serpapi_results = GoogleReverseImageApi(context).apply(
            image_url=args.searchitem,
            number_of_results=args.number_of_results,
        )

    # Step 4: Use Zyte to process the URLs further
    zyte_results = ZyteExtractor(context).apply(serpapi_results)
    
    # Step 5: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists
    country_filtering_results = CountryFilterer(context).apply(
        previous_step_results=zyte_results
    )

    # Step 6: delivery policy filtering based on online analysis of domains public delivery information
    delivery_policy_filtering_results = DeliveryPolicyExtractor(context).apply(
        previous_step_results=country_filtering_results
    )

    # Step 7: page type filtering based on either a probability of Zyte (=default) or a custom BERT model deployed on the mutualized GPU. The pageType can be either 'ecommerce_product' or 'other'.
    page_type_filtering_results = PageTypeDetector(context).apply(
        previous_step_results=delivery_policy_filtering_results,
        page_type_detection_method=args.page_type_detection_method,
    )

    # Step 8: blocked / corrupted content detection based the prediction with a BERT model.
    blocked_content_results = BlockedContentDetector(context).apply(
        previous_step_results=page_type_filtering_results
    )

    # Step 9: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)
    content_domain_results = ContentDomainDetector(context).apply(
        previous_step_results=blocked_content_results
    )

    # Step 10: Binary classifier per organisation, whether a product is classified as suspicious or not.
    suspiscousness_results = SuspiciousnessClassifier(context).apply(
        previous_step_results=content_domain_results
    )

    # Step 11: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away.
    ResultRanker(context).apply(previous_step_results=suspiscousness_results)

    # TODO transform final_results into List[CrawlResult] for the libnightcawler lib
    # TODO store final_results as CrawlResult object to storage
