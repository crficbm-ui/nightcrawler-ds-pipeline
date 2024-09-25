import argparse
import logging
from typing import List
from nightcrawler.utils import merge_pipeline_steps_results
from nightcrawler.process.s03_dataprocessor import DataProcessor
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.extract.s02_zyte import ZyteExtractor
from nightcrawler.extract.s01_reverse_image_search import GoogleReverseImageApi
from nightcrawler.process.s05_delivery_page_detection import DeliveryPolicyDetector
from nightcrawler.process.s06_page_type_detection import PageTypeDetector
from nightcrawler.process.s07_blocket_content_detection import BlockedContentDetector
from nightcrawler.process.s08_content_domain_detection import ContentDomainDetector
from nightcrawler.process.s09_suspiciousness_classifier import SuspiciousnessClassifier
from nightcrawler.process.s10_result_ranker import ResultRanker

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


@timeit
def apply(args: argparse.Namespace) -> None:
    """
    Applies the full pipeline, combining extraction and processing.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()

    # Step 0: create the results directory
    context.output_dir = create_output_dir(args.keyword, context.output_path)

    # Step 1a Extract URLs using Serpapi based on a keyword provided by the users
    serpapi_keyword_results = SerpapiExtractor(context).apply(
        keyword=args.keyword, number_of_results=args.number_of_results
    )

    # Step 1b  Use serpapi to perform a Google reverse image search.  if image-urls were provided
    if args.reverse_image_search:
        # Handle reverse image search
        image_urls = args.reverse_image_search
        serpapi_image_results = GoogleReverseImageApi(context).apply(
            image_urls=image_urls,
            keywords=args.keyword,
            number_of_results=args.number_of_results,
        )

        # if there is keyword and reverse image search results, we want to combine them into a new serpapi_results object. If no image results are present, we want to return only the keyword results
        serpapi_results = merge_pipeline_steps_results(
            previousStep=serpapi_image_results, currentStep=serpapi_keyword_results
        )
    else:
        serpapi_results = serpapi_keyword_results

    # Step 2: Use Zyte to retrieve structured information from each URL collected by serpapi
    zyte_results = ZyteExtractor(context).apply(serpapi_results)

    # Step 3: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists. All these depend on the --country input of the pipeline call.
    # TODO replace the manual filtering logic with Mistral call by Nicolas W.
    processor_results = DataProcessor(context).apply(
        pipeline_results=zyte_results, country=args.country
    )

    # Step 4: delivery policy filtering based on offline analysis of domains public delivery information
    delivery_policy_filtering_results = DeliveryPolicyDetector(context).apply(
        processor_results
    )

    # Step 5: page type filtering based on an offline trained model which filters pages in a multiclass categorical problem assigining one of the following classes [X, Y, Z]
    page_type_filtering_results = PageTypeDetector(context).apply(
        delivery_policy_filtering_results
    )

    # Step 6: blocked / corrupted content detection based the prediction with a BERT model.
    blocked_content_results = BlockedContentDetector(context).apply(
        page_type_filtering_results
    )

    # Step 7: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)
    content_domain_results = ContentDomainDetector(context).apply(
        blocked_content_results
    )

    # Step 8: Binary classifier per organisation, whether a product is classified as suspicious or not.
    suspiscousness_results = SuspiciousnessClassifier(context).apply(
        content_domain_results
    )

    # Step 9: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away.
    ResultRanker(context).apply(suspiscousness_results)

    # TODO transform final_results into List[CrawlResult] for the libnightcawler lib
    # TODO store final_results as CrawlResult object to storage
