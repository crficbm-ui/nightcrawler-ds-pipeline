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

    # Step 1a Extract URLs using Serpapi - Perform keyword search and add them to the urls variable (empty, if no reverse image search was performed)
    serpapi_keyword_results = SerpapiExtractor(context).apply(
        keyword=args.keyword, number_of_results=args.number_of_results
    )

    # Step 1b Extract URLs using Serpapi - Perform reverse image search if image-urls were provided
    if args.reverse_image_search:
        # Handle reverse image searchcl
        image_urls = args.reverse_image_search
        serpapi_image_results = GoogleReverseImageApi(context).apply(
            image_urls=image_urls,
            keywords=args.keyword,
            number_of_results=args.number_of_results,
        )

        # if there is keyword and reverse image search results, we want to combine them into a new serapi_results object. If no image results are present, we want to return only the keyword results
        serpapi_results = merge_pipeline_steps_results(
            previousStep=serpapi_image_results, currentStep=serpapi_keyword_results
        )
    else:
        serpapi_results = serpapi_keyword_results

    # Step 2: Use Zyte to process the URLs further
    zyte_results = ZyteExtractor(context).apply(serpapi_results)

    # Step 3: Process the results using DataProcessor based on the country
    processor_results = DataProcessor(context).apply(
        pipeline_results=zyte_results, country=args.country
    )

    # Step 4: delivery policy filtering
    delivery_policy_filtering_results = DeliveryPolicyDetector(context).apply(
        processor_results
    )

    # Step 5: page type filtering
    page_type_filtering_results = PageTypeDetector(context).apply(
        delivery_policy_filtering_results
    )

    # Step 6: blocked / corrupted content detection
    blocked_content_results = BlockedContentDetector(context).apply(
        page_type_filtering_results
    )

    # Step 7: content domain filtering
    content_domain_results = ContentDomainDetector(context).apply(
        blocked_content_results
    )

    # Step 8: suspiciousness classifier
    suspiscousness_results = SuspiciousnessClassifier(context).apply(
        content_domain_results
    )

    # Step 9: ranking
    ResultRanker(context).apply(suspiscousness_results)

    # TODO transform final_results into List[CrawlResult] for the libnightcawler lib
    # TODO store final_results as CrawlResult object to storage
