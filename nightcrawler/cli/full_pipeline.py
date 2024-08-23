import argparse
import logging

from typing import List
from datetime import datetime
from nightcrawler.process.dataprocessor import DataProcessor
from nightcrawler.extract.serp_api import SerpapiExtractor
from nightcrawler.extract.zyte import ZyteExtractor
from helpers import LOGGER_NAME
from helpers.context import Context
from helpers.utils import create_output_dir

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
    Adds the 'fullrun' parser and its subparsers to the given subparsers collection.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers collection to add to.
        parents_ (List[argparse.ArgumentParser]): A list of parent parsers to inherit arguments from.

    Returns:
        argparse.ArgumentParser: The parser that was added to the subparsers collection.
    """
    parents = parents_
    parser = subparsers.add_parser(
        parser_name(),
        help="Run the full pipeline from extraction to processing",
        parents=parents,
    )

    parser.add_argument(
        "--full-output",
        action="store_true",
        help="Set this argument if you want to see the full results rather than only the URLs provided by SerpAPI. (default: %(default)s)",
    )

    return parser


def apply(args: argparse.Namespace) -> None:
    """
    Applies the full pipeline, combining extraction and processing.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    starttime = context.today

    # Step 1: Extract URLs using Serpapi
    output_dir = create_output_dir(args.keyword, context.output_path)
    urls = SerpapiExtractor(context).apply(
        keyword=args.keyword,
        number_of_results=args.num_of_results,
        full_output=args.full_output,
        output_dir=output_dir,
    )

    # Step 2: Use Zyte to process the URLs further
    ZyteExtractor(context).apply(urls, output_dir=output_dir)

    # Step 3: Process the results using DataProcessor based on the country
    if args.country:
        DataProcessor(context).step_country_filtering(
            country=args.country, urlpath=output_dir.split("/")[-1]
        )

    else:
        # TODO implement full run across countries
        DataProcessor(context).apply()

    runtime = (datetime.now() - starttime).seconds
    logger.info(f"Pipeline execution finished after {runtime} seconds.")
