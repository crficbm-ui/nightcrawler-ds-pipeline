import argparse
import logging

from typing import List
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.extract.s03_reverse_image_search import GoogleReverseImageApi
from nightcrawler.extract.s04_zyte import ZyteExtractor
from nightcrawler.base import ProcessData
from nightcrawler.helpers.utils_io import get_object_from_file
from nightcrawler.context import Context
from nightcrawler.helpers.utils import create_output_dir
from nightcrawler.helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'extract'.
    """
    return "extract"


def add_parser(
    subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]
) -> argparse.ArgumentParser:
    """
    Adds the 'extract' parser and its subparsers to the given subparsers collection.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers collection to add to.
        parents_ (List[argparse.ArgumentParser]): A list of parent parsers to inherit arguments from.

    Returns:
        argparse.ArgumentParser: The parser that was added to the subparsers collection.
    """
    parents = parents_
    parser = subparsers.add_parser(
        parser_name(),
        help="extract calls the extractor class",
        parents=parents,
    )

    parser.add_argument(
        "-u", "--urlpath", help="Filepath to URL file produced by Serpapi"
    )

    parser.add_argument(
        "-s",
        "--step",
        choices=["serpapi", "zyte"],
        required=False,
        default=None,
        help="Specify the step to execute: 'serpapi' to retrieve URLs, 'zyte' to process URLs from a file.",
    )

    return parser


def apply(args: argparse.Namespace) -> None:
    """
    Applies the functionality specified by the parsed command-line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    context.output_dir = None

    if args.step != "zyte":
        # create the output directory only if the full extract pipeline is run or if the serpapi extraction is performed as a single step
        context.output_dir = create_output_dir(args.searchitem, context.output_path)

    # if a full pipeline run is triggered (therefore args.step is empty)
    if not args.step:
        # Step 1a: Perform reverse image search only if image_urls (List[str]) are provided
        if args.reverse_image_search:
            # Handle reverse image search
            image_urls = args.reverse_image_search
            serpapi_results = GoogleReverseImageApi(context).apply(
                image_urls=image_urls,
                keywords=args.searchitem,
                number_of_results=args.number_of_results,
            )
        else:
            # Step 1b Extract URLs using Serpapi if no image_urls were provided
            serpapi_results = SerpapiExtractor(context).apply(
                keyword=args.searchitem, number_of_results=args.number_of_results
            )
        ZyteExtractor(context).apply(serpapi_results)
    elif args.step == "serpapi":
        SerpapiExtractor(context).apply(
            keyword=args.searchitem, number_of_results=args.number_of_results
        )
    elif args.step == "zyte":
        if args.step == "zyte" and not args.urlpath:
            logger.error(
                "No URL-path provided, do so by adding '--urlpath' to your CLI command "
            )
        else:
            context.output_dir = "/".join(args.urlpath.split("/")[:-1])
            pipeline_result = get_object_from_file(
                dir=context.output_dir,
                filename=context.serpapi_filename,
                processing_object=ProcessData,
            )
            ZyteExtractor(context).apply(pipeline_result)

    else:
        logger.error(f"{args} not yet implemented")
