import argparse
import logging

from typing import List
from nightcrawler.extract.serp_api import SerpapiExtractor
from nightcrawler.extract.zyte import ZyteExtractor

from helpers.context import Context
from helpers.utils import create_output_dir
from helpers import LOGGER_NAME

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
        "--full-output",
        action="store_true",
        help="Set this argument, if you want to see the full results rather then only the URLs provided by SerpAPI. %(default)s",
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

    if args.step != "zyte":
        # create the output directory only if the full extract pipeline is run or if the serpapi extraction is performed as a single step
        output_dir = create_output_dir(args.keyword, context.output_path)

    if not args.step:
        urls = SerpapiExtractor(context).apply(
            keyword=args.keyword,
            number_of_results=args.num_of_results,
            output_dir=output_dir,
        )
        ZyteExtractor(context).apply(urls, output_dir=output_dir)
    elif args.step == "serpapi":
        SerpapiExtractor(context).apply(
            keyword=args.keyword,
            number_of_results=args.num_of_results,
            full_output=args.full_output,
            output_dir=output_dir,
        )
    elif args.step == "zyte":
        if args.step == "zyte" and not args.urlpath:
            logger.error(
                "No URL-path provided, do so by adding '--urlpath' to your CLI command "
            )
        else:
            with open(args.urlpath, "r") as file:
                urls = eval(file.read())
            output_dir = "/".join(args.urlpath.split("/")[:-1])
            ZyteExtractor(context).apply(urls, output_dir=output_dir)

    else:
        logger.error(f"{args} not yet implemented")
