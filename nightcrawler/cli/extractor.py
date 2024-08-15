import argparse
import logging

from typing import List
from nightcrawler.extract.serp_api import SerpapiExtractor
from nightcrawler.extract.zyte import ZyteExtractor

from helpers.context import Context
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
    parser.add_argument("keyword", help="Keyword to search for")
    parser.add_argument(
        "-n",
        "--num-of-results",
        help="Set the number of results your want to include from serpapi %(default)s",
        default=50,
    )

    subparser = parser.add_subparsers(help="Modules", dest="extract", required=False)

    serpapi = subparser.add_parser(
        "serpapi",
        help="Retrieve a list of URLs for a given keyword",
        parents=parents,
    )
    serpapi.add_argument(
        "--full-output",
        action="store_true",
        help="Set this argument, if you want to see the full results rather then only the URLs provided by SerpAPI. %(default)s",
    )

    zyte = subparser.add_parser(
        "zyte",
        help="Search URLs from zyte for a given keyword",
        parents=parents,
    )
    zyte.add_argument("urlpath", help="Filepath to URL file produced by Serpapi")

    return parser


def apply(args: argparse.Namespace) -> None:
    """
    Applies the functionality specified by the parsed command-line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()

    if not args.extract:
        urls = SerpapiExtractor(context).apply(keyword = args.keyword, number_of_results = args.num_of_results)
        ZyteExtractor(context).apply(urls)

    elif args.extract == "serpapi":
        SerpapiExtractor(context).apply(keyword = args.keyword, number_of_results = args.num_of_results, full_output = args.full_output)
    elif args.extract == "zyte":
        with open(args.urlpath, "r") as file:
            urls = eval(file.read())
        ZyteExtractor(context).apply(urls)

    else:
        logger.error(f"{args} not yet implemented")
