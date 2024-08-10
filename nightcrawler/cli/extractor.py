import argparse
import logging
from typing import List, Any
from dataclasses import asdict
from nightcrawler.context import Context
from nightcrawler.extract.datacollector import DataCollector
from nightcrawler.utils import write_json

def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'extract'.
    """
    return "extract"


def add_parser(subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]) -> argparse.ArgumentParser:
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
    subparser = parser.add_subparsers(help="Modules", dest="extract", required=True)

    serpapi = subparser.add_parser(
        "serpapi",
        help="Retrieve a list of URLs for a given keyword",
        parents=parents,
    )
    serpapi.add_argument("keywords", nargs="+", help="Keywords to search for, comma-separated")

    diffbot = subparser.add_parser(
        "diffbot",
        help="Search URLs from Diffbot for a given keyword",
        parents=parents,
    )
    diffbot.add_argument("urlpath", help="Filepath to URL file produced by Serpapi")

    return parser


def apply(args: argparse.Namespace) -> None:
    """
    Applies the functionality specified by the parsed command-line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    dc_client = DataCollector(context)
    
    if args.extract == "serpapi":
        urls = dc_client.get_urls_from_serpapi(keywords=args.keywords)
        write_json(context.output_path, context.diffbot_filename, urls)

    elif args.extract == "diffbot":
        results = dc_client.get_diffbot_bulk(urlpath=args.urlpath)
        write_json(context.output_path, context.diffbot_filename, results)
