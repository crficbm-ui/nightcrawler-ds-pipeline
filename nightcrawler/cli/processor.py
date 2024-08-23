import argparse
import logging

from typing import List
from nightcrawler.process.dataprocessor import DataProcessor
from helpers import LOGGER_NAME
from helpers.context import Context

logger = logging.getLogger(LOGGER_NAME)


def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'processor'.
    """
    return "process"

def add_parser(
        subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]
               ) -> argparse.ArgumentParser:
    """
    Adds the 'process' parser and its subparsers to the given subparsers collection.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers collection to add to.
        parents_ (List[argparse.ArgumentParser]): A list of parent parsers to inherit arguments from.

    Returns:
        argparse.ArgumentParser: The parser that was added to the subparsers collection.
    """
    parents = parents_
    parser = subparsers.add_parser(
        parser_name(),
        help="process calls the processor class",
        parents=parents,
    )
    #parser.add_argument("processorpath", help="Indicates the URL path to be produced through the processor")

    subparser = parser.add_subparsers(help="Modules", dest="process", required=False)

    country = subparser.add_parser(
        "country",
        help="Processes URLs using a country specific pipeline",
        parents=parents,
    )
    country.add_argument("country", help="country used from given set",
                         choices=["CH", "AT", "CL"])  # Restrict to the specified choices)

    country.add_argument("countryinputpath",
                         help="Filepath to be produced by zyte and consumed by country filter",
                         nargs="?",  # Makes this argument optional
                         )

    return parser


def apply(args: argparse.Namespace) -> None:
    """
    Applies the functionality specified by the parsed command-line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()

    # Individual components run through CLI: COUNTRY
    if args.process == "country":
        if not args.countryinputpath:
            logger.error("No country input path argument was provided (zyte output). No can do amigo")
        else:
            DataProcessor(context).step_country_filtering(country=args.country, urlpath=args.countryinputpath)

    # Fill pipeline
    elif not args.extract:
        DataProcessor(context).apply()

    else:
        logger.error(f"{args} not yet implemented")