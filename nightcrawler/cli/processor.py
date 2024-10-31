import argparse
import logging

from typing import List
from nightcrawler.process.s05_dataprocessor import DataProcessor
from nightcrawler.base import ProcessData

from nightcrawler.helpers.utils_io import get_object_from_file
from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.context import Context

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

    parser.add_argument(
        "countryinputpath",
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
    if args.country:
        if not args.countryinputpath:
            logger.error(
                "No country input path argument was provided (zyte output). No can do amigo"
            )
        else:
            context.output_dir = f"{context.output_path}/{args.countryinputpath}"
            pipeline_result = get_object_from_file(
                dir=context.output_dir,
                filename=context.zyte_filename,
                processing_object=ProcessData,
            )

            DataProcessor(context).step_country_filtering(
                pipeline_result=pipeline_result, country=args.country
            )

    else:
        # Full pipeline
        DataProcessor(context).apply()
