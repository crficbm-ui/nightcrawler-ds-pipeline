import argparse
import logging
import os
import sys
from typing import List

import nightcrawler.cli.extractor as extractor
import nightcrawler.cli.processor as processor
import nightcrawler.cli.full_pipeline as fullrun
import nightcrawler.cli.version

from nightcrawler.helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)
MODULES = [extractor, processor, fullrun]


def config_logs(args: List[str]) -> None:
    """
    Configures logging based on the provided arguments.

    - Ensures that the log directory exists if a log file is specified.
    - Sets the logging level and log handlers (stream and file).

    Args:
        args (List[str]): A list of arguments. Expected attributes include:
            - log_file (str): Path to the log file.
            - log_level (str): Logging level (e.g., "DEBUG", "INFO").
    """
    # Ensure log directory exists if a log file is specified
    if args.log_file:
        log_dir = os.path.dirname(args.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    # Convert the log level to a numeric value
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(
            f"Invalid log level: {args.log_level}. Choose any of {', '.join([name for name in logging._nameToLevel.keys()][:-1])}"
        )

    # Clear any existing handlers from the logger
    logger.handlers.clear()

    # Set up the stream handler for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(stream_handler)

    # Set up the file handler if a log file is specified
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        )
        logger.addHandler(file_handler)

    # Set the logger level to the specified numeric level
    logger.setLevel(numeric_level)
    logger.debug(args)


def parse_args(args_: List[str]) -> argparse.Namespace:
    """
    Parse command-line arguments and set up logging configuration.

    Args:
        args_ (List[str]): List of command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments as a namespace object.
    """
    global_parser = argparse.ArgumentParser(add_help=False)
    group = global_parser.add_argument_group("Global options")
    group.add_argument(
        "--log-level",
        default=os.getenv("NIGHTCRAWLER_LOG_LEVEL", "INFO"),
        help="Log level (%(default)s)",
    )
    group.add_argument(
        "--log-file",
        default=os.getenv("NIGHTCRAWLER_LOG_FILE", None),
        help="Log to file (%(default)s)",
    )
    group.add_argument(
        "-v",
        "--version",
        action="version",
        version=nightcrawler.cli.version.__version__,
    )

    # Define the parent parser for common arguments, without keyword
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "-n",
        "--number-of-results",
        help="Set the number of results you want to include from Serpapi.  (3 per google shopping, google site search, google and ebay = 12 URLs total, default: %(default)s)",
        default=50,
        type=int,
    )

    common_parser.add_argument(
        "--unit",
        choices=["Swissmedic AM", "Swissmedic MEP", "Ages"],
        required=True,
        help="Processes URLs using org. unit specific settings.",
    )

    common_parser.add_argument(
        "--page-type-detection-method",
        choices=["infer", "zyte"],
        required=False,
        default="zyte",
        help="By default we are using a probability of Zyte indicating, if the page type is ecommerce or not. You can change this to using a custom BERT model served on the GPU.",
    )

    common_parser.add_argument(
        "-r",
        "--reverse-image-search",
        action="store_true",
        default=False,
        help="Enable reverse image search (default is False)",
    )

    common_parser.add_argument(
        "-e",
        "--enrich-keyword",
        action="store_true",
        help="Enrich the user entered keyword with Dataforseo-API",
    )

    parser = argparse.ArgumentParser(
        description="NightCrawler", parents=[global_parser]
    )
    subparsers = parser.add_subparsers(help="Modules", dest="module", required=True)

    # Add parsers for each module
    for module in MODULES:
        module_parser = module.add_parser(subparsers, [global_parser, common_parser])

        # Add the keyword argument only if the module is "extract"
        if module.parser_name() in ["extract", "fullrun"]:
            module_parser.add_argument(
                "searchitem",
                help="The searchitem typically is a keyword, if so, you can type any term you want to search for. However, if you want to do a reverse image search (with -r argument), this will also accept an URL",
            )

    args = parser.parse_args(args_)

    return args


def apply(args: argparse.Namespace) -> None:
    """
    Apply the selected module's functionality based on the parsed arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    for module in MODULES:
        if args.module == module.parser_name():
            module.apply(args)


def run(args_: List[str]) -> None:
    """
    Run the argument parsing and apply the selected module.

    Args:
        args_ (List[str]): List of command-line arguments.
    """
    args = parse_args(args_)
    config_logs(args)
    apply(args)


def main() -> None:
    """
    Main entry point for the script.
    """
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
