import argparse
import logging
import os
import sys
from typing import List

import nightcrawler.cli.extractor as extractor
import nightcrawler.cli.version

from helpers import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)
MODULES = [extractor]


def parse_args(args_: List[str]) -> argparse.Namespace:
    """
    Parse command-line arguments and set up logging configuration.

    Args:
        args_ (List[str]): List of command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments as a namespace object.
    """
    # Create global parser for logs
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

    parser = argparse.ArgumentParser(
        description="Nightcrawler", parents=[global_parser]
    )
    subparsers = parser.add_subparsers(help="Modules", dest="module", required=True)

    for module in MODULES:
        module.add_parser(subparsers, [global_parser])


    args = parser.parse_args(args_)

    # Ensure log directory exists if a log file is specified
    if args.log_file:
        log_dir = os.path.dirname(args.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    # Log management
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    
    # Remove all existing handlers to prevent duplicate logs
    logger.handlers.clear()

    # Conditionally create a StreamHandler or FileHandler
    handler = (
        logging.StreamHandler()
        if args.log_file is None
        else logging.FileHandler(args.log_file)
    )

    # Set the formatter
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    # Set the log level
    logger.setLevel(numeric_level)

    # Example log statement
    logger.debug(args)
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
    apply(args)


def main() -> None:
    """
    Main entry point for the script.
    """
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
