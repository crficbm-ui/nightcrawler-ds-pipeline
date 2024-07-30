import argcomplete
import argparse
import logging
import os
import sys

import nightcrawler.cli.extractor as ncdc
import nightcrawler.cli.version

MODULES = [ncdc]


def parse_args(args_):
    # Create global parser for logs
    global_parser = argparse.ArgumentParser(add_help=False)
    group = global_parser.add_argument_group("Global options")
    group.add_argument(
        "--log-level",
        default=os.getenv("NIGHTCRAWLER_LOG_LEVEL", "INFO"),
        help="log level (%(default)s)",
    )
    group.add_argument(
        "--log-file",
        default=os.getenv("NIGHTCRAWLER_LOG_FILE", None),
        help="log to file (%(default)s)",
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

    # Concatenate arguments
    argcomplete.autocomplete(parser)
    args = parser.parse_args(args_)

    # Log management
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % args.log_level)  # pragma: no cover
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            (
                logging.StreamHandler()
                if args.log_file is None
                else logging.FileHandler(args.log_file)
            )
        ],
    )
    logging.debug(args)
    return args


def apply(args):
    for module in MODULES:
        if args.module == module.parser_name():
            module.apply(args)


def run(args_):
    args = parse_args(args_)
    apply(args)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
