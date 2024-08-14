from nightcrawler.contex import Context
from nightcrawler.process.dataprocessor import DataProcessor

def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'processor'.
    """
    return "process"

def add_parser(subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]) -> argparse.ArgumentParser:
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
    parser.add_argument("urlspath", help="Indicates the URL path to be produced through the processor")

    subparser = parser.add_subparsers(help="Modules", dest="process", required=True)

    country = subparser.add_parser(
        "country",
        help="Processes URLs using a country specific pipeline",
        parents=parents,
    )
    country.add_argument("country", help="country used from set [CH,AT,CL]")

    return parser


def apply(args):
    """
    Applies the functionality specified by the parsed command-line arguments.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    dp_client = DataProcessor()
    if args.extract == "country":
        processed_urls = dp_client.process_urls_from_datacollector(country=args.country)
        # logging.info(processed_urls)

