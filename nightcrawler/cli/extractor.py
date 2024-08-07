import argparse
import logging
from dataclasses import asdict
from nightcrawler.contex import Context
from nightcrawler.extract.datacollector import DataCollector
from nightcrawler.utils import write_json

def parser_name():
    return "extract"


def add_parser(subparsers, parents_):
    parents = parents_
    parser = subparsers.add_parser(
        parser_name(),
        help="extract calls the extractor class ",
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
    diffbot.add_argument("urlpath", help="Filetpath to URL file produced by Serpapi")

    return parser


def apply(args):
    context = Context()
    dc_client = DataCollector(context)
    if args.extract == "serpapi":
        urls = dc_client.get_urls_from_serpapi(keywords=args.keywords)
        write_json(context.output_path, context.diffbot_filename, urls)

    elif args.extract == "diffbot":
        results = dc_client.get_diffbot_bulk(urlpath=args.urlpath)
        write_json(context.output_path, context.diffbot_filename, results)
