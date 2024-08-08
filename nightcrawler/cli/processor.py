def parser_name():
    return "process"

def add_parser(subparsers, parents_):
    parents = parents_
    parser = subparsers.add_parser(
        parser_name(),
        help="process calls the processor class",
        parents=parents,
    )

    subparser = parser.add_subparsers(help="Modules", dest="process", required=True)

    country = subparser.add_parser(
        "country",
        help="Processes URLs using a country specific pipeline",
        parents=parents,
    )
    country.add_argument("country", help="country used from set [CH,AT,CL]")

    return parser


def apply(args):
    dp_client = DataProcessor()
    if args.extract == "country":
        processed_urls = dp_client.process_urls_from_datacollector(country=args.country)
        # logging.info(processed_urls)

