import argparse
import backoff
import logging
from typing import List
from nightcrawler.process.s05_dataprocessor import DataProcessor
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.extract.s02_enriched_keywords import KeywordEnricher
from nightcrawler.extract.s04_zyte import ZyteExtractor
from nightcrawler.extract.s03_reverse_image_search import GoogleReverseImageApi
from nightcrawler.process.s06_delivery_page_detection import DeliveryPolicyDetector
from nightcrawler.process.s07_page_type_detection import PageTypeDetector
from nightcrawler.process.s08_blocket_content_detection import BlockedContentDetector
from nightcrawler.process.s09_content_domain_detection import ContentDomainDetector
from nightcrawler.process.s10_suspiciousness_classifier import SuspiciousnessClassifier
from nightcrawler.process.s11_result_ranker import ResultRanker
from nightcrawler.base import BaseStep, PipelineResult, MetaData, ExtractSerpapiData

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.context import Context
from nightcrawler.helpers.utils import create_output_dir
from nightcrawler.helpers.decorators import timeit

import libnightcrawler.objects as lo

logger = logging.getLogger(LOGGER_NAME)


def parser_name() -> str:
    """
    Returns the name of the parser.

    Returns:
        str: The name of the parser, 'fullrun'.
    """
    return "fullrun"


def add_parser(
    subparsers: argparse._SubParsersAction, parents_: List[argparse.ArgumentParser]
) -> argparse.ArgumentParser:
    """
    Adds the 'fullrun' parser to the collection of subparsers for the command-line interface.

    This parser is specifically designed to handle the 'fullrun' command, which executes the entire
    data pipeline end-to-end. The subparsers collection typically contains different parsers for various
    commands that can be executed through the CLI, each corresponding to a different aspect of the pipeline
    or a distinct operation within the application.

    Args:
        subparsers (argparse._SubParsersAction): The collection of subparsers associated with the
            primary argument parser. This collection organizes the different commands available in
            the CLI, allowing the user to select and execute specific functionalities.
        parents_ (List[argparse.ArgumentParser]): A list of parent parsers that provide common
            arguments and configurations shared across multiple subparsers, ensuring consistency
            and reusability in argument parsing.

    Returns:
        argparse.ArgumentParser: The parser associated with the 'fullrun' command, equipped with
        arguments and help descriptions specific to executing the full pipeline.
    """
    parser = subparsers.add_parser(
        parser_name(),
        help="Run the full pipeline from extraction to processing",
        parents=parents_,
    )

    parser.add_argument(
        "--case-id", default=None, help="DB identifier of the case (%(default)s)"
    )
    parser.add_argument(
        "--keyword-id", default=None, help="DB identifier of the keyword (%(default)s)"
    )
    return parser


@timeit
@backoff.on_exception(backoff.expo, Exception, logger=logger, factor=3, max_tries=4)
def handle_request(context: Context, request: lo.CrawlRequest) -> None:
    """
    Applies the full pipeline on a single request
    """

    if not context.settings.use_file_storage:
        context.set_crawl_pending(request.case_id, request.keyword_id)

    if request.keyword_type in ["text", "url"]:
        # Step 0: create the results directory with searchitem = keyword
        context.output_dir = create_output_dir(
            request.keyword_value, context.output_path
        )

        if request.keyword_type == "text":
            # Step 1 Extract URLs using Serpapi based on a searchitem (=keyword) provided by the users
            serpapi_results = SerpapiExtractor(context).apply(
                keyword=request.keyword_value,
                number_of_results=request.number_of_results,
            )
        elif request.keyword_type == "url":
            serpapi_results = PipelineResult(
                meta=MetaData(
                    keyword=request.keyword_value,
                    numberOfResults=1,
                    numberOfResultsAfterStage=1,
                ),
                results=[
                    ExtractSerpapiData(offerRoot="manual", url=request.keyword_value)
                ],
            )

        # Step 2: Enricht query by adding additional keywords if `-e` argument was set
        if request.enrich_keyword:
            # load dataForSeo configs based on the country information, if none provided, default to CH
            api_config_for_country = context.settings.data_for_seo.api_params.get(
                request.organization.countries[0]
            )
            serpapi_results = KeywordEnricher(context).apply(
                keyword=request.keyword_value,
                serpapi=SerpapiExtractor(context),
                number_of_keywords=3,
                location=api_config_for_country.get("location"),
                language=api_config_for_country.get("language"),
                previous_step_results=serpapi_results,
            )
        else:
            logger.warning(
                "Skipping keyword enrichment as option `-e` was not specified"
            )
            BaseStep._step_counter += 1  # doing this, so that the the output files still match the step count specified in the README.md. However, this will lead to gaps in the numbering of the output files (3 will be missing).

    elif request.keyword_type == "image":
        # Step 0: create the results directory with searchitem = url, so just name it 'reverse_image_search'.
        context.output_dir = create_output_dir(
            "reverse_image_search", context.output_path
        )

        # Make image publicly accessible if necessary
        public_url = (
            request.keyword_value
            if "http" in request.keyword_value
            else context.blob_client.make_public(request.keyword_value)
        )

        # Step 3 Extract URLs using Serpapi - Perform reverse image search if image-urls were provided
        serpapi_results = GoogleReverseImageApi(context).apply(
            image_url=public_url,
            number_of_results=request.number_of_results,
        )

        # Remove image from publicly accessible container if necessary
        if "http" not in request.keyword_value:
            context.blob_client.remove_from_public(request.keyword_value)

    else:
        raise ValueError("Unknown keyword type %s", request.keyword_type)

    # Step 4: Use Zyte to process the URLs further
    zyte_results = ZyteExtractor(context).apply(serpapi_results)

    # Step 5: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists. All these depend on the --country input of the pipeline call.
    # TODO replace the manual filtering logic with Mistral call by Nicolas W.
    # TODO Must support a list of countries, not a single one
    processor_results = DataProcessor(context).apply(
        previous_step_results=zyte_results, country=request.organization.countries[0]
    )

    # Step 6: delivery policy filtering based on offline analysis of domains public delivery information
    delivery_policy_filtering_results = DeliveryPolicyDetector(context).apply(
        previous_step_results=processor_results
    )

    # Step 7: page type filtering based on either a probability of Zyte (=default) or a custom BERT model deployed on the mutualized GPU. The pageType can be either 'ecommerce_product' or 'other'.
    page_type_filtering_results = PageTypeDetector(context).apply(
        previous_step_results=delivery_policy_filtering_results,
        page_type_detection_method=request.page_type_detection_method,
    )

    # Step 8: blocked / corrupted content detection based the prediction with a BERT model.
    blocked_content_results = BlockedContentDetector(context).apply(
        previous_step_results=page_type_filtering_results
    )

    # Step 9: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)
    content_domain_results = ContentDomainDetector(context).apply(
        previous_step_results=blocked_content_results
    )

    # Step 10: Binary classifier per organisation, whether a product is classified as suspicious or not.
    suspiscousness_results = SuspiciousnessClassifier(context).apply(
        previous_step_results=content_domain_results
    )

    # Step 11: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away.
    final_results = ResultRanker(context).apply(
        previous_step_results=suspiscousness_results
    )

    if not context.settings.use_file_storage:
        data = [
            request.new_result(
                url=x.url,
                text=x.fullDescription or "",
                root=x.offerRoot,
                title=x.title or "",
                uid="",
                platform="",
                source="",
                language="",
                score=0,
                relevant=True,
                images=x.images,
            )
            for x in final_results.results
        ]
        context.store_results(data, request.keyword_id)


@timeit
def apply(args: argparse.Namespace) -> None:
    """
    Applies the full pipeline, combining extraction and processing.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    all_orgs = context.get_organization()
    org = (
        all_orgs[args.org]
        if args.org
        else next(x for x in all_orgs.values() if args.country in x.countries)
    )
    logger.debug("Using org: %s", org)

    keyword_type = "text"
    if args.searchitem.startswith("http") and not args.reverse_image_search:
        keyword_type = "url"
    if args.reverse_image_search:
        keyword_type = "image"
    request = lo.CrawlRequest(
        keyword_type=keyword_type,
        keyword_value=args.searchitem,
        case_id=args.case_id,
        keyword_id=args.keyword_id,
        organization=org,
        number_of_results=args.number_of_results,
        page_type_detection_method=args.page_type_detection_method,
        enrich_keyword=args.enrich_keyword,
    )
    handle_request(context, request)
