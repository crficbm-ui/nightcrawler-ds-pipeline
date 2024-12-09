import argparse
import backoff
import logging
from typing import List
from nightcrawler.process.s05_dataprocessor import DataProcessor
from nightcrawler.extract.s01_serp_api import SerpapiExtractor
from nightcrawler.extract.s02_enriched_keywords import KeywordEnricher
from nightcrawler.extract.s04_zyte import ZyteExtractor
from nightcrawler.extract.s03_google_lens_search import GoogleLensApi
from nightcrawler.process.s06_delivery_page_detection import DeliveryPolicyDetector
from nightcrawler.process.s07_page_type_detection import PageTypeDetector
from nightcrawler.process.s08_corrupted_content_detection import (
    CorruptedContentDetector,
)
from nightcrawler.process.s09_content_domain_detection import ContentDomainDetector
from nightcrawler.process.s10_suspiciousness_classifier import SuspiciousnessClassifier
from nightcrawler.process.s11_result_ranker import ResultRanker
from nightcrawler.base import BaseStep, PipelineResult, MetaData, ExtractSerpapiData

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.context import Context
from nightcrawler.helpers.decorators import timeit
from nightcrawler.helpers.utils import create_result

import libnightcrawler.objects as lo
import libnightcrawler.db.schema as lds


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
        "--case-id",
        type=int,
        default=None,
        help="DB identifier of the case (%(default)s)",
    )
    parser.add_argument(
        "--keyword-id",
        type=int,
        default=None,
        help="DB identifier of the keyword (%(default)s)",
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

    keyword_type = request.keyword_type.lower()
    if keyword_type in ["text", "url"]:
        if keyword_type == "text":
            # Step 1 Extract URLs using Serpapi based on a searchitem (=keyword) provided by the users
            serpapi_results = SerpapiExtractor(context, request.organization).apply(
                keyword=request.keyword_value,
                number_of_results=request.number_of_results,
            )
        elif keyword_type == "url":
            serpapi_results = PipelineResult(
                meta=MetaData(
                    keyword=request.keyword_value,
                    numberOfResultsManuallySet=1,
                ),
                relevant_results=[
                    ExtractSerpapiData(offerRoot="manual", url=request.keyword_value)
                ],
            )

        # Step 2: Enricht query by adding additional keywords if `-e` argument was set
        if request.enrich_keyword:
            serpapi_results = KeywordEnricher(context).apply(
                keyword=request.keyword_value,
                serpapi=SerpapiExtractor(context, request.organization),
                number_of_keywords=3,
                location=request.organization.countries[0],
                language=request.organization.languages[0],
                previous_step_results=serpapi_results,
            )
        else:
            logger.warning(
                "Skipping keyword enrichment as option `-e` was not specified"
            )
            BaseStep._step_counter += 1  # doing this, so that the the output files still match the step count specified in the README.md. However, this will lead to gaps in the numbering of the output files (3 will be missing).

    elif keyword_type == "image":
        # Make image publicly accessible if necessary
        image_path = f"{request.case_id}/{request.keyword_value}"
        public_url = (
            request.keyword_value
            if request.keyword_value.startswith("http")
            else context.blob_client.make_public(image_path)
        )

        # Step 3 Extract URLs using Serpapi - Perform google lens search if image-urls were provided
        serpapi_results = GoogleLensApi(context).apply(
            image_url=public_url,
            number_of_results=request.number_of_results,
            country=request.organization.country_codes[0].lower(),
        )

        # Remove image from publicly accessible container if necessary
        if not request.keyword_value.startswith("http"):
            context.blob_client.remove_from_public(image_path)

    else:
        raise ValueError("Unknown keyword type %s", keyword_type)

    # Step 4: Use Zyte to process the URLs further
    zyte_results = ZyteExtractor(context).apply(serpapi_results)

    # Step 5: Apply some (for the time-being) manual filtering logic: filter based on URL, currency and blacklists. All these depend on the --country input of the pipeline call.
    # TODO replace the manual filtering logic with Mistral call by Nicolas W.
    # TODO Must support a list of countries, not a single one
    processor_results = DataProcessor(context).apply(
        previous_step_results=zyte_results,
        country=request.organization.country_codes[0],
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
    corrupted_content_results = CorruptedContentDetector(
        context, organization=request.organization
    ).apply(previous_step_results=page_type_filtering_results)

    # Step 9: classification of the product type is relvant to the target organization domain (i.e. pharmaceutical for Swissmedic AM or medical device for Swissmedic MD)
    content_domain_results = ContentDomainDetector(
        context, organization=request.organization
    ).apply(
        previous_step_results=corrupted_content_results,
    )

    # Step 10: Binary classifier per organisation, whether a product is classified as suspicious or not.
    suspiscousness_results = SuspiciousnessClassifier(context).apply(
        previous_step_results=content_domain_results
    )

    # Step 11: Apply any kinf of (rule-based?) ranking or filtering of results. If this last step is really needed needs be be confirmed, maybe this step will fall away.
    final_results = ResultRanker(context).apply(
        previous_step_results=suspiscousness_results
    )

    # The user should see the relevant results (seen by full pipeline) and the bypassed results.
    if not context.settings.use_file_storage:
        # Add relevant results and those that are bypassed
        # TODO: should the bypassed results have their own status so that they can be marked as such in the frontend?
        data = [
            create_result(request, x)
            for x in final_results.relevant_results + final_results.bypassed_results
        ]

        # Add erroneous results with status ERROR
        data.extend(
            create_result(request, x, offer_status=lds.Offer.OfferStatus.ERROR)
            for x in final_results.erroreous_results
        )
        context.store_results(data, request.case_id, request.keyword_id)
        context.report_usage(request.case_id, final_results.usage)


@timeit
def apply(args: argparse.Namespace) -> None:
    """
    Applies the full pipeline, combining extraction and processing.

    Args:
        args (argparse.Namespace): Parsed arguments as a namespace object.
    """
    context = Context()
    org = context.organizations.get(args.unit)
    logger.debug("Using org: %s", org)

    keyword_type = "text"
    if args.searchitem.startswith("http") and not args.google_lens_search:
        keyword_type = "url"
    if args.google_lens_search:
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

    # create the output directory before the pipeline starts so that if a backoff occurs, the output directory is not recreated
    if keyword_type in ["text", "url"]:
        # create the results directory with searchitem = keyword
        context.update_output_dir(request.keyword_value)
    elif keyword_type == "image":
        # create the results directory 'google_lens_search'
        context.update_output_dir("google_lens_search")

    handle_request(context, request)
