from typing import List, Dict, Any

from nightcrawler.helpers.context import Context
from nightcrawler.helpers.api.serp_api import SerpAPI
from nightcrawler.helpers.api.dataforseo_api import DataforSeoAPI
from nightcrawler.helpers.analytics import keywords_selection
from nightcrawler.helpers.utils_io import from_dict

from nightcrawler.base import ExtractSerpapiData, PipelineResult, BaseStep


class KeywordEnricher(BaseStep):
    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        """
        Initializes the KeywordEnricher with a given context.

        Args:
            context (Context): The execution context for the enrichment pipeline.
        """
        super().__init__(self._entity_name)
        self.context = context

    def apply_step(
        self,
        keyword: str,
        serpapi: SerpAPI,
        number_of_keywords: int,
        location: str,
        language: str,
        previous_step_results: PipelineResult,
        check_limit: int = 200,
    ) -> PipelineResult:
        """
        Makes the API call to SerpAPI to enrich the keyword and retrieve search results.

        The process is decomposed as follows:
        1. From root keyword, call DataforSeo API to get suggested and related keywords with maximum search volumes for different locations and languages.
        2. Deduplicate keywords and aggregate search volume from different locations/languages.
        3. Call SerpAPI for selected keywords and get the corresponding URLs (top 20 only).
        4. Deduplicate URLs and estimate total traffic per URL.
        5. Return the top 200 URLs with the highest traffic.

        Args:
            keyword (str): The search keyword to enrich.
            serpapi (SerpAPI): The SerpAPI client instance to fetch results.
            number_of_keywords (int): The number of keywords to retrieve.
            location (str): The location for keyword enrichment.
            language (str): The language to search in.

        Returns:
            PipelineResult: Contains metadata and enriched search results.
        """

        client = serpapi.initiate_client()

        suggested_kw: List[Dict[str, Any]] = []
        related_kw: List[Dict[str, Any]] = []

        suggested_kw += DataforSeoAPI().get_keyword_suggestions(
            keyword, location, language, number_of_keywords
        )
        related_kw += DataforSeoAPI().get_related_keywords(
            keyword, location, language, number_of_keywords
        )

        enriched_kw = suggested_kw + related_kw
        filtered_kw: List[str] = []

        for kw in enriched_kw:
            filtered_kw.append(
                keywords_selection.filter_keywords(kw["keywordEnriched"])
            )

        agg_kw = keywords_selection.aggregate_keywords(enriched_kw).to_dict(
            orient="records"
        )

        urls: List[Dict[str, Any]] = []
        for kw in agg_kw:
            response = serpapi.retrieve_response(
                keyword=kw["keywordEnriched"],
                client=client,
                offer_root=kw["offerRoot"],
                number_of_results=number_of_keywords,
            )
            items = client.get_organic_results(response)

            kw_urls = [item.get("link") for item in items]
            results = client._check_limit(kw_urls, kw, check_limit)
            urls += keywords_selection.estimate_volume_per_url(
                results,
                kw["keywordVolume"],
                kw["keywordEnriched"],
                kw["keywordLocation"],
                kw["keywordLanguage"],
                kw["offerRoot"],
            )

        enriched_results = keywords_selection.aggregate_urls(urls)

        # Put the enriched_results into an ExtractSerpapiData object
        enriched_results_object = [
            from_dict(ExtractSerpapiData, enriched_result)
            for enriched_result in enriched_results
        ]

        # Updating the PipelineResults Object (append the results to the results list und update the number of results after this stage)
        enriched_results_formated = self.add_pipeline_steps_to_results(
            currentStepResults=enriched_results_object,
            pipelineResults=previous_step_results,
            currentStepResultsIsPipelineResultsObject=False,
        )

        self.store_results(
            enriched_results_formated,
            self.context.output_dir,
            self.context.serpapi_filename_keyword_enrichement,
        )

        return enriched_results_formated
