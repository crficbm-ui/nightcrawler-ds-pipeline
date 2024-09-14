from typing import List


from helpers.context import Context
from helpers.api.serp_api import SerpAPI
from helpers.api.dataforseo_api import DataforSeoAPI
from helpers.analytics import keywords_selection
from helpers.utils import from_dict

from nightcrawler.base import ExtractSerpapiData, MetaData, PipelineResult, BaseStep


class KeyWordEnricher(BaseStep):
    _entity_name: str = __qualname__

    def __init__(self, context: Context) -> None:
        super().__init__(self._entity_name)

        self.context = context

    def apply(
        self,
        keyword: str,
        serpapi: SerpAPI,
        number_of_results: int,
        locations: str,
        languages: List[str],
    ) -> PipelineResult:
        """
         Makes the API call to SerpAPI to enrich with multiple keywords and retrieve search results.
         Processed is decomposed as the following:
             1. From root keyword call dataforSeo API to get suggested and related keywords with maximum search volumes for different location and volume
             2. Deduplicate keywords and add search volume from different locations/languages
             3. Call serp API for selected keywords and get the corresponding urls (top 20 only)
             4. Deduplicate urls and estimate total traffic per url
             5. Return the 200 first urls with highest traffic
        Args:
             keyword (str): The search keyword.
             client (SerpAPI): The SerpAPI client instance.
             number_of_results (int): The number of search results to retrieve.
             locations (List[str]): The list of locations to search in.
             languages (List[str]): The list of languages to search in.
         Returns:
             Dict[str, Any]: The raw response data from the SerpAPI.
        """

        client = serpapi.initiate_client()

        suggested_kw = []
        related_kw = []

        # TODO if there will be only a single location / language per enriched keyword, this can be simplified
        for loc in locations:
            for lang in languages:
                suggested_kw = suggested_kw + DataforSeoAPI().get_keyword_suggestions(
                    keyword, loc, lang, number_of_results
                )
                related_kw = related_kw + DataforSeoAPI().get_related_keywords(
                    keyword, loc, lang, number_of_results
                )

        enriched_kw = suggested_kw + related_kw
        filtered_kw = []
        for keyword in enriched_kw:
            filtered_kw.append(
                keywords_selection.filter_keywords(keyword["keywordEnriched"])
            )
        agg_kw = keywords_selection.aggregate_keywords(enriched_kw).to_dict(
            orient="records"
        )

        urls = []
        for keyword in agg_kw:
            response = serpapi.retrieve_response(
                keyword=keyword["keywordEnriched"],
                client=client,
                offer_root=keyword["offerRoot"],
                number_of_results=number_of_results,
            )
            items = client.get_organic_results(response)

            kw_urls = [item.get("link") for item in items]
            results = client._check_limit(kw_urls, keyword, 200)
            urls = urls + keywords_selection.estimate_volume_per_url(
                results,
                keyword["keywordVolume"],
                keyword["keywordEnriched"],
                keyword["keywordLocation"],
                keyword["keywordLanguage"],
                keyword["offerRoot"],
            )

        enriched_results = keywords_selection.aggregate_urls(urls)

        # put the enriched_results into an ExtractSerpapiData object
        enriched_results = [
            from_dict(ExtractSerpapiData, enriched_result)
            for enriched_result in enriched_results
        ]

        metadata = MetaData(
            keyword=keyword,
            numberOfResults=number_of_results,
            numberOfResultsAfterStage=len(enriched_results),
        )

        enriched_results_formated = PipelineResult(
            meta=metadata, results=enriched_results
        )

        self.store_results(
            enriched_results_formated,
            self.context.output_dir,
            self.context.serpapi_filename_keyword_enrichement,
        )

        return enriched_results_formated
