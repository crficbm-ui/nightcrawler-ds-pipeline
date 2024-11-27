from http.client import HTTPSConnection
from base64 import b64encode
from json import loads
from json import dumps
import logging

from nightcrawler.helpers import LOGGER_NAME
from nightcrawler.helpers.api.api_caller import APICaller
from nightcrawler.context import Context

logger = logging.getLogger(LOGGER_NAME)


class DataforSeoAPI(APICaller):
    def __init__(
        self,
        context: Context,
        cache_name: str = "dataforseoapi",
        max_retries: int = 3,
        retry_delay: int = 2,
    ):
        """
        Initializes the SerpAPI class.

        Args:
            cache_name (str): The name of the cache (default is "serpapi").
            max_retries (int): The maximum number of retries for API calls (default is 3).
            retry_delay (int): The delay in seconds between retry attempts (default is 2).
        """
        super().__init__(context, cache_name, max_retries, retry_delay, 24 * 60 * 60)

    def request(self, path, method, data=None):
        """Make a request to the DataforSEO API

        Args:
            path (str): path to the API endpoint
            method (str): HTTP method
            data (str): data to send with the request

        Returns:
            dict with the response from the API"""

        connection = HTTPSConnection("api.dataforseo.com")
        try:
            base64_bytes = b64encode(
                (
                    "%s:%s"
                    % (
                        self.context.settings.data_for_seo.username,
                        self.context.settings.data_for_seo.password,
                    )
                ).encode("ascii")
            ).decode("ascii")
            headers = {
                "Authorization": "Basic %s" % base64_bytes,
                "Content-Encoding": "gzip",
            }
            connection.request(method, path, headers=headers, body=data)
            response = connection.getresponse()
            if response.status >= 400:
                raise Exception(
                    f"Failed to call dataforseo: {response.status} {response.reason}"
                )
            return loads(response.read().decode())
        finally:
            connection.close()

    def get(self, path):
        return self.request(path, "GET")

    def post(self, path, data):
        if isinstance(data, str):
            data_str = data
        else:
            data_str = dumps(data)
        return self.request(path, "POST", data_str)

    def get_keyword_suggestions(self, keyword, location_name, language_name, limit=100):
        """Get keyword suggestions for a given keyword, location and language

        Args:
            keyword (str): keyword to get suggestions for
            location_name (str): location name
            language_name (str): language name
            limit (int): limit of suggestions to get

        Returns:
            list of dictionaries with keyword, volume, location and language
        """
        post_data = dict()
        post_data[len(post_data)] = dict(
            keyword=keyword,
            location_name=location_name,
            language_name=language_name,
            include_serp_info=True,
            include_seed_keyword=True,
            limit=limit,
        )
        response = self.post(
            "/v3/dataforseo_labs/google/keyword_suggestions/live", post_data
        )
        if response is None:
            return None
        else:
            keywords = []
            for task in response["tasks"]:
                if "result" in task:
                    for result in task["result"]:
                        if "items" in result:
                            for item in result["items"]:
                                keyword = item["keyword"]
                                search_volume = item["keyword_info"]["search_volume"]
                                keywords.append(
                                    {
                                        "keywordEnriched": keyword,
                                        "keywordLocation": location_name,
                                        "keywordLanguage": language_name,
                                        "keywordVolume": search_volume,
                                        "offerRoot": "KEYWORD_SUGGESTION",
                                    }
                                )
            return keywords

    def get_related_keywords(self, keyword, location_name, language_name, limit=100):
        """Get related keywords for a given keyword, location and language

        Args:
            keyword (str): keyword to get suggestions for
            location_name (str): location name
            language_name (str): language name
            limit (int): limit of suggestions to get

        Returns:
            list of tuples with keyword and search volume"""
        post_data = dict()
        post_data[len(post_data)] = dict(
            keyword=keyword,
            location_name=location_name,
            language_name=language_name,
            limit=limit,
        )
        response = self.post(
            "/v3/dataforseo_labs/google/related_keywords/live", post_data
        )
        if response is None:
            return None
        else:
            keywords = []
            for task in response["tasks"]:
                if "result" in task:
                    for result in task["result"]:
                        if "items" in result:
                            for item in result["items"]:
                                keyword = item["keyword_data"]["keyword"]
                                search_volume = item["keyword_data"]["keyword_info"][
                                    "search_volume"
                                ]
                                keywords.append(
                                    {
                                        "keywordEnriched": keyword,
                                        "keywordLocation": location_name,
                                        "keywordLanguage": language_name,
                                        "keywordVolume": search_volume,
                                        "offerRoot": "RELATED_KEYWORD",
                                    }
                                )

            return keywords
