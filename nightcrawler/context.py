from typing import Any
from nightcrawler.helpers.utils import create_output_dir
from nightcrawler.settings import Settings
from datetime import datetime

try:
    from libnightcrawler.context import Context as StorageContext
except ImportError:
    StorageContext = object


class Context(StorageContext):
    """
    Context class that holds configuration options for the application.

    Attributes:
        settings (Settings): An instance of the Settings class that holds application-specific settings.
        output_path (str): The directory path where output files will be saved.
        serpapi_filename (str): The filename for storing URLs retrieved from Serpapi, including a timestamp.
        zyte_filename (str): The filename for storing URLs retrieved from zyte, including a timestamp.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializes the Context with default configuration options.

        Args:
            **kwargs (Any): Additional keyword arguments that might be used to customize the context.
        """
        super().__init__()
        self.settings = Settings()
        self.today = datetime.now()
        self.today_ts = self.today.strftime("%Y-%m-%d_%H-%M-%S")
        self.crawlStatus: str = "processing"

        # ----------------------------------------------------------------------------------------
        # Scraping
        # ----------------------------------------------------------------------------------------
        self.output_path: str = "./data/output"
        self.serpapi_filename_keyword_enrichement: str = (
            "extract_keyword_enrichement.json"
        )
        self.serpapi_filename_reverse_image_search: str = (
            "extract_serpapi_reverse_image_search.json"
        )
        self.serpapi_filename: str = "extract_serpapi_keywords.json"
        self.zyte_filename: str = "extract_zyte.json"
        # ----------------------------------------------------------------------------------------
        # Processing
        # ----------------------------------------------------------------------------------------
        self.processing_filename_raw: str = "process_raw.json"
        self.processing_filename_filtered: str = "process_filtered_country.json"
        self.processing_filename_country_filtering: str = (
            "process_country_filtering.json"
        )
        self.processing_filename_delivery_policy: str = "process_delivery_policy.json"
        self.processing_filename_page_type_detection: str = "process_page_type.json"
        self.processing_filename_blocked_content_detection: str = (
            "process_blocked_content.json"
        )
        self.processing_filename_content_domain_detection: str = (
            "process_content_domain.json"
        )
        self.processing_filename_suspiciousness_classifier: str = (
            "process_suspiciousness.json"
        )
        self.filename_final_results: str = "final_results.json"

    def update_output_dir(self, path: str):
        self.output_dir = create_output_dir(path, self.output_path, skip=not self.settings.use_file_storage)
