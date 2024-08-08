from datetime import datetime
from typing import Any
from nightcrawler.settings import Settings

class Context:
    """
    Context class that holds configuration options for the application.
    
    Attributes:
        settings (Settings): An instance of the Settings class that holds application-specific settings.
        output_path (str): The directory path where output files will be saved.
        serpapi_filename (str): The filename for storing URLs retrieved from Serpapi, including a timestamp.
        diffbot_filename (str): The filename for storing URLs retrieved from Diffbot, including a timestamp.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializes the Context with default configuration options.

        Args:
            **kwargs (Any): Additional keyword arguments that might be used to customize the context.
        """
        self.settings = Settings()

        self.today = datetime.now()
        self.today_ts = self.today.strftime("%Y-%m-%d_%H-%M-%S")

        # ----------------------------------------------------------------------------------------
        # Scraping
        # ----------------------------------------------------------------------------------------
        self.output_path: str = "./data/output"
        self.serpapi_filename: str = f"serpapi_urls_{self.today_ts}.json"
        self.diffbot_filename: str = f"diffbot_urls_{self.today_ts}.json"

        # ----------------------------------------------------------------------------------------
        # Modelling
        # (Placeholder for future modelling attributes)
        # ----------------------------------------------------------------------------------------

        # ----------------------------------------------------------------------------------------
        # Storage
        # (Placeholder for future storage attributes)
        # ----------------------------------------------------------------------------------------
