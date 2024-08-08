"""
This file holds all configuration options.
"""

from datetime import datetime
from nightcrawler.settings import Settings


class Context:
    def __init__(self, **kwargs):
        self.settings = Settings()

        today = datetime.now()
        today_ts = today.strftime("%Y-%m-%d_%H-%M-%S")

        # ----------------------------------------------------------------------------------------
        # Scraping
        # ----------------------------------------------------------------------------------------
        self.output_path = "./data/output"
        self.serpapi_filename = f"serpapi_urls_{today_ts}.json"
        self.diffbot_filename = f"diffbot_urls_{today_ts}.json"

        # ----------------------------------------------------------------------------------------
        # Modelling
        # ----------------------------------------------------------------------------------------

        # ----------------------------------------------------------------------------------------
        # Storage
        # ----------------------------------------------------------------------------------------
