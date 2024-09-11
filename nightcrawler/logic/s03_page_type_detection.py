import os
import joblib
from typing import Dict

from nightcrawler.logic.s00_base import BaseLogic

class PageTypes:
    ECOMMERCE_PRODUCT = "ecommerce_product"
    ECOMMERCE_OTHER = "ecommerce_other"
    WEB_PRODUCT_ARTICLE = "web_product_article"
    WEB_ARTICLE = "web_article"
    BLOGPOST = "blogpost"
    OTHER = "other"

class PageTypeDetectionZyteLogic(BaseLogic):
    DEFAULT_THRESHOLD = 0.4

    def __init__(self, *args, **kwargs):
        self.threshold = kwargs.get("threshold", self.DEFAULT_THRESHOLD)
        
    def apply_one(self, item: Dict) -> Dict:
        zyte_probability = item.get("zyte_probability", None)

        if not zyte_probability:
            raise ValueError("Item does not contain Zyte probability")
        
        page_type = PageTypes.OTHER
        if zyte_probability > self.threshold:
            page_type = PageTypes.ECOMMERCE_PRODUCT

        result = {
            "page_type": page_type
        }

        return result

class PageTypeDetectionBinaryModelLogic(BaseLogic):
    DEFAULT_THRESHOLD = 0.5

    def __init__(self, *args, **kwargs):
        self.path_to_pipeline = kwargs.get("path_to_pipeline", None)
        self.path_to_pipeline = os.environ.get("PATH_TO_PIPELINE", self.path_to_pipeline)

        self.threshold = kwargs.get("threshold", self.DEFAULT_THRESHOLD)
        
        self.pipeline = self._load_pipeline(self.path_to_pipeline)

    def _load_pipeline(self, path_to_pipeline: str) -> None:
        if not path_to_pipeline:
            raise ValueError("Path to pipeline is not provided")

        pipeline = joblib.load(path_to_pipeline)

        return pipeline

    def apply_one(self, item: Dict) -> Dict:
        html = item.get("html", None)

        if not html:
            raise ValueError("Item does not contain HTML content")
        
        page_type = PageTypes.OTHER
        proba = self.pipeline.predict_proba([html])[0][1]

        if proba > self.threshold:
            page_type = PageTypes.ECOMMERCE_PRODUCT

        result = {
            "page_type": page_type
        }

        return result