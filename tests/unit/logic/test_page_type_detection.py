from unittest import TestCase
from unittest.mock import patch, MagicMock

from nightcrawler.logic.s03_page_type_detection import (
    PageTypeDetectionZyteLogic,
    PageTypes,
    PageTypeDetectionBinaryModelLogic
)



class TestPageTypeDetectionZyteLogic(TestCase):
    def setUp(self):
        self.page_type_cls = PageTypeDetectionZyteLogic()

    def test_model_predict_ecommerce_product(self):
        result = self.page_type_cls.apply_one({"zyte_probability": 0.8})
        
        self.assertEqual(result, {'page_type': PageTypes.ECOMMERCE_PRODUCT})

    def test_model_predict_other(self):
        result = self.page_type_cls.apply_one({"zyte_probability": 0.2})
        
        self.assertEqual(result, {'page_type': PageTypes.OTHER})


class TestPageTypeDetectionBinaryModelLogic(TestCase):
    def setUp(self):
        self.mock_model = MagicMock()

        patcher = patch.object(PageTypeDetectionBinaryModelLogic, '_load_pipeline', return_value=self.mock_model)

        self.mock_load_pipeline = patcher.start()

        self.page_type_cls = PageTypeDetectionBinaryModelLogic("path/to/fake/model")

        # Ensures that the patch is stopped after the test
        self.addCleanup(patcher.stop)

    def test_model_predict_ecommerce_product(self):
        self.mock_model.predict_proba.return_value = [[0.3, 0.7]]

        result = self.page_type_cls.apply_one({"html": "<html><body>test content</body></html>"})
        
        self.assertEqual(result, {'page_type': PageTypes.ECOMMERCE_PRODUCT})

    def test_model_predict_other(self):
        self.mock_model.predict_proba.return_value = [[0.7, 0.3]]

        result = self.page_type_cls.apply_one({"html": "<html><body>test content</body></html>"})
        
        self.assertEqual(result, {'page_type': PageTypes.OTHER})
