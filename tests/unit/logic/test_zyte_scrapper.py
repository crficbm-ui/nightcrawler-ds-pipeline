
import base64

from unittest import TestCase
from unittest.mock import patch, MagicMock

from nightcrawler.logic.s01_zyte import ZyteLogic

class TestZyteLogic(TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.call_api.return_value = {}

        patcher = patch.object(ZyteLogic, '_setup_client', return_value=self.mock_client)
        patcher.start()

        self.zyte_logic = ZyteLogic()

        # Ensures that the patch is stopped after the test
        self.addCleanup(patcher.stop)

    def test_apply_one(self):
        result = self.zyte_logic.apply_one({"url": "http://example.com/product1"})
        
        self.assertEqual(result, {
            'url': 'http://example.com/product1',
            'price': '',
            'title': '',
            'full_description': '',
            'seconds_taken': '0',
            'html': None,
            'zyte_probability': None
        })

    def test_browserHtml(self):
        self.mock_client.call_api.return_value = {
            "browserHtml": "<html><body>test content</body></html>"
        }

        result = self.zyte_logic.apply_one({"url": "http://example.com/product1"})
        
        self.assertEqual(result, {
            'url': 'http://example.com/product1',
            'price': '',
            'title': '',
            'full_description': '',
            'seconds_taken': '0',
            'html': "<html><body>test content</body></html>",
            'zyte_probability': None
        })

    def test_httpResponseBody(self):
        self.mock_client.call_api.return_value = {
            "httpResponseBody": base64.b64encode(b"<html><body>test content</body></html>").decode()
        }

        result = self.zyte_logic.apply_one({"url": "http://example.com/product1"})
        
        self.assertEqual(result, {
            'url': 'http://example.com/product1',
            'price': '',
            'title': '',
            'full_description': '',
            'seconds_taken': '0',
            'html': "<html><body>test content</body></html>",
            'zyte_probability': None
        })