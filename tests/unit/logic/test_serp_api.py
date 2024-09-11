
from unittest import TestCase
from unittest.mock import patch, MagicMock

from nightcrawler.logic.s01_serp import SerpLogic


class TestZyteLogic(TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.call_serpapi.return_value = {"organic_results": [{"link": "http://example.com/product1"}]}

        patcher = patch.object(SerpLogic, '_setup_client', return_value=self.mock_client)
        patcher.start()

        self.serp_logic = SerpLogic()

        # Ensures that the patch is stopped after the test
        self.addCleanup(patcher.stop)

    def test_apply_one(self):
        result = self.serp_logic.apply_one("aspirin", 3, False)

        self.assertEqual(result, ["http://example.com/product1"])

    def test_apply_one_with_empty_results(self):
        self.mock_client.call_serpapi.return_value = {}
        
        with self.assertRaises(ValueError):  
            result = self.serp_logic.apply_one("aspirin", 3, True)

    def test_apply_one_with_full_output(self):
        result = self.serp_logic.apply_one("aspirin", 3, True)

        self.assertEqual(result, [{"link": "http://example.com/product1"}])
