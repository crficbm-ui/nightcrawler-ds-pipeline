import pytest
from unittest.mock import patch, MagicMock

from nightcrawler.base import PipelineResult, CrawlResultData, CountryFilteringData
from helpers.context import Context
from nightcrawler.process.s05_country_filterer import CountryFilterer, MasterCountryFilterer


@pytest.fixture
def sample_previous_pipeline_result():
    """Fixture to create a sample PipelineResult object with mock results."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE",
                                 url="u1.ch"),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.at")]
    )


@pytest.fixture
def sample_pipeline_result_ch():
    return [CountryFilteringData(offerRoot="GOOGLE",
                                 url="u1.ch",
                                 keywordEnriched=None,
                                 keywordVolume=-1,
                                 keywordLanguage=None, 
                                 keywordLocation=None,
                                 imageUrl=None,
                                 price=None, 
                                 title=None,
                                 fullDescription=None,
                                 zyteExecutionTime=0.0, 
                                 html=None, 
                                 zyteProbability=0.0,
                                 domain="u1.ch",
                                 filtererName="url",
                                 deliveringToCountry=1,
                                 ),
            CountryFilteringData(offerRoot='GOOGLE', 
                            url='u2.at', 
                            keywordEnriched=None, 
                            keywordVolume=-1, 
                            keywordLanguage=None, 
                            keywordLocation=None, 
                            imageUrl=None, 
                            price=None, 
                            title=None, 
                            fullDescription=None, 
                            zyteExecutionTime=0.0, 
                            html=None, 
                            zyteProbability=0.0, 
                            domain='u2.at', 
                            filtererName='unknown', 
                            deliveringToCountry=0,
                            ),
        ]

# @pytest.fixture
# @patch('nightcrawler.process.s05_country_filterer.MasterCountryFilterer.save_new_known_domains')  # Patch save_new_known_domains in MasterCountryFilterer
# def master_country_filterer(mock_save_new_known_domains):
#     mock_client = MagicMock()
#     mock_save_new_known_domains.return_value = mock_client

#     return MasterCountryFilterer()


def test_apply_step_country_filterer_ch(sample_previous_pipeline_result, sample_pipeline_result_ch):
    """Test the apply_step method for Zyte detection."""
    # Init context
    context = Context()
    
    # Init country filterer
    country_filterer = CountryFilterer(context=context, country="CH")

    # Apply country filtering
    pipeline_result_ch = country_filterer.get_step_results(sample_previous_pipeline_result)
    print(pipeline_result_ch)
    print(sample_pipeline_result_ch)

    # Check country filerer worked
    assert(pipeline_result_ch == sample_pipeline_result_ch)
