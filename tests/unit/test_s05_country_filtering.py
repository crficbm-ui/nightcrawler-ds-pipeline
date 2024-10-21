import pytest
from unittest.mock import patch, MagicMock

from nightcrawler.base import PipelineResult, CrawlResultData, CountryFilteringData
from helpers.context import Context
from nightcrawler.process.s05_country_filterer import CountryFilterer, BaseCountryFilterer


# ---------------------------------------------------
# Tests for Switzerland
# ---------------------------------------------------


@pytest.fixture
def sample_previous_pipeline_result_ch():
    """Fixture to create a sample PipelineResult object of previous step (step 4 zyte extraction) with mock results, for Switzerland."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE", # offerRoot is mandatory
                                 url="u1.ch"),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.at")]
    )


@pytest.fixture
def sample_country_filtering_result_ch():
    """Fixture to create a sample Country filtering result, for Switzerland."""
    return [CountryFilteringData(offerRoot="GOOGLE", # offerRoot is mandatory
                                 url="u1.ch",
                                 domain="u1.ch",
                                 filtererName="url",
                                 deliveringToCountry=1,
                                 ),
            CountryFilteringData(offerRoot="GOOGLE",
                                 url='u2.at',
                                 domain='u2.at', 
                                 filtererName='unknown', 
                                 deliveringToCountry=0,
                                 ),
        ]



@patch.object(BaseCountryFilterer, "save_new_known_domains")
def test_get_step_results_country_filterer_ch(mock_save_new_known_domains, sample_previous_pipeline_result_ch, sample_country_filtering_result_ch):
    """Test the get_step_results method for Country filtering, for Switzerland."""
    # Mock save_new_known_domains
    mock_save_new_known_domains.return_value = None

    # Init context
    context = Context()
    
    # Init country filterer
    country_filterer = CountryFilterer(context=context, country="CH")

    # Apply country filtering
    country_filtering_result_ch = country_filterer.get_step_results(sample_previous_pipeline_result_ch)
    print(country_filtering_result_ch)
    print(sample_country_filtering_result_ch)

    # Check country filerer worked
    assert(country_filtering_result_ch == sample_country_filtering_result_ch)


# ---------------------------------------------------
# Tests for Austria
# ---------------------------------------------------


@pytest.fixture
def sample_previous_pipeline_result_at():
    """Fixture to create a sample PipelineResult object of previous step (step 4 zyte extraction) with mock results, for Austria."""
    return PipelineResult(
        meta=MagicMock(),
        results=[CrawlResultData(offerRoot="GOOGLE", # offerRoot is mandatory
                                 url="u1.at"),
                CrawlResultData(offerRoot="GOOGLE",
                                url="u2.ch")]
    )


@pytest.fixture
def sample_country_filtering_result_at():
    return [CountryFilteringData(offerRoot="GOOGLE", # offerRoot is mandatory
                                 url="u1.at",
                                 domain="u1.at",
                                 filtererName="url",
                                 deliveringToCountry=1,
                                 ),
            CountryFilteringData(offerRoot="GOOGLE",
                                 url='u2.ch',
                                 domain='u2.ch', 
                                 filtererName='url', # .ch is included in domains to be classified as positive in UrlCountryFilterer for AT/Austria
                                 deliveringToCountry=1, 
                                 ),
        ]



@patch.object(BaseCountryFilterer, "save_new_known_domains")
def test_get_step_results_country_filterer_at(mock_save_new_known_domains, sample_previous_pipeline_result_at, sample_country_filtering_result_at):
    """Test the get_step_results method for Country filtering, for Austria."""
    # Mock save_new_known_domains
    mock_save_new_known_domains.return_value = None

    # Init context
    context = Context()
    
    # Init country filterer
    country_filterer = CountryFilterer(context=context, country="AT")

    # Apply country filtering
    country_filtering_result_at = country_filterer.get_step_results(sample_previous_pipeline_result_at)
    print(country_filtering_result_at)
    print(sample_country_filtering_result_at)

    # Check country filerer worked
    assert(country_filtering_result_at == sample_country_filtering_result_at)

