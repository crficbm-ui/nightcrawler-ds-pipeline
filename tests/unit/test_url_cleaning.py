from nightcrawler.helpers.utils import remove_tracking_parameters


def test_remove_tracking():
    base = "https://a.ch/some/levels/"
    url = base + "?srsltid=AfmBOoqCctfdXwRhSCqHCHO3sxdW0PCZaXKHkAUqBwe6PeQClu7nGATWc7s"
    assert remove_tracking_parameters(base) == base
    assert remove_tracking_parameters(url) == base
    assert remove_tracking_parameters(url + "&aa=bb") == base + "?aa=bb"
    assert remove_tracking_parameters(url + "&aa=bb&utm_source=xx") == base + "?aa=bb"
    assert (
        remove_tracking_parameters(url + "&aa=bb&source=xx")
        == base + "?aa=bb&source=xx"
    )

    # Check removes all parameters for ebay
    base = "https://www.ebay.ch/some/levels/"
    assert remove_tracking_parameters(base) == base
    assert remove_tracking_parameters(base + "?a=b&c=d") == base
