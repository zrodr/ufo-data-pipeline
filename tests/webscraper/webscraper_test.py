import pytest

from datapipeline.webscraper.ufosightingwebscraper import UFOSightingWebScraper

@pytest.fixture(scope="module")
def webscraper():
    return UFOSightingWebScraper("https://nuforc.org/webreports")


def test_request_nonexistent_url_path(webscraper):
    res = webscraper.get_html_content(f"{webscraper.base_url}/notfound")
    assert res is None


def test_request_nonexistent_base_url(webscraper):
    res = webscraper.get_html_content("https://nonexistentwebsite.com")
    assert res is None


def test_request_http_url(webscraper):
    res = webscraper.get_html_content(
        "http://codedamn-classrooms.github.io/webscraper-python-codedamn-classroom-website/"
    )
    
    # expecting requests library to convert from http to https
    assert res is not None
    assert not res.is_xml


@pytest.mark.parametrize(
    "url",
    [
        "mailto:zrodr@protonmail.com",
        "ftp://ftp.myurl.com",
        "file://path/to/file"
    ]
)
def test_request_non_http_uri_schemes(webscraper, url):
    # behavior of get_html_content is to swallow errors for non-existent urls,
    # unsupported URI schemes, etc. and simply return nothing if the html doc
    # is unobtainable
    res = webscraper.get_html_content(url)
    assert res is None


def test_rate_limit_except_on_invalid_args(webscraper):
    with pytest.raises(ValueError) as e:
        webscraper.rate_limit(seconds=1.5, random=True, randbetween=(5.0, 11.0))


def test_rate_limit_negative_sec_values(webscraper):
    explicit_delay = webscraper.rate_limit(seconds=-1.0)
    random_delay = webscraper.rate_limit(random=True, randbetween=(-5.0, -10.0))
    
    assert explicit_delay > 0 and random_delay > 0
    assert random_delay > 5 and random_delay < 10 


def test_rate_limit_default_randrange(webscraper):
    default_range = (2.0, 10.0)
    delay = webscraper.rate_limit(random=True)

    assert delay >= default_range[0] and delay <= default_range[1]


def test_rate_limit_user_randrange(webscraper):
    user_range = (1.2, 5.0)
    delay = webscraper.rate_limit(
        random=True, randbetween=(user_range[0], user_range[1])
    )

    assert delay >= user_range[0] and delay <= user_range[1]