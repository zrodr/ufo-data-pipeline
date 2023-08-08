from requests import get, ConnectionError, HTTPError, Timeout, RequestException
from bs4 import BeautifulSoup
from time import sleep
from random import uniform

class WebScraper:
    """ """
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def get_html_content(self, url: str) -> BeautifulSoup | None:
        """
        Utility method to handle a single http request to a url.
        Params:
            url: 
        Returns:
            BeautifulSoup object on successful requests, None otherwise
        """
        try:
            response = get(url, timeout=8.0)
            # catch http error codes, 404, 501, etc
            response.raise_for_status()
        except (ConnectionError, HTTPError, Timeout, RequestException) as e:
            return None

        return BeautifulSoup(response.content, "html.parser")

    def _extract_data_from_html(self, soup: BeautifulSoup) -> list[dict]:
        """
        Searches the DOM tree for the desired data.
        Meant to be overridden by subclasses to fit specific html docs.
        """
        pass

    def rate_limit(
        self,
        seconds: float=None,
        random=False,
        randbetween: tuple[float, float]=None
    ) -> float:
        """
        Get an explicit or random time (sec) value to pause the webscraper. 
        When scraping multiple urls, utilize random generation to avoid detection
        and request at a more "natural" rate.

        Returns:
            float: the delay in seconds, guaranteed positive
        """
        if seconds and (random or randbetween):
            # prevent all 3 values being passed in
            raise ValueError(
            "rate_limit() expects parameters (seconds) XOR (random,randbetween)"
            )

        delay: float = seconds
        if random and not randbetween:
            delay = round(uniform(2.0, 10.0), 3)
        elif random and randbetween:
            delay = round(uniform(randbetween[0], randbetween[1]), 3)

        return abs(delay)

    def delay(self, seconds: float) -> None:
        """
        Pauses the web scraper to avoid 429 errors and rate limiting. Use with
        WebScraper.rate_limit() to generate time values.
        """
        sleep(seconds)


    def scrape(self) -> list[dict]:
        """
        Run the scraping operation on a url or set of pages from a url, extracting
        data according to its implementation of _extract_data_from_html
        """
        pass