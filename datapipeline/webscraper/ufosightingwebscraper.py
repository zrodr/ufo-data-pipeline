from .webscraper import WebScraper

class UFOSightingWebScraper(WebScraper):
    def __init__(self, base_url: str) -> None:
        super().__init__(base_url)

    def _extract_data_from_html(self, soup) -> list[dict]:
        data_table = soup.find('table')
        column_names = [el.text for el in data_table.select('thead font')]

        return [
            dict(zip(column_names, [
                row.select_one('td:nth-of-type(1) > a').text,
                row.select_one('td:nth-of-type(2)').text,
                row.select_one('td:nth-of-type(3)').text,
                row.select_one('td:nth-of-type(4)').text,
                row.select_one('td:nth-of-type(5)').text,
                row.select_one('td:nth-of-type(6)').text,
                row.select_one('td:nth-of-type(7)').text,
                row.select_one('td:nth-of-type(8)').text,
                row.select_one('td:nth-of-type(9)').text,
            ]))
            for row in data_table.select('tbody tr')
        ]

    def scrape(self) -> list[dict]:
        # for the pages corresponding to specific months, use the extension
        # /ndxe{yyyy}{mm}.html (e.g. /ndxe202208.html)
        records: list[dict] = []
        urls = [
            f"{self.base_url}/ndxe2022{month:02}.html" for month in range(1, 13)
        ]
        
        for url in urls:
            soup = self.get_html_content(url)
            if soup is None:
                # something went wrong during the request, skip it
                print(f"Skipping page: {url}", flush=True)
                continue

            data = self._extract_data_from_html(soup)
            records = [*records, *data]

            print(f"Parsed url {url}")
            delay_period = self.rate_limit(random=True, randbetween=(2.0, 4.0))
            print(f"waiting {delay_period}s", flush=True)
            self.delay(delay_period)
        
        return records