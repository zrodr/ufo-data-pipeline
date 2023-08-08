from abc import ABC, abstractmethod
import pandas as pd

from datapipeline.webscraper.webscraper import WebScraper


class DataExtractStrategy(ABC):
    @property
    @abstractmethod
    def resource_name(self): pass

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """ 
        Read data from the chosen source into a pandas dataframe
        """
        pass


#
#   Supported concrete forms of data extraction
#
class WebScraperExtractStrategy(DataExtractStrategy):
    def __init__(self, scraper: WebScraper) -> None:
        super().__init__()
        self.scraper = scraper

    @property
    def resource_name(self): return self.scraper.base_url

    def extract(self) -> pd.DataFrame:
        return pd.DataFrame(self.scraper.scrape())


class MySQLExtractStrategy(DataExtractStrategy):
    def __init__(self, host: str, database: str, user: str, password: str) -> None:
        super().__init__()
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    @property
    def resource_name(self): return f'{self.host}/{self.database}'

    def extract(self) -> pd.DataFrame:
        # TODO
        return pd.DataFrame()


class FileExtractStrategy(DataExtractStrategy):
    """ Base class for input files on disk """
    def __init__(self, in_filepath: str) -> None:
        super().__init__()
        self._in_filepath = in_filepath
    
    @property
    def resource_name(self): return self._in_filepath


class ExcelExtractStrategy(FileExtractStrategy):
    def extract(self) -> pd.DataFrame:
        return pd.read_excel(self._in_filepath, converters={"Date": str})


class CSVExtractStrategy(FileExtractStrategy):
    def extract(self) -> pd.DataFrame:
        return pd.read_csv(self._in_filepath)


class JSONExtractStrategy(FileExtractStrategy):
    def extract(self) -> pd.DataFrame:
        return pd.read_json(
            self._in_filepath,
            orient="records",
            lines=True,
        )
