from ..extract_strategies import DataExtractStrategy
from ..load_strategies import DataLoadStrategy

class DataSource:
    """ 
    Represents a single source in the pipeline. In this default class, data will be
    moved from source to destination without any transformations applied. Subclasses
    only need to define the transform method to put data in a suitable format.
    """
    def __init__(self, extractor: DataExtractStrategy, loader: DataLoadStrategy):
        self.df = None
        self.extractor = extractor
        self.loader = loader
    
    def extract(self) -> int:
        """
        Returns: number of records read from the data source
        """
        self.df = self.extractor.extract()
        return len(self.df.index)

    def transform(self):
        """ 
        Perform operations (cleaning, drop rows, remove outliers) on raw data to 
        put it in a unified, usable form.
        """
        # transformations/cleaning
        pass

    def load(self) -> int:
        """
        Returns: number of records written to the target repo for this source
        """
        # changes are made to the original dataframe, so use it directly
        self.loader.load(self.df)
        return len(self.df.index)
