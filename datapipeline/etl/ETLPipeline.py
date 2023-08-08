from .datasources.datasource import DataSource 

class ETLPipeline:
    def __init__(self, data_sources: list[DataSource]) -> None:
        self.data_sources = data_sources
    
    def run(self):
        """ 
        Driver method for the data pipeline. Collects the data from each source
        in the pipeline and writes it to the target repository
        """
        records_written = 0

        for src in self.data_sources:
            src.extract()
            src.transform()
            records_written += src.load()
        
        print(f'Total new records: {records_written}')