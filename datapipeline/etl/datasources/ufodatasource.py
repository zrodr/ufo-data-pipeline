from ..datasources.datasource import DataSource

class UFOSightingDataSource(DataSource):
    # Date / Time,City,State,Country,Shape,Duration,Summary,Posted,Images
    def transform(self):
        # split date and time into separate columns
        self.df[["Date", "Time"]] = self.df["Date / Time"].str.split(" ", expand=True)
        self.df.drop(columns=["Date / Time"], inplace=True)

        # get only sightings in the United States / Canada
        self.df.drop(
            self.df[~(self.df["Country"].isin(["USA", "Canada"]))].index,
            inplace=True
        )

        self.df = self.df[["Date","Time","City","State","Country","Shape","Duration","Summary","Posted","Images"]]
