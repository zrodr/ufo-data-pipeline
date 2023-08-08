from abc import ABC, abstractmethod
import pandas as pd
import sqlite3 as sqlite

class DataLoadStrategy(ABC):
    @property
    @abstractmethod
    def resource_name(self) -> str: pass

    @abstractmethod
    def load(self, data: pd.DataFrame) -> None:
        """ 
        Write a dataframe to the target repository
        """
        pass

#
#   Supported concrete write targets
#
class SQLiteLoadStrategy(DataLoadStrategy):
    def __init__(self, db_path: str) -> None:
        super().__init__()
        self.db_path = db_path
        self.conn: sqlite.Connection = None

    @property
    def resource_name(self): return self.db_path

    def load(self, data: pd.DataFrame):
        self.conn = sqlite.connect(self.db_path)
        c = self.conn.cursor()

        c.execute(
            """
            create table if not exists sightings (
                Date text,
                Time text,
                City text,
                State text,
                Country text,
                Shape text,
                Duration text,
                Summary text,
                Posted text,
                Images text
            )
            """
        )
        self.conn.commit()

        data.to_sql('sightings', self.conn, if_exists='replace')
        
        c.close()
        self.conn.close()


class FileLoadStrategy(DataLoadStrategy):
    """
    Base class for data to be written on disk. All strategies write to files in 
    append mode to allow multiple sources to write to the same target.
    """
    def __init__(self, out_filepath: str):
        super().__init__()
        self._out_filepath = out_filepath

    def file_exists(self) -> bool:
        from os.path import exists
        return exists(self._out_filepath)
    
    @property
    def resource_name(self): return self._out_filepath


class CSVLoadStrategy(FileLoadStrategy): 
    def load(self, data: pd.DataFrame):
        # make sure that the headers are not written twice if the file exists.
        data.to_csv(
            self._out_filepath,
            mode="a",
            index=False,
            header=not self.file_exists()
        )


class ExcelLoadStrategy(FileLoadStrategy):
    def load(self, data: pd.DataFrame):
        if self.file_exists():
            with pd.ExcelWriter(
                self._out_filepath,
                mode="a",
                if_sheet_exists="overlay"
            ) as w:
                data.to_excel(w, index=False, startrow=w.sheets['Sheet1'].max_row, header=None)
        else:
            with pd.ExcelWriter(
                self._out_filepath,
                mode="w",
            ) as w:
                data.to_excel(w, index=False)


class JSONLoadStrategy(FileLoadStrategy):
    def load(self, data: pd.DataFrame):
        # Writes to a line-delimited JSON file (allows appending)
        data.to_json(
            self._out_filepath,
            orient='records',
            lines=True,
            mode='a',
            # TODO figure out date formats
            date_format="iso"
        )
