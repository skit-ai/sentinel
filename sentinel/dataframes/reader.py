"""
Input dataframe readers. Currently supports Protobuf and CSVs
"""
import json
from abc import ABC, abstractmethod

import pandas as pd


class Reader(ABC):
    def __init__(self, path: str):
        self.path = path

    @abstractmethod
    def read(self):
        """
        Read input dataframe (csv, protobuf etc.) to pandas dataframes.

        Returns:
            pd.DataFrame: pandas dataframe for input csv.
        """
        raise NotImplementedError


class ProtobufReader(Reader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def read(self):
        raise NotImplementedError


class CSVReader(Reader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def _jsonify(item: str):
        """
        Convert stringified json to native dictionary.

        Parameters:
            item (str): stringified json.
        """
        if isinstance(item, str):
            return json.loads(item)
        else:
            return []

    @staticmethod
    def _post_read(df: pd.DataFrame):
        """
        Post csv read transformations.

        Transformations done:
        - Convert stringified alternatives to native format.
        - ...

        Parameters:
            df (pd.DataFrame): call dataframe.
        """
        df.alternatives = df.alternatives.apply(CSVReader._jsonify)

        return df

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.path, encoding="utf-8")
        df = self._post_read(df)

        return df

