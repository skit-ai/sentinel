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
    def _jsonify(item):
        if isinstance(item, str):
            return json.loads(item)
        else:
            return []

    def _post_read(self, df: pd.DataFrame):
        df.alternatives = df.alternatives.apply(self._jsonify)

        return df

    def read(self):
        df = pd.read_csv(self.path, encoding="utf-8")
        df = self._post_read(df)

        return df

