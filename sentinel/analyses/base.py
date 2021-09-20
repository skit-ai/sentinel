"""
Base class for all monitoring and analysis plugins
"""
from typing import Optional, Callable
from abc import ABC, abstractmethod

import pandas as pd

from sentinel.types import T


class Analysis(ABC):
    def __init__(self, successor: Optional[T] = None):
        self.successor = successor

    def handle(self, df: pd.DataFrame):
        df = self.process(df)
        if self.successor:
            self.successor.handle(df)
        return df

    @abstractmethod
    def preprocess(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def process(self, df: pd.DataFrame):
        pass

    def annotate(self, df: pd.DataFrame,
                 df_series: pd.Series,
                 func: Callable, annotation: str) -> pd.DataFrame:
        df[annotation] = df_series.apply(func)

        return df
