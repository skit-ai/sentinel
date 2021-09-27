from typing import Optional, List
from abc import ABC, abstractmethod

import pandas as pd

from sentinel.types import T


class Exporter(ABC):
    """
    Base class for report exporters.

    Attributes:
        None

    Methods:
        export_report(df: pd.DataFrame, categories): export report for different filters applied.
    """

    def __init__(self, successor: Optional[T] = None):
        self.successor = successor

    @abstractmethod
    def export_report(self, df: pd.DataFrame, categories: List[str]):
        """
        Export report for different filters applied.

        Parameters:
            df (pd.DataFrame): dataframe.
            categories (list): list of filters applied on the dataframe.

        Returns:
            Dict[str, io.StringIO]: map of filter function and associated dataframe buffer.
        """
        pass
