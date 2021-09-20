from typing import Optional
from abc import ABC, abstractmethod

import pandas as pd

from sentinel.types import T


class Exporter(ABC):
    def __init__(self, successor: Optional[T] = None):
        self.successor = successor

    @abstractmethod
    def export_report(self, df: pd.DataFrame):
        pass
