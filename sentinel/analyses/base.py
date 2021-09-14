"""
Base class for all monitoring and analysis plugins
"""

from abc import ABC, abstractmethod

from sentinel.types import DataFrame


class Analysis(ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def preprocess(self, df: DataFrame):
        pass

    @abstractmethod
    def process(self, df: DataFrame):
        pass

