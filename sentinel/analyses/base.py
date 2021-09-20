"""
Base class for all monitoring and analysis plugins
"""
from typing import Optional, Callable
from abc import ABC, abstractmethod

import pandas as pd

from sentinel.types import T


class AnalysisBase(ABC):
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


class AnalysisFactory:
    """
    Factory class for creating executors
    """

    # registry for available executors
    registry = {}

    @classmethod
    def register(cls, name: str, description: str) -> Callable:
        """
        Class method to register AnalysisFactory class to the internal
        registry.

        :returns: The analysis class
        """

        def inner_wrapper(wrapped_class: AnalysisBase) -> Callable:
            if name in cls.registry:
                print("Executor {name} already exists. Replacing it")
            cls.registry[name] = {"class": wrapped_class,
                                  "description": description}
            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_executor(cls, name: str, **kwargs) -> AnalysisBase:
        """
        Factory command to create the executor.  This method gets the
        appropriate Analysis class from the registry and creates an
        instance of it, while passing in the parameters given in ``kwargs``.

        :returns: An instance of the executor that is created.
        """

        if name not in cls.registry:
            print(f"Executor {name} does not exist in the registry")
            return None

        exec_class = cls.registry[name].get("class")
        executor = exec_class(**kwargs)

        return executor
