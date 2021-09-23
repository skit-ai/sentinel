from typing import Callable
from abc import ABC, abstractmethod

import pandas as pd


class FilterBase(ABC):
    """
    Base class for anomaly filters.

    Attributes:
        successor (FilterBase): filter function instances implemented from
                                  this base class.

    Methods:
        handle(df: pd.DataFrame): Calls filter functions chained as its successor.
        preprocess(df: pd.DataFrame): :TODO:
        process(df: pd.DataFrame): Process serving sample (untagged call/turn dataframe).
    """

    def __init__(self, successor: "FilterBase" = None, **kwargs):
        self.successor = successor

    def handle(self, df: pd.DataFrame):
        """
        Calls filter functions chained as its successor.

        Parameters:
            df (pd.DataFrame): dataframe.
        """
        df = self.process(df)

        if self.successor:
            self.successor.handle(df)
        return df

    @abstractmethod
    def preprocess(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def process(self, df: pd.DataFrame):
        """
        Process serving sample (untagged call/turn dataframe).

        Parameters:
            df (pd.DataFrame): serving sample dataframe.
        """
        pass

    def annotate(self, df: pd.DataFrame,
                 df_series: pd.Series,
                 func: Callable, annotation: str) -> pd.DataFrame:
        """
        Annotate dataframe elements by running it through an annotation function.

        Parameters:
            df (pd.DataFrame): serving sample dataframe.
            df_series (pd.Series): source column to derive annotations from.
            func (Callable): the annotation function.
            annotation (str): name of the annotation column in the dataframe.

        Returns:
            pd.DataFrame: annotated dataframe
        """
        df[annotation] = df_series.apply(func)

        return df


class FilterFactory:
    """
    Factory class for creating executors.

    Attributes:
        None

    Methods:
        register(name: str, description: str): Class method to register
                                               FilterFactory class to the internal registry.
        create_executor(name: str): Factory command to create the executor.
    """

    # registry for available executors
    registry = {}

    @classmethod
    def register(cls, name: str, description: str) -> Callable:
        """
        Class method to register FilterFactory class to the internal
        registry.

        Returns:
            Callable: The filter class.
        """

        def inner_wrapper(wrapped_class: FilterBase) -> Callable:
            if name in cls.registry:
                print("Executor {name} already exists. Replacing it")
            cls.registry[name] = {"class": wrapped_class,
                                  "description": description}
            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_executor(cls, name: str, **kwargs) -> FilterBase:
        """
        Factory command to create the executor.  This method gets the
        appropriate Filter class from the registry and creates an
        instance of it, while passing in the parameters given in ``kwargs``.

        Returns:
            FilterBase: An instance of the executor that is created.
        """

        if name not in cls.registry:
            print(f"Executor {name} does not exist in the registry")
            return None

        exec_class = cls.registry[name].get("class")
        executor = exec_class(**kwargs)

        return executor
