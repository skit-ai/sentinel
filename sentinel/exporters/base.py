from typing import Optional, List, Callable
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

    def handle(self, df_list: List[pd.DataFrame], filters: List[str]):
        """
        Calls exporter functions chained as its successor.

        Parameters:
            df_list (list): list of dataframes.
            filters (list): list of filters applied.
        """
        self.export_report(df_list, filters)

        if self.successor:
            self.successor.handle(df_list, filters)

        return df_list

    @abstractmethod
    def export_report(self, df_list: List[pd.DataFrame], categories: List[str]):
        """
        Export report for different filters applied.

        Parameters:
            df_list (list): list of dataframes.
            categories (list): list of filters applied on the dataframe.

        Returns:
            Dict[str, io.StringIO]: map of exporter function and associated dataframe buffer.
        """
        pass


class ExporterFactory:
    """
    Factory class for creating executors.

    Attributes:
        None

    Methods:
        register(exporter_name: str, description: str): Class method to register
                                                        ExporterFactory class to the internal registry.
        create_executor(exporter_name: str): Factory command to create the executor.
    """

    # registry for available executors
    registry = {}

    @classmethod
    def register(cls, exporter_name: str, description: str) -> Callable:
        """
        Class method to register ExporterFactory class to the internal
        registry.

        Returns:
            Callable: The Exporter class.
        """

        def inner_wrapper(wrapped_class: Exporter) -> Callable:
            if exporter_name in cls.registry:
                print("Executor {exporter_name} already exists. Replacing it")
            cls.registry[exporter_name] = {"class": wrapped_class,
                                           "description": description,
                                           "verbose": wrapped_class.__doc__}
            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_executor(cls, exporter_name: str, **kwargs) -> Exporter:
        """
        Factory command to create the executor.  This method gets the
        appropriate Exporter class from the registry and creates an
        instance of it, while passing in the parameters given in ``kwargs``.

        Returns:
            Exporter: An instance of the executor that is created.
        """

        if exporter_name not in cls.registry:
            print(f"Executor {exporter_name} does not exist in the registry")
            return None

        exec_class = cls.registry[exporter_name].get("class")
        executor = exec_class(**kwargs)

        return executor
