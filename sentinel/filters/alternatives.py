"""
Filters to flag out turns with no alternatives.
"""
from typing import List, Dict, Union

import pandas as pd

from sentinel.filters.base import FilterBase, FilterFactory


@FilterFactory.register(name="no_alternatives", description="Turns with no alternatives")
class AlternativesFilter(FilterBase):
    """
    This filter reports turns with no alternatives.

    In most of the cases, these are turns where users are unresponsive.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.annotate(df, df.alternatives,
                           self._get_none_alternatives, "no_alternatives")

        return df

    def _get_none_alternatives(self, item: List) -> Union[Dict[str, bool], None]:
        """
        Flag turn if it doesn't have alternatives.

        Parameters:
            item (list): Alternatives list.

        Returns:
            Metadata dict with annotation.
        """
        if not item:
            return {"no_alternatives": True}


@FilterFactory.register(name="filter_words", description="Turns with certain words")
class WordFilter(FilterBase):
    """
    This filter can be used to flag out turns with certain words.

    Use the below keyword arguments in the config to specify configurable attributes.
    kwargs:
        word_list (list): List of words to be flagged out.
    """

    def __init__(self, *args, **kwargs):
        self.word_list = kwargs.get("word_list", [])

        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.annotate(df, df.alternatives,
                           self._get_filtered_transcripts, "filter_words")

        return df

    def _get_filtered_transcripts(self, item: List) -> Union[Dict[str, bool], None]:
        """
        Flag turn if it has any of the words mentioned in the config.

        Parameters:
            item (list): Alternatives list.

        Returns:
            Metadata dict with annotation.
        """
        if not item:
            return None

        item = item[0]
        item = sorted(item, key=lambda x: x["confidence"], reverse=True)

        if any(word in item[0]["transcript"] for word in self.word_list):
            return {"word_match": self.word_list}
        return None
