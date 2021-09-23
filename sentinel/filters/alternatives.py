"""
Filters to flag out turns with no alternatives.
"""
from typing import List, Dict, Union

import pandas as pd

from sentinel.analyses.base import AnalysisBase, AnalysisFactory


@AnalysisFactory.register(name="no_alternatives", description="Turns with no alternatives")
class AlternativesFilter(AnalysisBase):
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
