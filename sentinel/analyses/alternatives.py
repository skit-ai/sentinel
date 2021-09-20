from typing import List

import pandas as pd

from sentinel.analyses.base import AnalysisBase, AnalysisFactory


@AnalysisFactory.register(name="no_alternatives", description="Turns with no alternatives")
class AlternativesFilter(AnalysisBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame):
        df = self.annotate(df, df.alternatives,
                           self._get_none_alternatives, "no_alternatives")

        return df

    def _get_none_alternatives(self, item: List):
        if not item:
            return {"no_alternatives": True}
