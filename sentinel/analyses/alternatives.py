from typing import List

import pandas as pd

from sentinel.analyses.base import Analysis


class AlternativesFilter(Analysis):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame):
        df = self.annotate(df, df.alternatives,
                           self._get_none_alternatives, "annotation")

        return df

    def _get_none_alternatives(self, item: List):
        if not item:
            return {"no_alternatives": True}
