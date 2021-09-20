from typing import List

import pandas as pd

from sentinel.analyses.base import AnalysisBase, AnalysisFactory


@AnalysisFactory.register(name="low_confidence", description="Low confidence turns")
class ConfidenceFilter(AnalysisBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame):
        df = self.annotate(df, df.alternatives,
                           self._get_low_confidence, "low_confidence")

        return df

    def _get_low_confidence(self, item: List):
        if not item:
            return

        item = item[0]
        item = sorted(item, key=lambda x: x["confidence"])
        if item[0]["confidence"] < 95:
            return {"score": item[0]["confidence"]}
