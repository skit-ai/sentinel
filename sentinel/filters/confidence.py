"""
Filters to flag out low confidence calls.
"""
from typing import List, Dict, Union

import pandas as pd

from sentinel.filters.base import FilterBase, FilterFactory


@FilterFactory.register(name="low_asr_confidence", description="Low ASR confidence turns")
class ConfidenceFilter(FilterBase):
    def __init__(self, *args, **kwargs):
        self.confidence_threshold = kwargs.get("confidence_threshold", 95)

        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.annotate(df, df.alternatives,
                           self._get_low_confidence, "low_asr_confidence")

        return df

    def _get_low_confidence(self, item: List) -> Union[Dict[str, float], None]:
        """
        Flag turn if alternatives list has confidence less than some threshold.

        Parameters:
            item (list): Alternatives list.

        Returns:
            Metadata dict with annotation.
        """
        if not item:
            return None

        item = item[0]
        item = sorted(item, key=lambda x: x["confidence"], reverse=True)
        if item[0]["confidence"] < self.confidence_threshold:
            return {"score": item[0]["confidence"]}
        return None
