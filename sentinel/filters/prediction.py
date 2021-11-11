"""
Filters to flag out low prediction confidence calls.
"""
import json
from typing import List, Dict, Union

import pandas as pd

from sentinel.filters.base import FilterBase, FilterFactory


@FilterFactory.register(name="prediction_low_confidence", description="Low prediction confidence turns")
class PredictionConfidenceFilter(FilterBase):
    def __init__(self, *args, **kwargs):
        self.confidence_threshold = kwargs.get("confidence_threshold", 0.95)

        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.prediction = df.prediction.apply(
            lambda x: json.loads(x) if isinstance(x, str) else json.loads("[]"))
        df = self.annotate(df, df.prediction,
                           self._get_low_confidence, "prediction_low_confidence")

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

        if item["score"] < float(self.confidence_threshold):
        if float(item["score"]) < float(self.confidence_threshold):
            return {"score": item["score"]}
        return None
