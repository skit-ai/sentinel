"""
Filters to flag out calls with a particular end state.
"""
import pandas as pd

from sentinel.filters.base import FilterBase, FilterFactory


@FilterFactory.register(name="call_end_state", description="Calls with a particular end state")
class EndStateFilter(FilterBase):
    def __init__(self, *args, **kwargs):
        self.end_state = kwargs.get("end_state", ["COF"])

        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = self._filter_last_turn(df)
        filtered_df = self.annotate(
            filtered_df,
            filtered_df.state,
            self._filter_state,
            "call_end_state")

        return filtered_df

    def _filter_state(self, item: str):
        if item in self.end_state:
            return True

    def _filter_last_turn(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df.groupby(
            ["call_uuid", "state"], as_index=False).first()
        filtered_df = filtered_df.drop_duplicates(
            subset=["call_uuid"], keep="last")

        return filtered_df
