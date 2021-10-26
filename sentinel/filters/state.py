"""
Filters to flag out calls with a particular end state.
"""
import pandas as pd
from itertools import groupby
from collections import Counter

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


@FilterFactory.register(name="state_stuck", description="Calls which are stuck in particular state")
class RepeatedCallState(FilterBase):
    def __init__(self, *args, **kwargs):
        self.max_state_count = kwargs.get("max_state_count", 4)

        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = self._filter_last_turn(df)

        filtered_df = self.annotate(filtered_df, df.state_transitions,
                                    self._get_repeated_state, "state_stuck")

        return filtered_df

    def _get_repeated_state(self, item: str):
        state_transitions = list(map(lambda x: x.strip(), item.split("->")))
        state_transition_count = Counter(state_transitions)

        if any(filter(lambda key: state_transition_count[key] >= self.max_state_count, state_transition_count)):
            print(state_transition_count, state_transitions)
            return {"state_stuck": self.max_state_count}
        return None

    def _filter_last_turn(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df.drop_duplicates(subset=["call_uuid"], keep="last")

        return filtered_df


@FilterFactory.register(name="state_loop", description="Calls which come back to already visited state")
class LoopCallState(FilterBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._filter_last_turn(df)
        df = self.annotate(df, df.state_transitions,
                           self._get_loop, "state_loop")

        return df

    def _get_loop(self, item: str):
        state_transitions = list(map(lambda x: x.strip(), item.split("->")))

        # remove consecutive duplicates
        filtered_state_list = [x[0] for x in groupby(state_transitions)]

        state_transition_count = Counter(filtered_state_list)

        if any(filter(lambda key: state_transition_count[key] >= 2, state_transition_count)):
            print(state_transitions)
            return {"state_loop": True}
        return None

    def _filter_last_turn(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df.drop_duplicates(subset=["call_uuid"], keep="last")

        return filtered_df
