---
layout: default
title: Filters
nav_order: 2
---

# Filters

This page explains what are filter functions and how to write your own filter
functions.

Filters are functions which:
- Take a dataframe as input.
- Check and apply certain constraints.

and
- Return back a smaller, subset of the original dataframe which holds true for
  the constraints applied by the filter.
  
Calls/turns from these dataframes are reported as reports via Slack, e-mail
etc.


#### Example

Consider a dataframe below:

```
call_uuid,conversation_uuid,alternatives,audio_url,reftime,prediction,state,call_duration,state_transitions
1,2,"[[{""confidence"": 0.9299549, ""transcript"": ""Hindi""}, {""confidence"": 0.04444444, ""transcript"": ""hinadi""}, {""confidence"": 0.48719966, ""transcript"": ""in Hindi""}]]",https://example.com/%2F2021-09-27%21%2Fc0f5b3ef-da72-4488-8191-b0412685fd3c.flac,2021-09-27 03:37:14.806623 +0000 UTC,"{""name"": ""inform_hindi"", ""score"": 0.9949743324486925, ""slot"": []}",COF,,UPCOMING_FLIGHT_DETAILS_ASYNC -> GET_LANGUAGE_PREFERENCE -> COF -> COF -> SET_LANGUAGE_PREFERENCE -> COF_QUERY -> COF_QUERY -> COF_QUERY -> COF_QUERY -> CONFIRMATION_CANCELLATION_POLICY -> CONFIRMATION_CANCELLATION_POLICY -> CONFIRMATION_CANCELLATION_POLICY -> ACTION_FETCH_UPCOMING_FLIGHT_DETAILS -> BOOKING_DONE -> BOOKING_DONE -> COF_ALPHANUMERIC -> COF_ALPHANUMERIC -> COLLECT_INPUT -> COLLECT_INPUT -> COLLECT_INPUT -> COLLECT_INPUT -> COLLECT_INPUT -> ACTION_GROUP_4 -> GROUP_4_2 -> GROUP_4_2 -> ACTION_CONFIRM_GROUP_4 -> ACTION_GROUP_4 -> GROUP_4_2 -> GROUP_4_2 -> ACTION_CONFIRM_GROUP_4 -> FINAL_CONFIRM -> FINAL_CONFIRM -> FINAL_CONFIRM -> FINAL_CONFIRM -> TRANSFER_AGENT -> NA
```

If you apply an [`ASRConfidenceFilter`][asr-confidence] filter on this dataframe with a
threshold of `0.95`, this will be marked as an anomalous call because of the
low confidence in of the alternatives.

---

### Writing a custom filter

Implement the [`FilterBase`][filter-base] class and register it via
[`@FilterFactory.register`][register] decorator to add it to supported filter factory.

For example, consider a filter to flag out calls with certain end state.

```python
@FilterFactory.register(name="call_end_state", description="Calls with a particular end state")
class EndStateFilter(FilterBase):
    def __init__(self, *args, **kwargs):
        self.end_state = kwargs.get("end_state", ["COF"])
        super().__init__(*args, **kwargs)

    def preprocess(self, df: pd.DataFrame):
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = self._filter_last_turn(df)
        filtered_df = self.annotate(filtered_df, filtered_df.state,
                                    self._filter_state, "call_end_state")

        return filtered_df

    def _filter_state(self, item: str):
        if item in self.end_state:
            return True

    def _filter_last_turn(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df.groupby(["call_uuid", "state"], as_index=False).first()
        filtered_df = filtered_df.drop_duplicates(subset=["call_uuid"], keep="last")

        return filtered_df
```

Logic on how filtering is being performed for this particular example might be
out of the scope of this doc but the basic steps taken are:

1. Create a new filter class inheriting the [`FilterBase`][filter-base] ABC and register it
   with the [`FilterFactory`](./)

```python
@FilterFactory.register(name="call_end_state", description="Calls with a particular end state")
class EndStateFilter(FilterBase):
    def __init__(self, *args, **kwargs):
        self.end_state = kwargs.get("end_state", ["COF"])
        super().__init__(*args, **kwargs)

    ...
```
Here,

- `name` is the name of this filter which will be used to refer this filter in [`config.yml`][config-spec].
- `description` is the description of this filter which is mostly used while
  reporting calls over Slack or email.

Any keyword arguments described in the `config.yml` (see how to provide keyword
arguments in `config.yml` [here][config-spec]), must be initialzed and in `__init__()`
like above

2. For serving dataframes, implement `process()` method to filter out a smaller
   dataframe which matches your filter constraints and return.

```python
@FilterFactory.register(name="call_end_state", description="Calls with a particular end state")
class EndStateFilter(FilterBase):

    ...
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = self._filter_last_turn(df)
        filtered_df = self.annotate(filtered_df, filtered_df.state,
                                    self._filter_state, "call_end_state")

        return filtered_df
        
    ...
```


3. While reporting, sentinel will automatically pick up relevant calls/turns
   from the dataframe and report. You don't need to make changes in reporting
   logic. (You might have to make appropriate changes in the config file).

### Using your newly created filters

Check the `config.yml` doc [here][config-spec] to see how to add your filter to be used
by sentinel.


[filter-base]: https://github.com/skit-ai/sentinel/blob/master/sentinel/filters/base.py#L7
[asr-confidence]: https://github.com/skit-ai/sentinel/blob/master/sentinel/filters/confidence.p
[register]: https://github.com/skit-ai/sentinel/blob/master/sentinel/filters/base.py#L89
[config-spec]: ./config-spec.html
