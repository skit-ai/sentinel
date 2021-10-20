---
layout: default
title: Config specification
nav_order: 3
---

# Config specification

Here is a sample configuration yaml structure which sentinel expects.

```yaml
name: anomaly
description: Get anomalous calls 

data_url: s3://bucket/path/to/dataframe.csv 

export:
  slack:
    channel_name: "app-testing"
  email:
    ids:
      - user@example.com
      - user2@example.com

filters:
  prediction_low_confidence:
    limit: 10
    annotation_key: "prediction_low_confidence"
    kwargs:
      confidence_threshold: 0.50
  low_asr_confidence:
    limit: 10
    annotation_key: "low_asr_confidence"
    kwargs:
      confidence_threshold: 0.50
  no_alternatives:
    limit: 10
    annotation_key: "no_alternatives"
  call_end_state:
    limit: 10
    annotation_key: "call_end_state"
    kwargs:
      end_state: ["REPROMPT_INTENT"]
```

### General tool config spec

- `name`: Name for this config, not used by sentinel. This is mostly for easy
  config management for the users.
- `description`: Description about this config, not used by sentinel. This is
  mostly for easy config management for the users.
- `data_url`: S3 path of the dataframe.
- `export`: Report exporting methods.
  - `slack`:
    - `channel_name`: Slack channel name to post report in.
  - `email`: This feature is not yet supported.
    - `ids`: List of email ids to send reports to.

### Filter spec

All the filters to use for sentinel runs are specified under this.

- `<filter-name>`: Name of the filter you used while registering. For example:
  see how [`ASRConfidenceFilter`][asr-confidence] is registered.
  - `limit`: Number of items (calls/turns) you want to see in the final report
    satisfying this filter's constraints.
  - `annotation_key`: name of the column name to be added in the dataframe.
    Sentinel adds an annotation to the items which satisfy the constraints of
    the filter. This can be same as the `<filter-name>` mentioned above.
  - `kwargs`: Any keyword arguments you would want to pass to the filters.
    - `<kwarg-name>`: Value which the filters can use. Make sure that the
      filter functions (available or custom defined) make use of these keyword
      arguments. Check a sample filter which makes use of kwargs [here][asr-confidence].


[asr-confidence]: https://github.com/skit-ai/sentinel/blob/master/sentinel/filters/confidence.p
