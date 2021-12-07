import os
import pandas as pd

from sentinel import __version__
from sentinel.filters.confidence import ASRConfidenceFilter
from sentinel.filters.alternatives import AlternativesFilter, WordFilter
from sentinel.filters.prediction import PredictionConfidenceFilter
from sentinel.filters.state import EndStateFilter, RepeatedCallState, LoopCallState
from sentinel.dataframes.reader import CSVReader
from sentinel.exporters.csv import CSVExporter


package_dir = os.path.dirname(os.path.realpath(__file__))


def test_version():
    assert __version__ == '0.2.6'


def test_csv_reader():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()
    assert isinstance(df, pd.DataFrame)


def test_csv_exporter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(df, ["alternatives"])

    assert all(df.columns == df_processed.columns)


def test_low_asr_confidence_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(f"{package_dir}/resources/low_asr_confidence.csv")
    df_expected = csv_reader.read()

    confidence_filter = ASRConfidenceFilter(**{"confidence_threshold": 0.95})
    df_processed = confidence_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(
        df_processed, "low_asr_confidence")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_no_alternatives():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(f"{package_dir}/resources/no_alternatives.csv")
    df_expected = csv_reader.read()

    alternatives_filter = AlternativesFilter()
    df_processed = alternatives_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(
        df_processed, "no_alternatives")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_low_confidence_prediction_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(
        f"{package_dir}/resources/prediction_low_confidence.csv")
    df_expected = csv_reader.read()

    confidence_filter = PredictionConfidenceFilter(
        **{"confidence_threshold": 0.95})
    df_processed = confidence_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(
        df_processed, "prediction_low_confidence")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_end_state_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(
        f"{package_dir}/resources/call_end_state.csv")
    df_expected = csv_reader.read()

    end_state_filter = EndStateFilter(**{"end_state": ["FOLLOW_UP"]})
    df_processed = end_state_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(df_processed,
                                                     "call_end_state")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_state_stuck_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(
        f"{package_dir}/resources/state_stuck.csv")
    df_expected = csv_reader.read()

    repeated_call_state_filter = RepeatedCallState(**{"max_state_count": 4})
    df_processed = repeated_call_state_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(df_processed,
                                                     "state_stuck")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_filter_words_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(
        f"{package_dir}/resources/filter_words.csv")
    df_expected = csv_reader.read()

    word_filter = WordFilter(**{"word_list": ["customer"]})
    df_processed = word_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(df_processed,
                                                     "filter_words")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)


def test_state_loop_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(
        f"{package_dir}/resources/state_loop.csv")
    df_expected = csv_reader.read()

    word_filter = LoopCallState()
    df_processed = word_filter.process(df)

    csv_exporter = CSVExporter()
    df_processed = csv_exporter._get_df_for_category(df_processed,
                                                     "state_loop")

    pd.testing.assert_series_equal(
        df_processed.call_uuid, df_expected.call_uuid, check_index=False)
