import os
import pandas as pd

from sentinel import __version__
from sentinel.filters.confidence import ConfidenceFilter
from sentinel.filters.alternatives import AlternativesFilter
from sentinel.filters.prediction import PredictionConfidenceFilter
from sentinel.dataframes.reader import CSVReader
from sentinel.exporters.csv import CSVExporter


package_dir = os.path.dirname(os.path.realpath(__file__))


def test_version():
    assert __version__ == '0.1.0'


def test_csv_reader():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()
    assert isinstance(df, pd.DataFrame)


def test_csv_exporter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_exporter = CSVExporter()
    fp_map = csv_exporter.export_report(df, ["alternatives"])

    csv_reader = CSVReader(fp_map["alternatives"])
    df_processed = csv_reader.read()

    assert all(df.columns == df_processed.columns)


def test_low_confidence_filter():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(f"{package_dir}/resources/low_confidence.csv")
    df_expected = csv_reader.read()

    confidence_filter = ConfidenceFilter(**{"confidence_threshold": 95})
    df_processed = confidence_filter.process(df)

    csv_exporter = CSVExporter()
    fp_map = csv_exporter.export_report(df_processed, ["low_confidence"])
    csv_reader = CSVReader(fp_map["low_confidence"])
    df_processed = csv_reader.read()

    assert df_processed.low_confidence.equals(df_expected.low_confidence)


def test_no_alternatives():
    csv_reader = CSVReader(f"{package_dir}/resources/records.csv")
    df = csv_reader.read()

    csv_reader = CSVReader(f"{package_dir}/resources/no_alternatives.csv")
    df_expected = csv_reader.read()

    alternatives_filter = AlternativesFilter()
    df_processed = alternatives_filter.process(df)

    csv_exporter = CSVExporter()
    fp_map = csv_exporter.export_report(df_processed, ["no_alternatives"])

    csv_reader = CSVReader(fp_map["no_alternatives"])
    df_processed = csv_reader.read()

    assert df_processed.no_alternatives.equals(df_expected.no_alternatives)
