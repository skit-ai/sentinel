import io
import json
from typing import List, Dict

import pandas as pd

from sentinel.exporters.base import Exporter


class CSVExporter(Exporter):
    """
    CSV report exporter.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def _get_df_for_category(df: pd.DataFrame, category) -> pd.DataFrame:
        """
        Get non null dataframe rows.

        Parameters:
            df (pd.DataFrame): dataframe.
            category (str): filter function name.
        Returns:
            pd.DataFrame: filtered dataframe.
        """
        return df[~df[category].isnull()]

    @staticmethod
    def _serialize(df: pd.DataFrame) -> pd.DataFrame:
        """
        Serialize dataframe items post filtering.

        Parameters:
            df (pd.DataFrame): dataframe.

        Returns:
            pd.DataFrame: updated dataframe.
        """
        df.alternatives = df.alternatives.apply(json.dumps)
        df.prediction = df.prediction.apply(json.dumps)

        return df

    def export_report(self, df: pd.DataFrame, categories: List) -> Dict[str, io.StringIO]:
        file_objects_map = {}

        for category in categories:
            buf = io.StringIO()

            filtered_df = self._get_df_for_category(df, category)
            filtered_df = self._serialize(df)

            filtered_df.to_csv(buf, mode="w", index=False)
            buf.seek(0)
            file_objects_map[category] = buf

        return file_objects_map
