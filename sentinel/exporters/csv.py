import io
from typing import List

import pandas as pd

from sentinel.exporters.base import Exporter


class CSVExporter(Exporter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def _get_df_for_category(df: pd.DataFrame, category):
        return df[~df[category].isnull()]

    def export_report(self, df: pd.DataFrame, categories: List):
        file_objects_map = {}

        for category in categories:
            buf = io.StringIO()

            filtered_df = self._get_df_for_category(df, category)
            filtered_df.to_csv(buf, mode="w", index=False)
            buf.seek(0)
            file_objects_map[category] = buf

        return file_objects_map
