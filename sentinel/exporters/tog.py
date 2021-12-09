from typing import List

import pandas as pd
from togpush import tog
from fsm_pull.connection import Connection

from sentinel.exporters.base import ExporterFactory
from sentinel.exporters.csv import CSVExporter


@ExporterFactory.register(exporter_name="tog", description="Tog Exporter")
class TogExporter(CSVExporter):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        self.access_token = Connection(self.config.get("client_id", 1))

        super().__init__(*args, **kwargs)

    def export_report(self, df_list: List[pd.DataFrame], categories: List):
        """
        Export a list of dataframes to Tog.

        :param df_list: List of dataframes to export
        :param categories: List of categories to export
        :return:
        """
        job_id = self.config.get("export", {}).get("tog", {}).get("job_id", "")

        for idx, category in enumerate(categories):
            category_data = self.config.get("filters", {}).get(category, {})
            limit = category_data.get("limit", 50)
            df = df_list[idx][:limit]
            df = self._serialize(df)

            tog.push_df2tog(job_id, df, self.access_token)
