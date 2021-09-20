import os
import uuid
from typing import List

import pandas as pd
import requests

import sentinel.util as util
from sentinel.exporters.csv import CSVExporter


class SlackExporter(CSVExporter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def export_report(self, df: pd.DataFrame, categories: List):
        file_objects_map = super().export_report(df, categories)
        s3_uuid = uuid.uuid4()

        slack_webhook_url = os.environ.get("SENTINEL_SLACK_WEBHOOK")
        s3_bucket = os.environ.get("SENTINEL_S3_BUCKET")

        message = f"We have found anomalous calls under following categories: {', '.join(categories)}"

        for category, fp in file_objects_map.items():
            util.upload_file(fp, s3_bucket, f"sentinel/{s3_uuid}/{category}.csv")
            message += f"\ns3://{s3_bucket}/sentinel/{s3_uuid}/{category}.csv"

        requests.post(slack_webhook_url, json={"text": message})
