import os
import uuid
from typing import List, Iterator

import pandas as pd
import requests

import sentinel.util as util
from sentinel.exporters.csv import CSVExporter
from sentinel.filters.base import FilterFactory


class SlackExporter(CSVExporter):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        super().__init__(*args, **kwargs)

    def _get_call_reference(self, call_uuid: str):
        if self.config.get("client_id"):
            console_host = os.environ.get("SENTINEL_CONSOLE_HOST")
            return f"{console_host}/{self.config.get('client_id')}/#/call?uuid={call_uuid}"
        else:
            metabase_host = os.environ.get("SENTINEL_METABASE_HOST")
            return f"{metabase_host}?call_uuid={call_uuid}"

    def _write_block(self, message_blocks, text):
        # Slack block kit breaks if text is empty
        text = text if text else "None"

        return message_blocks.append({
            "type": "section",
            "text": {
                    "type": "mrkdwn",
                    "text": f"{text}"
            }
        })

    def _message_builder(self, df: pd.DataFrame, category: str):
        limit = self.config.get("filters", {}).get(category, {}).get("limit", 50)

        # Limit the number of calls to display
        call_uuids = df.call_uuid.unique()[:limit]

        # Get all available filter functions from registry
        registry = FilterFactory.registry

        # Stores block kit blocks
        message_blocks = []

        self._write_block(message_blocks, f"{registry.get(category, {}).get('description')}")

        # Create list of call urls
        call_text_list = []
        for call_uuid in call_uuids:
            call_reference = self._get_call_reference(call_uuid)
            call_text_list.append(f"• {call_reference}")

        self._write_block(message_blocks, "\n".join(call_text_list))

        return message_blocks

    def _chunk_blocks(self, blocks: List, chunk_size: int) -> Iterator:
        """
        Chunk block kit blocks to `chunk_size` size. Slack doesn't support
        more than 50 blocks at a time. This generator yields smaller chunks
        """
        for i in range(0, len(blocks), chunk_size):
            yield blocks[i:i + chunk_size]

    def export_report(self, df: pd.DataFrame, categories: List):
        s3_uuid = uuid.uuid4()

        slack_webhook_url = os.environ.get("SENTINEL_SLACK_WEBHOOK")
        s3_bucket = os.environ.get("SENTINEL_S3_BUCKET")

        dataframe_message = ""
        message_blocks = []
        self._write_block(
            message_blocks, f"*We have found anomalous calls under following categories: {', '.join(categories)}*")

        # For each filter function generate slack message blocks
        for category in categories:
            filtered_df = self._get_df_for_category(df, category)
            filtered_df = self._serialize(filtered_df)

            message_blocks.extend(self._message_builder(filtered_df, category))

            util.upload_df_to_s3(filtered_df, s3_bucket, f"sentinel/{s3_uuid}/{category}.csv")
            dataframe_message += f"\ns3://{s3_bucket}/sentinel/{s3_uuid}/{category}.csv"

        self._write_block(message_blocks, f"Exported dataframes at: {dataframe_message}")

        # For each block chunk send a new message
        blocks_chunk = self._chunk_blocks(message_blocks, 50)
        for blocks in blocks_chunk:
            response = requests.post(slack_webhook_url, json={"text": "", "blocks": message_blocks})
            if not response.ok:
                print(response.text)
