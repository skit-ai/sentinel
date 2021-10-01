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

    def _get_call_reference(self, call_uuid: str) -> str:
        """
        Get call reference url. Skit internal console url or otherwise.

        Parameters:
            call_uid (str): Call uuid.
        Returns:
            str: URL for a call with `call_uuid`
        """
        console_host = os.environ.get("SENTINEL_CONSOLE_HOST")
        return f"{console_host}{self.config.get('client_id', 1)}/call-report/#/call?uuid={call_uuid}"

    def _write_block(self, message_blocks: List, text: str) -> List:
        """
        Write a new block in the block kit message

        Parameters:
            message_blocks (list): Block kit message list. This is the global list.
            text (str): text message for this block.
        """
        # Slack block kit breaks if text is empty
        text = text if text else "None"

        return message_blocks.append({
            "type": "section",
            "text": {
                    "type": "mrkdwn",
                    "text": f"{text}"
            }
        })

    def _chunk_call_list(self, call_uuids: List[str], chunk_size: int) -> Iterator:
        """
        Chunk text message to so that it doesn't exceed Slack's max limit.
        Slack allows max 3001 characters.

        Parameters:
            call_uuids (list): List of call UUIDs.
            chunk_size (int): Size of uuid list to chunk into.
        """
        # Create list of call urls
        call_text_list = []
        for call_uuid in call_uuids:
            call_reference = self._get_call_reference(call_uuid)
            call_text_list.append(f"• <{call_reference}|{call_uuid}>")

        for idx in range(0, len(call_text_list), chunk_size):
            yield call_text_list[idx: idx + chunk_size]

    def _chunk_blocks(self, blocks: List, chunk_size: int) -> Iterator:
        """
        Chunk block kit blocks to `chunk_size` size. Slack doesn't support
        more than 50 blocks at a time. This generator yields smaller chunks

        Parameters:
            blocks (list): Block kit message list (List of blocks)
            chunk_size (int): Size of blocks to chunk into
        Returns:
            Iterator: blocks chunked into `chunk_size`
        """
        for i in range(0, len(blocks), chunk_size):
            yield blocks[i:i + chunk_size]

    def _message_builder(self, df: pd.DataFrame, category: str):
        """
        Build block kit message

        Parameters:
            df (pd.DataFrame): dataframe.
            category (str): filter name.

        Returns:
            None
        """
        category_data = self.config.get("filters", {}).get(category, {})
        limit = category_data.get("limit", 50)

        # Limit the number of calls to display
        call_uuids = df.call_uuid.unique()[:limit]

        # Get all available filter functions from registry
        registry = FilterFactory.registry

        # Stores block kit blocks
        message_blocks = []

        category_description = registry.get(category, {}).get('description')
        self._write_block(
            message_blocks, f":pencil: *{category_description}. Kwargs: {category_data.get('kwargs')}*")

        call_text_list = self._chunk_call_list(call_uuids, 20)
        for call_text in call_text_list:
            self._write_block(message_blocks, "\n".join(call_text))

        return message_blocks

    def export_report(self, df_list: List[pd.DataFrame], categories: List):
        s3_uuid = uuid.uuid4()

        slack_webhook_url = os.environ.get("SENTINEL_SLACK_WEBHOOK")
        s3_bucket = os.environ.get("SENTINEL_S3_BUCKET")

        dataframe_message = ""
        message_blocks = []
        self._write_block(
            message_blocks, f"*We have found anomalous calls under following categories: {', '.join(categories)}*")

        # For each filter function generate slack message blocks
        for idx, category in enumerate(categories):
            filtered_df = self._get_df_for_category(df_list[idx], category)
            filtered_df = self._serialize(filtered_df)

            message_blocks.extend(self._message_builder(filtered_df, category))

            util.upload_df_to_s3(filtered_df, s3_bucket,
                                 f"sentinel/{s3_uuid}/{category}.csv")
            dataframe_message += f"\n<s3://{s3_bucket}/sentinel/{s3_uuid}/{category}.csv|{category}.csv>"

        self._write_block(
            message_blocks, f"* :file_cabinet: Exported dataframes at:* {dataframe_message}")

        # For each block chunk send a new message
        blocks_chunk = self._chunk_blocks(message_blocks, 50)
        for blocks in blocks_chunk:
            response = requests.post(slack_webhook_url, json={
                                     "text": "", "blocks": blocks})
            if not response.ok:
                print(response.text)
