import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def upload_df_to_s3(df: pd.DataFrame, bucket: str, object_name: str) -> bool:
    """
    Upload a file to an S3 bucket.

    Parameters:
        fp: File to upload.
        bucket (str): Bucket to upload to.
        object_name (str): S3 object name.

    Returns:
        True if file was uploaded, else False.
    """

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        fp = io.StringIO()
        df.to_csv(fp, mode="w", index=False)

        s3_client.put_object(
            Bucket=bucket,
            Body=fp.getvalue(),
            Key=object_name,
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(s3_path: str) -> None:
    """
    Download file from S3 bucket.

    Parameters:
        s3_path (str): S3 object name
    """
    s3_path_split = s3_path.split("/")
    bucket = s3_path_split[2]
    object_name = "/".join(s3_path_split[3:])

    s3_client = boto3.client('s3')
    try:
        s3_response_object = s3_client.get_object(
            Bucket=bucket,
            Key=object_name
        )
        return s3_response_object
    except ClientError as e:
        logging.error(e)
