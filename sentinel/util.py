import logging
import boto3
from botocore.exceptions import ClientError


def upload_file(fp, bucket, object_name):
    """Upload a file to an S3 bucket

    :param fp: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket,
            Body=fp.read(),
            Key=object_name,
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(s3_path):
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
