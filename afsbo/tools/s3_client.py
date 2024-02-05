import logging
from pathlib import Path
from typing import Union, List

import boto3
import botocore

from afsbo.utils import init_basic_logger

logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)


def get_s3_resource(
    aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str
):
    return boto3.resource(
        service_name="s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )


def check_bucket_access(
    s3_resource: boto3.resources.base.ServiceResource, bucket_name: str
):
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 403:
            raise ValueError("Private Bucket. Forbidden Access!")
        elif error_code == 404:
            raise ValueError(f"Didn't find bucket with name {bucket_name}.")


class S3Client:
    def __init__(
        self, s3_resource: boto3.resources.base.ServiceResource, bucket_name: str
    ) -> None:
        self.s3_resource = s3_resource
        self.bucket_name = bucket_name
        self.logger = init_basic_logger(__name__, logging.DEBUG)
        check_bucket_access(self.s3_resource, self.bucket_name)
        self.bucket = self.s3_resource.Bucket(bucket_name)

    def download_file(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        self.logger.debug(
            "Download file from s3://%s/%s to %s", self.bucket_name, src, dst
        )
        try:
            self.bucket.download_file(str(src), str(dst))
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Message"] == "Not Found":
                raise FileNotFoundError(f"No file by path {str(src)} in s3!")
            raise err

    def upload_file(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        try:
            self.logger.debug(
                "Upload file from %s to s3://%s/%s", src, self.bucket_name, dst
            )
            self.bucket.upload_file(str(src), str(dst))
        except:
            self.logger.error(
                "Error occurred while uploading file from %s to s3://%s/%s",
                src,
                self.bucket_name,
                dst,
            )

    def list_folder_object(self, folder: Union[str, Path]) -> List[str]:
        return [
            obj.key.split("/")[-1]
            for obj in self.bucket.objects.filter(Prefix=folder)
            if obj.key[-1] != "/"
        ]


def make_s3_client_from_credentials(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    endpoint_url: str,
    bucket_name: str,
) -> S3Client:
    s3_resource = get_s3_resource(
        aws_access_key_id, aws_secret_access_key, endpoint_url
    )
    return S3Client(s3_resource, bucket_name)
