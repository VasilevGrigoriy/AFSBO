import os
import argparse
import logging
from typing import Tuple

from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm

from afsbo.tools.s3_client import get_s3_resource, check_bucket_access, S3Client
from afsbo.utils import init_basic_logger

load_dotenv()


def get_s3_credentials_from_env() -> Tuple[str, str, str]:
    return (
        os.environ["ACCESS_KEY_ID"],
        os.environ["SECRET_ACCESS_KEY"],
        os.environ["ENDPOINT_URL"],
    )


def main(bucket_name: str, upload_folder: str, files_dir: Path):
    logger = init_basic_logger(__name__, logging.DEBUG)

    resourse = get_s3_resource(*get_s3_credentials_from_env())
    if not check_bucket_access(resourse, bucket_name):
        logger.debug("Creating bucket with passed name...")
        resourse.create_bucket(Bucket=bucket_name)
    s3_client = S3Client(resourse, bucket_name)

    data_objects = [obj for obj in files_dir.glob("*")]
    logger.debug("Found %s objects in bucket folder", len(data_objects))

    for object_path in tqdm(
        data_objects, total=len(data_objects), desc="Uploading files", leave=False
    ):
        s3_client.upload_file(
            object_path.resolve(), upload_folder + "/" + str(object_path).split("/")[-1]
        )

    logger.debug("Saved data in s3 %s/%s", bucket_name, upload_folder)


def parse_args():
    parser = argparse.ArgumentParser(description="Script to load data from s3")
    parser.add_argument(
        "--bucket_name",
        type=str,
        help="Bucket name where files will be upload",
    )
    parser.add_argument(
        "--upload_folder",
        type=str,
        help="Folder in s3 bucket where data will be placed",
    )
    parser.add_argument(
        "--files_dir",
        type=Path,
        help="Path to dir where files placed",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.bucket_name, args.upload_folder, args.files_dir.resolve())
