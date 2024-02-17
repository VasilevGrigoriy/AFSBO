import argparse
import logging
import os
import sys
from typing import Tuple

sys.path.append("/opt/airflow/afsbo/tools/")
sys.path.append("/opt/airflow/afsbo/")

from pathlib import Path

from dotenv import load_dotenv
from s3_client import make_s3_client_from_credentials
from tqdm import tqdm
from utils import init_basic_logger

load_dotenv()


def get_s3_credentials_from_env() -> Tuple[str, str, str]:
    return (
        os.environ["ACCESS_KEY_ID"],
        os.environ["SECRET_ACCESS_KEY"],
        os.environ["ENDPOINT_URL"],
    )


def main(bucket_name: str, data_folder: str, save_dir: Path):
    logger = init_basic_logger(__name__, logging.DEBUG)

    s3_client = make_s3_client_from_credentials(
        *get_s3_credentials_from_env(), bucket_name
    )
    data_objects = s3_client.list_folder_object(data_folder)
    logger.debug("Found %s objects in bucket folder", len(data_objects))

    if isinstance(save_dir, str):
        save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    for object in tqdm(
        data_objects, total=len(data_objects), desc="Downloading files", leave=False
    ):
        s3_client.download_file(data_folder + "/" + object, save_dir / object)

    logger.debug("Saved data in %s", save_dir)


def parse_args():
    parser = argparse.ArgumentParser(description="Script to load data from s3")
    parser.add_argument(
        "--bucket_name",
        type=str,
        help="Bucket name where file to dowload are placed",
    )
    parser.add_argument(
        "--data_folder",
        type=str,
        help="Folder in s3 bucket where data is placed",
    )
    parser.add_argument(
        "--save_dir",
        type=Path,
        help="Path to dir where files will be saved",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.bucket_name, args.data_folder, args.save_dir)
