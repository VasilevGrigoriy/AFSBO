import datetime
import logging
import os
from glob import glob
from typing import List
import shutil

import click
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pathlib import Path
from pyspark.sql.types import DoubleType, IntegerType, ShortType, TimestampType
from tqdm import tqdm

from afsbo.tools.load_data_from_s3 import get_s3_credentials_from_env
from afsbo.tools.load_data_from_s3 import main as load_data_from_s3
from afsbo.tools.push_data_to_s3 import main as push_data_to_s3
from afsbo.tools.s3_client import S3Client, make_s3_client_from_credentials
from afsbo.utils import init_basic_logger

logger = init_basic_logger(__name__, logging.DEBUG)

# Эмулируем последовательно поступающие данные
if "DATE" not in os.environ:
    os.environ["DATE"] = "2019-08-23"
CURRENT_DATE = datetime.date.fromisoformat(os.environ.get("DATE"))
logger.debug("Current date in system env - %s", str(CURRENT_DATE))
# Обновляем дату на следующий месяц
os.environ["DATE"] = str(CURRENT_DATE + datetime.timedelta(days=31))
logger.debug(
    "Update system date by 31 days. Next run date will be %s", os.environ.get("DATE")
)

findspark.init()
findspark.find()


def load_spark_dataset(spark, file_path: str) -> pyspark.sql.DataFrame:
    data = spark.read.text(file_path)
    name_cols = [
        "tranaction_id",
        "tx_datetime",
        "customer_id",
        "terminal_id",
        "tx_amount",
        "tx_time_seconds",
        "tx_time_days",
        "tx_fraud",
        "tx_fraud_scenario",
    ]
    data = (
        data.withColumn("temp", f.split("value", ","))
        .select(
            *(
                f.col("temp").getItem(i).alias(name_col)
                for i, name_col in enumerate(name_cols)
            )
        )
        .filter(f.col("tx_datetime").isNotNull())
    )
    return data


def clean_data(spark_dataframe: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # Создадим копию, делаем преобразования на копии
    dataframe = spark_dataframe.alias("dataframe")

    # Удаляем коррелирующие столбцы
    dataframe = dataframe.drop(*["tx_fraud", "tx_time_days"])

    # Удалим выбросы по terminal_id перед кастами
    dataframe = dataframe.filter(dataframe.terminal_id != "")

    # Правим типы, а также название столбца tranaction_id
    types = {
        "transaction_id": dataframe["tranaction_id"].cast(IntegerType()),
        "tx_datetime": dataframe["tx_datetime"].cast(TimestampType()),
        "customer_id": dataframe["customer_id"].cast(IntegerType()),
        "terminal_id": dataframe["terminal_id"].cast(IntegerType()),
        "tx_amount": dataframe["tx_amount"].cast(DoubleType()),
        "tx_time_seconds": dataframe["tx_time_seconds"].cast(IntegerType()),
        "tx_fraud_scenario": dataframe["tx_fraud_scenario"].cast(ShortType()),
    }
    for column, typ in types.items():
        dataframe = dataframe.withColumn(column, typ)

    # Удаляем колонку с неверным названием
    dataframe = dataframe.drop("tranaction_id")

    # Удаляем выбросы
    lower_amount = dataframe.approxQuantile("tx_amount", [0.05], 0.2)[0]
    upper_amount = dataframe.approxQuantile("tx_amount", [0.95], 0.2)[0]
    dataframe = dataframe.filter(
        (dataframe.customer_id != -999_999)
        & (dataframe.tx_amount >= lower_amount)
        & (dataframe.tx_amount <= upper_amount)
    )
    return dataframe


def get_full_path(bucket: str, folder: str, filename: str) -> str:
    return f"s3a://{bucket}/{folder}/{filename}"


def process_file(
    processed_dir: Path,
    filepath: Path,
    spark: SparkSession,
) -> None:
    logger.debug("Read data from %s", filepath)
    try:
        logger.debug("Reading data file...")
        data = load_spark_dataset(spark, str(filepath))
        logger.debug("Cleaning data file...")
        data = clean_data(data)

        save_path = processed_dir / (filepath.name[:-4] + ".parquet")
        data.write.parquet(str(save_path))
        logger.debug(f"Cleaned data saved in {save_path}")
    except Exception as e:
        logger.debug(f"Problems with file {filepath}. Error: {str(e)}")


def get_raw_filenames_by_date(
    s3_client: S3Client, files_folder: str, current_date: datetime.date
) -> List[str]:
    dates_update = [
        datetime.date.fromisoformat(filename[:-4])
        for filename in s3_client.list_folder_object(files_folder)
    ]
    logger.debug("Found raw files dates - %s", dates_update)
    raw_files = [str(date_) + ".txt" for date_ in dates_update if date_ <= current_date]
    logger.debug(
        "Found %s raw files in %s by date %s",
        len(raw_files),
        str(files_folder),
        str(current_date),
    )
    return raw_files


def get_processed_filenames(s3_client: S3Client, files_folder: str) -> List[str]:
    return [
        filename.split(".")[0] + ".txt"
        for filename in s3_client.list_folder_object(files_folder)
    ]

def download_files(s3_client: S3Client, artifacts_path: Path, filenames_to_download: List[str], folder: str) -> List[str]:
    for filename in filenames_to_download:
        s3_client.download_file(f"{folder}/{filename}", artifacts_path / filename)

    return list(artifacts_path.rglob("*.txt"))

def upload_files(s3_client: S3Client, filenames_to_upload: List[Path], folder: str) -> None:
    logger.debug("Files to push: %s", filenames_to_upload)
    logger.debug("Pushing %s files to %s in s3", len(filenames_to_upload), folder)
    for filename in filenames_to_upload:
        s3_client.upload_file(filename, f"{folder}/{filename.name}")

@click.command()
@click.option(
    "--s3_bucket_name",
    default="mlops-otus-task2",
    type=str,
    help="Bucket name in s3",
)
@click.option(
    "--s3_raw_files_folder",
    default="raw_data",
    type=str,
    help="Folder in bucket with raw data",
)
@click.option(
    "--s3_processed_files_folder",
    default="new_processed_data",
    type=str,
    help="Folder in bucket with processed data",
)
@click.option(
    "--artifacts_path",
    default="/home/ubuntu/artifacts/",
    type=Path,
    help="Folder where temp files will be added"
)
def main(s3_bucket_name: str, s3_raw_files_folder: str, s3_processed_files_folder: str, artifacts_path: Path):
    logger.debug("Initializing spark session")
    spark = (
        SparkSession.builder.appName("OTUS")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.memory", "30g")
        .config("spark.driver.memory", "30g")
        .getOrCreate()
    )

    s3_client = make_s3_client_from_credentials(
        *get_s3_credentials_from_env(), s3_bucket_name
    )
    raw_filenames = get_raw_filenames_by_date(
        s3_client, s3_raw_files_folder, CURRENT_DATE
    )
    processed_filenames = get_processed_filenames(s3_client, s3_processed_files_folder)
    filenames_to_process = [
        filename for filename in raw_filenames if filename not in processed_filenames
    ]
    raw_datadir = artifacts_path / "raw_data/"
    raw_datadir.mkdir(parents=True, exist_ok=True)
    filenames_to_process = download_files(s3_client, raw_datadir, filenames_to_process, s3_raw_files_folder)
    logger.debug(
        "Found %s raw files, %s already processed files. Need to process - %s files",
        len(raw_filenames),
        len(processed_filenames),
        len(filenames_to_process),
    )
    processed_dir = artifacts_path / "processed_data"
    for filename in tqdm(
        filenames_to_process, desc="Processing files", total=len(filenames_to_process)
    ):
        process_file(
            processed_dir,
            filename,
            spark,
        )
        data_path = filename.name[:-4] + ".parquet"
        upload_files(s3_client, list((processed_dir / data_path).rglob("*.parquet")), s3_processed_files_folder + "/" + data_path)
    
    shutil.rmtree(str(artifacts_path.resolve()))

if __name__ == "__main__":
    main()
