import datetime
import logging
import os
from glob import glob
from typing import List

import click
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
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
    bucket: str,
    filename: str,
    s3_raw_files_folder: str,
    s3_processed_files_folder: str,
    spark: SparkSession,
) -> None:
    filepath = get_full_path(bucket, s3_raw_files_folder, filename)
    try:
        logger.debug("Reading data file...")
        data = load_spark_dataset(spark, filepath)
        logger.debug("Cleaning data file...")
        data = clean_data(data)

        save_path = get_full_path(bucket, s3_processed_files_folder, filename[:-4])
        data.write.parquet(save_path)
        logger.debug(f"Cleaned data saved in {save_path}")
    except Exception as e:
        logger.debug(f"Problems with file {filename}. Error: {str(e)}")


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


@click.command()
@click.option(
    "--s3_bucket_name",
    default="mlops-otus-task2",
    type=str,
    help="Folder in bucket with raw data",
)
@click.option(
    "--s3_raw_files_folder",
    default="raw_data",
    type=str,
    help="Folder in bucket with raw data",
)
@click.option(
    "--s3_processed_files_folder",
    default="processed_data",
    type=str,
    help="Folder in bucket with processed data",
)
def main(s3_bucket_name: str, s3_raw_files_folder: str, s3_processed_files_folder: str):
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
    logger.debug(
        "Found %s raw files, %s already processed files. Need to process - %s files",
        len(raw_filenames),
        len(processed_filenames),
        len(filenames_to_process),
    )
    for filename in tqdm(
        filenames_to_process, desc="Processing files", total=len(filenames_to_process)
    ):
        process_file(
            bucket_name,
            filename,
            s3_raw_files_folder,
            s3_processed_files_folder,
            spark,
        )


if __name__ == "__main__":
    main()
