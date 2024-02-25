import datetime
import logging
import os
from pathlib import Path
from typing import List

import click
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType, ShortType, TimestampType
from tqdm import tqdm

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


def load_spark_dataset(spark, file_path: Path) -> pyspark.sql.DataFrame:
    data = spark.read.text(str(file_path))
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


def process_file(
    filename: str,
    s3_raw_files_folder: Path,
    s3_processed_files_folder: Path,
    spark: SparkSession,
) -> None:
    filepath = str(s3_raw_files_folder) + filename
    try:
        logger.debug("Reading data file...")
        data = load_spark_dataset(spark, filepath)
        logger.debug("Cleaning data file...")
        data = clean_data(data)

        save_path = str(s3_processed_files_folder) + filename[:-4]
        data.write.parquet(save_path)
        logger.debug(f"Cleaned data saved in {save_path}")
    except Exception as e:
        logger.debug(f"Problems with file {filename}. Error: {str(e)}")


def get_raw_filenames_by_date(
    files_folder: Path, current_date: datetime.date
) -> List[str]:
    logger.debug("Searching for raw data in %s", files_folder)
    dates_update = [
        datetime.date.fromisoformat(str(filename.name)[:-4])
        for filename in files_folder.rglob("*.txt")
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


def get_processed_filenames(files_folder: Path) -> List[str]:
    return [str(filename.name) for filename in files_folder.glob("*.parquet")]


@click.command()
@click.option(
    "--s3_raw_files_folder",
    default="s3a://mlops-otus-task2/raw_data/",
    type=Path,
    help="Path to folder with raw data in s3",
)
@click.option(
    "--s3_processed_files_folder",
    default="s3a://mlops-otus-task2/processed_data/",
    type=Path,
    help="Path to folder with already processed data in s3",
)
def main(s3_raw_files_folder: Path, s3_processed_files_folder: Path):
    logger.debug("Initializing spark session")
    spark = (
        SparkSession.builder.appName("OTUS")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.memory", "30g")
        .config("spark.driver.memory", "30g")
        .getOrCreate()
    )
    raw_filenames = get_raw_filenames_by_date(s3_raw_files_folder, CURRENT_DATE)
    processed_filenames = get_processed_filenames(s3_processed_files_folder)
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
            filename,
            s3_raw_files_folder,
            s3_processed_files_folder,
            spark,
        )


if __name__ == "__main__":
    main()
