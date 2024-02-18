import datetime
import logging
import sys
from pathlib import Path

import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType, ShortType, TimestampType
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(
    logging.Formatter(fmt="[%(asctime)s: %(levelname)s %(name)s] %(message)s")
)
logger.addHandler(handler)

FILENAMES = [
    "2019-08-22.txt",
    "2019-09-21.txt",
    "2019-10-21.txt",
    "2019-11-20.txt",
    "2019-12-20.txt",
    "2020-01-19.txt",
    "2020-02-18.txt",
    "2020-03-19.txt",
    "2020-04-18.txt",
    "2020-05-18.txt",
    "2020-06-17.txt",
    "2020-07-17.txt",
    "2020-08-16.txt",
    "2020-09-15.txt",
    "2020-10-15.txt",
    "2020-11-14.txt",
    "2020-12-14.txt",
    "2021-01-13.txt",
    "2021-02-12.txt",
    "2021-03-14.txt",
    "2021-04-13.txt",
    "2021-05-13.txt",
    "2021-06-12.txt",
    "2021-07-12.txt",
    "2021-08-11.txt",
    "2021-09-10.txt",
    "2021-10-10.txt",
    "2021-11-09.txt",
    "2021-12-09.txt",
    "2022-01-08.txt",
    "2022-02-07.txt",
    "2022-03-09.txt",
    "2022-04-08.txt",
    "2022-05-08.txt",
    "2022-06-07.txt",
    "2022-07-07.txt",
    "2022-08-06.txt",
    "2022-09-05.txt",
    "2022-10-05.txt",
    "2022-11-04.txt",
]

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
    s3_raw_files_folder: str,
    s3_processed_files_folder: str,
    spark: SparkSession,
    time_done: str,
) -> None:
    filepath = s3_raw_files_folder + filename
    try:
        logger.debug("Reading data file...")
        data = load_spark_dataset(spark, filepath)
        logger.debug("Cleaning data file...")
        data = clean_data(data)

        save_path = s3_processed_files_folder + f"{time_done}_{filename[:-4]}"
        data.write.parquet(save_path)
        logger.debug(f"Cleaned data saved in {save_path}")
    except Exception as e:
        logger.debug(f"Problems with file {filename}. Error: {str(e)}")


def main():
    logger.debug("Initializing spark session...")
    time_done = str(datetime.datetime.now())
    spark = (
        SparkSession.builder.appName("OTUS")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "10g")
        .getOrCreate()
    )
    for filename in tqdm(FILENAMES, desc="Processing files", total=len(FILENAMES)):
        process_file(
            filename,
            "s3a://mlops-otus-task2/raw_data/",
            "s3a://mlops-otus-task2/processed_data/",
            spark,
            time_done,
        )


if __name__ == "__main__":
    main()
