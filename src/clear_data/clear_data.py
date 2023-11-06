import argparse
from pathlib import Path

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, ShortType


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


def clear_data(spark_dataframe: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # Создадим копию, делаем преобразования на копии
    dataframe = spark_dataframe.alias("dataframe")

    # Удаляем коррелирующие столбцы
    dataframe = dataframe.drop(*["tx_fraud", "tx_time_days"])

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
    dataframe = dataframe.filter(
        (dataframe.customer_id != -999_999)
        & (dataframe.terminal_id != "")
        & (dataframe.tx_amount >= dataframe.approxQuantile("tx_amount", [0.05], 0.0)[0])
        & (dataframe.tx_amount <= dataframe.approxQuantile("tx_amount", [0.95], 0.0)[0])
    )
    return dataframe


def main(path_to_file: Path, save_dir: Path):
    spark = (
        SparkSession.builder.appName("OTUS")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    data = load_spark_dataset(spark, path_to_file)
    data = clear_data(data)

    save_dir.mkdir(exist_ok=True, parents=True)
    data.write.parquet(str(save_dir / path_to_file.name))


def parse_args():
    parser = argparse.ArgumentParser(description="Script to clear dataset")
    parser.add_argument(
        "--path_to_file",
        type=Path,
        help="Path to .txt file with data",
    )
    parser.add_argument(
        "--save_dir",
        action=Path,
        help="Path to dir where file will be saved",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    main(args.path_to_file.resolve(), args.save_dir.resolve())
