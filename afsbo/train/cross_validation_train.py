import logging
import os
from functools import reduce
from pathlib import Path

import click
import findspark
import mlflow
import pyspark
from dotenv import load_dotenv
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import dayofweek, hour, minute, month

from afsbo.utils import init_basic_logger

load_dotenv("../../.env")

logger = init_basic_logger("training models", logging.DEBUG)

MLFLOW_TRACKING_SERVER_HOST = os.environ.get("MLFLOW_TRACKING_SERVER_HOST")
MLFLOW_PORT = os.environ.get("MLFLOW_PORT")

findspark.init()
findspark.find()


def load_full_dataset(
    s3_processed_files_folder: Path, spark: SparkSession
) -> DataFrame:
    datasets = []
    filepaths = list(s3_processed_files_folder.glob("*.parquet"))
    logger.debug(
        "Found %s processed files in %s", len(filepaths), s3_processed_files_folder
    )
    for filepath in filepaths:
        datasets.append(spark.read.parquet(filepath))

    return reduce(DataFrame.unionAll, datasets)


def data_enrichment(spark_dataframe: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Функция обогащения данных, удаление лишних колонок для обучения
    """
    # Создадим копию, делаем преобразования на копии
    dataframe = spark_dataframe.alias("dataframe")

    # Преобразуем колонку даты tx_datetime в колонки секунд, минут, часов, дней недели, месяца
    dataframe = dataframe.withColumn("tx_datetime_month", month(dataframe.tx_datetime))
    dataframe = dataframe.withColumn(
        "tx_datetime_dayofweek", dayofweek(dataframe.tx_datetime)
    )
    dataframe = dataframe.withColumn("tx_datetime_hour", hour(dataframe.tx_datetime))
    dataframe = dataframe.withColumn(
        "tx_datetime_minutes", minute(dataframe.tx_datetime)
    )

    # Удалим колонку transaction_id, явно не несет информации, если это просто индекс транзакции
    # Также удаляем tx_datetime
    dataframe = dataframe.drop(*["transaction_id", "tx_datetime"])

    return dataframe


def transform_dataset_to_evaluation(
    dataset: pyspark.sql.DataFrame,
    features: list[str],
    features_col_name: str,
) -> Pipeline:
    assembler = VectorAssembler(inputCols=features, outputCol=features_col_name)
    output = assembler.transform(dataset)
    return output


def train(
    dataset: pyspark.sql.DataFrame,
    features: list[str],
    target: str,
    features_colname: str,
    reg_param_grid: list[float] = [0.0, 0.01, 0.05, 0.1, 0.3],
    elastic_param_grid: list[float] = [0.1, 0.5, 1.0],
):
    logger.debug("Start training model in cross-validation mode")
    dataset = transform_dataset_to_evaluation(dataset, features, features_colname)
    log_reg = LogisticRegression(featuresCol=features_colname, labelCol=target)
    grid_search = (
        ParamGridBuilder()
        .addGrid(log_reg.regParam, reg_param_grid)
        .addGrid(log_reg.elasticNetParam, elastic_param_grid)
        .build()
    )
    logger.debug(
        "There are %s variants in GridSearch",
        len(reg_param_grid) * len(elastic_param_grid),
    )
    evaluator = MulticlassClassificationEvaluator(
        predictionCol="prediction", labelCol=target
    )
    cross_validator = CrossValidator(
        estimator=log_reg, estimatorParamMaps=grid_search, evaluator=evaluator
    )
    cross_val_model = cross_validator.fit(train)

    return cross_val_model


@click.command()
@click.option(
    "--s3_processed_files_folder",
    default="s3a://mlops-otus-task2/processed_data/",
    type=Path,
    help="Path to folder with already processed data in s3",
)
@click.option(
    "--experiment_name",
    default="test_experiment",
    type=str,
    help="Experiment name in mlflow",
)
@click.option(
    "--run_name",
    default="test",
    type=str,
    help="Run name in mlflow",
)
@click.option(
    "--model_artifact_name",
    default="test_1",
    type=str,
    help="Model name in mlflow artifact to save",
)
def main(
    s3_processed_files_folder: Path,
    experiment_name: str,
    run_name: str,
    model_artifact_name: str,
):
    logger.debug("Initializing spark session")
    spark = (
        SparkSession.builder.appName("OTUS")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.memory", "30g")
        .config("spark.driver.memory", "30g")
        .getOrCreate()
    )

    # Теперь открываем все файлы, мерджим их и обучаем на этих данных
    dataset = load_full_dataset(s3_processed_files_folder, spark)
    # Немного обогатим данные
    dataset = data_enrichment(dataset)

    # Подготовим эксперимент
    mlflow.set_tracking_uri(f"http://{MLFLOW_TRACKING_SERVER_HOST}:{MLFLOW_PORT}")
    logger.debug("tracking mlflow URI: %s", {mlflow.get_tracking_uri()})
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    experiment_id = experiment.experiment_id

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        features = [
            "customer_id",
            "terminal_id",
            "tx_amount",
            "tx_time_seconds",
            "tx_datetime_month",
            "tx_datetime_dayofweek",
            "tx_datetime_hour",
            "tx_datetime_minutes",
        ]
        target = "tx_fraud_scenario"
        cross_validation_results = train(dataset, features, target, "features")

        best_regParam = round(
            cross_validation_results.bestModel.stages[-1].getRegParam(), 5
        )
        best_fitIntercept = round(
            cross_validation_results.bestModel.stages[-1].getFitIntercept(), 5
        )
        best_elasticNetParam = round(
            cross_validation_results.bestModel.stages[-1].getElasticNetParam(), 5
        )

        # Логгируем лучшие параметры
        logger.debug("Best regParam - %s", best_regParam)
        logger.debug("Best fitIntercept - %s", best_fitIntercept)
        logger.debug("Best elasticNetParam - %s", best_elasticNetParam)

        mlflow.log_param("optimal_regParam", best_regParam)
        mlflow.log_param("optimal_fitIntercept", best_fitIntercept)
        mlflow.log_param("optimal_elasticNetParam", best_elasticNetParam)

        # Логгируем метрику по этим параметрам
        f1_score_best = cross_validation_results.bestModel.summary.f1
        logger.debug("F1-score on best params - %s", f1_score_best)
        mlflow.log_metric("f1_score", f1_score_best)

        logger.debug("Saving model")
        mlflow.spark.save_model(cross_validation_results.bestModel, model_artifact_name)

        logger.debug("Logging model")
        mlflow.spark.log_model(cross_validation_results.bestModel, model_artifact_name)
        logger.debug("Done")


if __name__ == "__main__":
    main()
