import logging

import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from afsbo.utils import init_basic_logger

logger = init_basic_logger(__name__, logging.DEBUG)

default_args = {
    "owner": "airflow",
    # "start_date": airflow.utils.dates.days_ago(2),
    # "end_date": datetime(),
    "depends_on_past": True,
    "email": ["vasiliev-gregmail.ru"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag_spark = DAG(
    dag_id="ClearDataSpark",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=timedelta(minutes=10),
    description="Clear data from s3 using spark script",
)


logger.debug("pull raw data from s3 to local")
pull_data_from_s3 = BashOperator(
    task_id="upload_data_from_s3",
    bash_command="python3 /home/greg/afsbo/scripts/load_data_from_s3.py --bucket_name mlops-otus-task2 --data_folder test --save_dir uploaded_data/",
    dag=dag_spark,
    run_as_user="VasilevGrigoriy",
)

logger.debug("process data with spark clear script")
clear_spark_locals = SparkSubmitOperator(
    task_id=f"clear_datafiles_with_spark",
    application="python3 /home/greg/afsbo/clear_data/clear_data.py --files_dir uploaded_data/ --save_dir processed_data/",
    conn_id="spark_local",
    dag=dag_spark,
)

logger.debug("push processed data to s3")
push_data_to_s3 = BashOperator(
    task_id="upload_data_from_s3",
    bash_command="python3 /home/greg/afsbo/scripts/push_data_to_s3.py --bucket_name mlops-otus-task2 --upload_folder test_processed_data --files_dir processed_data/",
    dag=dag_spark,
    run_as_user="VasilevGrigoriy",
)

pull_data_from_s3 >> clear_spark_locals >> push_data_to_s3
