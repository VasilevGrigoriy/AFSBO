import logging
import sys

sys.path.append("/opt/airflow/afsbo/scripts/")

import airflow
from datetime import timedelta
from airflow import DAG

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from load_data_from_s3 import main as load_data_from_s3
from push_data_to_s3 import main as push_data_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(
    logging.Formatter(fmt="[%(asctime)s: %(levelname)s %(name)s] %(message)s")
)
logger.addHandler(handler)

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    # "end_date": datetime(),
    "depends_on_past": True,
    "email": ["vasiliev-gregmail.ru"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id="aaaa",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=timedelta(minutes=10),
    description="Clear data from s3 using spark script",
)

# logger.debug("process data with spark clear script")
# clear_spark_locals = SparkSubmitOperator(
#     task_id="clear_datafiles_with_spark",
#     application="python3 /opt/airflow/afsbo/clear_data/clear_data.py --files_dir uploaded_data/ --save_dir processed_data/",
#     conn_id="spark_local",
#     dag=dag_spark,
# )
