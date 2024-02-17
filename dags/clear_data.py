from datetime import timedelta

import airflow
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

sshHook = SSHHook(ssh_conn_id="better_spark", cmd_timeout=18000, conn_timeout=18000)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "email": ["vasiliev-greg@mail.ru"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="clean-data-with-spark-cluster",
    default_args=default_args,
    start_date=days_ago(0),
    schedule_interval="0 */5 * * *",
    dagrun_timeout=timedelta(hours=5),
    description="Clear data from s3 using spark script",
) as dag:

    run_this = SSHOperator(
        task_id="clear-data",
        command="cd project/ && pip install -r requirements.txt && python3 afsbo/clear_data/clear_data.py",
        ssh_hook=sshHook,
    )

run_this
