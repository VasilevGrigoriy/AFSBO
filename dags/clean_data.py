from datetime import timedelta

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

sshHook = SSHHook(ssh_conn_id="spark_server", cmd_timeout=18000, conn_timeout=18000)

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
    schedule_interval="0 0 * * *",
    dagrun_timeout=timedelta(hours=3),
    description="Clear data from s3 using spark script",
) as dag:

    clean_data = SSHOperator(
        task_id="clear-data",
        command="cd project/\
            && curl -sSL https://install.python-poetry.org | python3\
            && export PATH='/root/.local/bin:$PATH'\
            && poetry shell\
            && poetry install --no-interaction --no-ansi -vvv --with=dev\
            && poetry install --no-interaction --no-ansi -vvv --with=pyspark\
            && poetry install --no-interaction --no-ansi -vvv --with=s3\
            && poetry install --no-interaction --no-ansi -vvv --with=logs\
            && poetry install --no-interaction --no-ansi -vvv --with=click\
            && poetry install --no-interaction --no-ansi -vvv --with=airflow\
            && python3 afsbo/clear_data/clear_data.py",
        ssh_hook=sshHook,
    )

clean_data
