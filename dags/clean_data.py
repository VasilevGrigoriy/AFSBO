from datetime import timedelta, date
import os
import dotenv

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

import dotenv
dotenv_file = dotenv.find_dotenv("../.env")
dotenv.load_dotenv(dotenv_file)
DATE = os.environ.get("DATE")
# Обновляем дату на следующий месяц
os.environ["DATE"] = str(date.fromisoformat(DATE) + timedelta(days=31))
dotenv.set_key(dotenv_file, "DATE", os.environ["DATE"])

sshHook = SSHHook(ssh_conn_id="spark_server", cmd_timeout=18000, conn_timeout=18000)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "email": ["vasiliev-greg@mail.ru"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

command = f'cd project/ \
&& curl -sSL https://install.python-poetry.org | python3 - \
&& export PATH="$HOME/.local/bin:$PATH" \
$$ export DATE={DATE} \
&& poetry install \
&& source $(poetry env info --path)/bin/activate \
&& python3 afsbo/clean_data/clean_data.py'

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
        command=command,
        ssh_hook=sshHook,
    )

clean_data
