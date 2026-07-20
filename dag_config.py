from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
import socket
from airflow.utils.dates import days_ago

def whoami():
    print("executor      :", conf.get("core", "executor"))
    print("sql_alchemy   :", conf.get("database", "sql_alchemy_conn", fallback="?"))
    print("parallelism   :", conf.get("core", "parallelism", fallback="?"))
    print("hostname      :", socket.gethostname())
    print("celery broker :", conf.get("celery", "broker_url", fallback="—"))

with DAG(
    dag_id="std17_168",
    start_date=days_ago(1),
    schedule=None,         
    catchup=False,
    tags=["std17_168"],
):
    PythonOperator(task_id="whoami", python_callable=whoami)
