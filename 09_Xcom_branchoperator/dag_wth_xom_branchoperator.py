from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from random import randint
from airflow.utils.dates import days_ago

default_args = {
    'owner':'airflow',
    'start_date': days_ago(1)
}

dag = DAG('dag', schedule_interval='@daily', default_args=default_args)

def rand(**kwargs):
  kwargs['ti'].xcom_push(key='rand', value=randint(0, 10))


def branch(**kwargs):
    value = kwargs['ti'].xcom_pull(key ='rand')
    if value > 5:
       return 'higher'
    return 'lower'

lower = DummyOperator(
    task_id = 'lower',
    dag=dag
)

higher = DummyOperator(
    task_id = 'higher',
    dag=dag
)

branch_op = BranchPythonOperator(
    task_id = 'branch_task',
    python_callable=branch,
    dag=dag
)

random_number = PythonOperator(
    task_id = 'random_number',
    python_callable=rand,
    dag=dag
)

random_number >> branch_op >> [lower, higher]