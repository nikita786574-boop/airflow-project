from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os

folder_path = "/usr/local/airflow/plugins/sql/"
tasks = []

from datetime import timedelta
dag = DAG(
    dag_id = 'yokusfgl',
    schedule_interval = timedelta(days = 1),
    start_date = days_ago(1)

)
for file_name in os.listdir(folder_path):
      with open(folder_path + file_name) as file:
          task = ClickHouseOperator(
              task_id = f'task_{file_name}',
              clickhouse_conn_id='clickhouse_default', 
              dag = dag,
              sql=f"CREATE VIEW {file_name.split('.')[0]}_yokusfgl AS {file.read()}"
          )
      tasks.append(task)
  
      

for i in range(len(tasks)):
    if i:
        tasks[i-1] >> tasks[i]
