from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException # Нужно для вызова исключений в Airflow
import pandas as pd
import requests
import json

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os

# Настройка подключения к базе данных ClickHouse
HOST = BaseHook.get_connection("clickhouse_default").host
USER = BaseHook.get_connection("clickhouse_default").login
PASSWORD = BaseHook.get_connection("clickhouse_default").password
DATABASE = BaseHook.get_connection("clickhouse_default").schema

CH_CLIENT = Client(
    host=HOST,
    user=USER,  
    password=PASSWORD,  
    database=DATABASE  
)

dag =  DAG('test_xcom', schedule_interval='@daily', start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 4), tags=['examples'], max_active_runs=1)

def fetch_data_to_xcom(api_url, **kwargs):
    
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='files_full', include_prior_dates=True) 
    if files is None:
        files = []
    
    response = requests.get(api_url + kwargs['ds'])  
    
    if response.status_code == 200:
        # Парсинг JSON ответа
        now_files = json.loads(response.text).get('files', [])
        
        # Находим разницу в 2 массивах
        difference = [x for x in now_files if x not in files]
        
        # Отправляем разницу в Xcom с другим ключем
        task_instance.xcom_push(key='file_difference', value = difference)
        task_instance.xcom_push(key='files_full', value = now_files)
    else:
        raise AirflowException(f"Request failed {response.status_code}")
        
# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(url, table_name, client, **kwargs):
    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='file_difference')

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (campaign String, cost Int64, date  String) ENGINE MergeTree ORDER BY date')
    
    # Итеративно проходимся по файлам и добавляем в ClickHouse
    for file in files:        
        # Чтение данных из CSV
        data_frame = pd.read_csv(url + file)  
        client.execute(f"ALTER TABLE {table_name} DELETE WHERE date='{data_frame['date'][0]}'")
        # Запись data frame в ClickHouse
        client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records')) 

fetch_data_to_xcom = PythonOperator(
    task_id='fetch_data_to_xcom',
    python_callable=fetch_data_to_xcom,
    op_args = [os.getenv('API_FILES')],
    dag=dag,
)

# Задачи для загрузки данных 
upload_to_clickhouse = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args = [os.getenv('API_DOWNLOAD'), 'campaign_table', CH_CLIENT],
    dag=dag,
)


fetch_data_to_xcom >> upload_to_clickhouse