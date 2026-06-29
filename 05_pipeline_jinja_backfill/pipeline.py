import requests as req
from datetime import datetime
import pandas as pd
import json
from clickhouse_driver import Client

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

CH_CLIENT = Client(
    host=os.getenv('CH_HOST'),
    user=os.getenv('CH_USER'),
    password=os.getenv('CH_PASSWORD'),
    database=os.getenv('CH_DATABASE')
)

URL = '''https://api.exchangerate.host/timeframe?access_key={API_KEY}&source=USD&start_date={date}&end_date={date}'''
TABLE_NAME = 'htkrlqzq'

def extract_data(url, file_name, **kwargs):
    """
    Выгружаем данные из url в файл с именем file_name
    """
    response = req.get(url)
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(response.text)
    

def transform_data(s_file, csv_file, **kwargs):
    """
    Работаем с данными в формате JSON.
    Преобразуем в табличные. 
    Записываем в csv файл
    """
    text = ''
    with open(s_file, 'r', encoding='utf-8') as file:
        text = file.read()
    data = json.loads(text)
    date = kwargs['ds']
    transformed_data = []
    for key, value in data['quotes'][date].items():
        transformed_data.append({
            'date':date,
            'currency_source':'USD',
            'currency':key[3:],
            'value':value
        })
    df = pd.DataFrame(transformed_data)
    df.to_csv(csv_file, sep=',', encoding='utf-8', index=False)
    

def upload_to_clickhouse(csv_file, table_name, client, **kwargs):
    """
    Считывает CSV файл
    Создаёт таблицу в clickhouse
    Добавляет данные из файла в clickhouse
    """
    data_frame = pd.read_csv(csv_file)
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (date String, currency_source String, currency String, value Float64) Engine = MergeTree ORDER BY date')

    client.execute(f"ALTER TABLE {table_name} DELETE WHERE date='{kwargs['ds']}'")

    client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))
      


dag = DAG(
    'nxmfyf',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    end_date = datetime(2024, 1, 10),
    max_active_runs = 1,
    tags=['example'],
)

extract_task = PythonOperator(
    task_id = 'extract_task',
    python_callable = extract_data,
    op_kwargs = {
        'url':URL.format(API_KEY=os.getenv("API_KEY"), date='{{ds}}'),
        'file_name':'/usr/local/airflow/dags/sandbox/189613857/extracted_data.txt'
        } ,
    dag = dag
)

transform_task = PythonOperator(
    task_id = 'transform_task',
    python_callable = transform_data,
    op_kwargs = {'s_file':'/usr/local/airflow/dags/sandbox/189613857/extracted_data.txt',
                'csv_file':'/usr/local/airflow/dags/sandbox/189613857/transformed.csv'},
    dag = dag,
)

upload_task = PythonOperator(
    task_id = 'upload_task',
    python_callable = upload_to_clickhouse,
    op_args = ['/usr/local/airflow/dags/sandbox/189613857/transformed.csv', TABLE_NAME, CH_CLIENT],
    dag = dag
)

extract_task >> transform_task >> upload_task