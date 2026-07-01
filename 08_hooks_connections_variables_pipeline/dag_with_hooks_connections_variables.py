import requests as req
from datetime import datetime
import pandas as pd
import json
from clickhouse_driver import Client

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

HOST = BaseHook.get_connection('clickhouse_default').host
USER = BaseHook.get_connection('clickhouse_default').login
PASSWORD = BaseHook.get_connection('clickhouse_default').password
DATABASE = BaseHook.get_connection('clickhouse_default').schema

HOST_EXCR = BaseHook.get_connection('api_exchange_rate').host
PASSWORD_EXCR = BaseHook.get_connection('api_exchange_rate').password

exchange_rate = Variable.get('exchange_rate', deserialize_json=True)

CH_CLIENT = Client(
    host= HOST,
    user= USER,
    password= PASSWORD,
    database= DATABASE
    )

TABLE_NAME = 'yuusovoq'

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
    
print(exchange_rate['s_file'])

dag = DAG(
    'majdvfyv',
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
        'url':f'{HOST_EXCR}?access_key={PASSWORD_EXCR}' + '&start_date={{ ds }}&end_date={{ ds }}&source=USD',
        'file_name':exchange_rate['s_file']
        },
    dag = dag
)

transform_task = PythonOperator(
    task_id = 'transform_task',
    python_callable = transform_data,
    op_kwargs = {'s_file':exchange_rate['s_file'],
                'csv_file':exchange_rate['csv_file']},
    dag = dag,
)

upload_task = PythonOperator(
    task_id = 'upload_task',
    python_callable = upload_to_clickhouse,
    op_args = [exchange_rate['csv_file'], TABLE_NAME, CH_CLIENT],
    dag = dag
)

extract_task >> transform_task >> upload_task