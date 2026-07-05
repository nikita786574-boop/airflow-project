# Импортируем необходимые библиотеки
import requests as req
import pandas as pd
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ВАШ КОД:
# импортируйте библиотеки для работы с Connection, Variable, ClickHouseOperator и ClickHouseHook
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# получите данные из Connection api_exchange_rate
HOST_EXCR = BaseHook.get_connection('api_exchange_rate').host
PASSWORD_EXCR = BaseHook.get_connection('api_exchange_rate').password


exchange_rate = Variable.get('exchange_rate', deserialize_json=True)

TABLE_NAME = 'gmugzbq'


def extract_data(url, s_file, **kwargs):
    """
    Выгружает данные по валютам через GET-запрос
    и сохраняет результат в JSON-файл.
    """
    res = req.get(url)
    
    with open(s_file, 'w', encoding='utf-8') as file:
        file.write(res.text)


def transform_data(s_file, csv_file, **kwargs):
    """
    Читает JSON-файл, преобразует данные в табличный формат
    и сохраняет результат в CSV-файл.
    """
    with open(s_file, 'r', encoding='utf-8') as file:
        res = file.read()
    js = json.loads(res)
    date = kwargs['ds']
    data = []
    for key, value in js['quotes'][date].items():
        data.append({
            'date': date,
            'currency_source':key[:3],
            'currency':key[3:],
            'value': value,
        })
    df = pd.DataFrame(data)
    df.to_csv(csv_file)


def upload_to_clickhouse(csv_file, table_name):
    """
    Читает CSV-файл и загружает данные в ClickHouse.
    """
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    df = pd.read_csv(csv_file)
    hook.execute(f'INSERT INTO {TABLE_NAME} VALUES', df.to_dict('records'))


with DAG(
    'kaevyvk',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 10),
    max_active_runs=1
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_args=[
            f'{HOST_EXCR}?access_key={PASSWORD_EXCR}' + '&start_date={{ ds }}&end_date={{ ds }}&source=USD',
            exchange_rate['s_file'],
        ],
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=[
            exchange_rate['s_file'],
            exchange_rate['csv_file'],
        ],
    )

    create_table = ClickHouseOperator(
        task_id='create_table',
        sql=f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date String, currency_source String, currency String, value Float64) Engine = MergeTree ORDER BY date',
        clickhouse_conn_id='clickhouse_default',
    )

    upload_task = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse,
        op_args=[
            exchange_rate['csv_file'],
            TABLE_NAME,
        ],
    )

    extract_task >> transform_task >> create_table >> upload_task