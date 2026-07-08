# Импортируем необходимые библиотеки
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 

# Настройка подключения к базе данных ClickHouse

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


exchange_rate = Variable.get("exchange_rate", deserialize_json=True)

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, s_file, **kwargs):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    ВАШ КОД

# Функция для обработки данных в формате JSON и преобразования их в CSV
def transform_data(s_file, csv_file, **kwargs):
    """
    Эта функция обрабатывает полученные данные в формате JSON
    и преобразует их в табличном формате для дальнейшей работы.
    В конце данные записываются в CSV файл
    """
    ВАШ КОД

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client, **kwargs):
    """
    Эта функция считывает CSV файл, создает таблицу в
    базе данных ClickHouse и добавляет данные в неё
    """
    ВАШ КОД

with DAG(
    ВАШ КОД,
    schedule_interval='@daily',  # Запуск каждый день
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,5),
    max_active_runs=1
) as dag:

    # Операторы для выполнения шагов
    extract_task = PythonOperator(
        ВАШ КОД
    )

    transform_task = PythonOperator(
        ВАШ КОД
    )
    
 # Оператор для выполнения запроса
    create_table = ClickHouseOperator(
        ВАШ КОД
    )

    upload_task = PythonOperator(
        ВАШ КОД
    )
    
 # Оператор для выполнения запроса
    create_new_order_table = ClickHouseOperator(
        task_id='create_new_order_table ',
        sql='CREATE TABLE IF NOT EXISTS new_order_table (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) ENGINE Log',
        clickhouse_conn_id='clickhouse_default', 
        dag=dag,
    )
    
 # Оператор для выполнения запроса
 # Не забывайте использовать  {{ ds }} а также если вам нужно обратиться к другой базу данных
 # То просто пропишите эт ов SQL <airflow.order>
    insert_join = ClickHouseOperator(
        task_id='insert_join',
        sql='INSERT INTO ...таблица... SELECT ...',
        clickhouse_conn_id='clickhouse_default', 
        dag=dag,
    )

    # Определение зависимостей между задачами
    extract_task >> transform_task >> [create_table, create_new_order_table ] >> upload_task >> insert_join