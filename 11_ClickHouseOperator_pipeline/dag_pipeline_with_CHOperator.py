from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime

from airflow.hooks.base_hook import BaseHook

TABLE_NAME = 'puzdqhao' # Укажите имя таблицы
HOST_EXCR = BaseHook.get_connection('api_exchange_rate').host
PASSWORD_EXCR = BaseHook.get_connection('api_exchange_rate').password

# Создаем DAG
dag = DAG(
    'ogffohft',
    description='Пример использования ClickHouseOperator',
    schedule_interval='@daily', 
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4)
)


# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql=f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date String, currency_source String, currency String, value Float64)  Engine = MergeTree ORDER BY date',
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)


# SQL-запрос, вставка данных в нашу таблицу после того как мы их выгрузим из API
sql_query = """ INSERT INTO {TABLE_NAME}
SELECT '{{ds}}' AS date, currency_source, currency, value
FROM (
    SELECT arrayJoin(splitByChar(',', replaceAll(replaceAll(replaceAll(visitParamExtractRaw(column, '{{ds}}'), '{', ''), '}', ''), '"', ''))) AS col1,
           LEFT(col1, 3) AS currency_source,
           SUBSTRING(col1, 4, 3) AS currency,
           SUBSTRING(col1, 8) AS value
    FROM url('{HOST_EXCR}?access_key={PASSWORD_EXCR}&start_date={{ ds }}&end_date={{ ds }}&source=USD'
  , LineAsString, 'column String')
)
   
"""
# Вместо format и f-строк
sql_query = (
    sql_query
    .replace('{TABLE_NAME}', TABLE_NAME)
    .replace('{HOST_EXCR}', HOST_EXCR)
    .replace('{PASSWORD_EXCR}', PASSWORD_EXCR)
)



# Оператор для вствыки данных в таблицу
insert_data = ClickHouseOperator(
    task_id='insert_data',
    sql=sql_query,  # SQL запрос, который нужно выполнить
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)

create_table >> insert_data