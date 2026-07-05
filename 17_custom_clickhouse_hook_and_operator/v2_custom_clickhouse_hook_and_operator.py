from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago 
import pandas as pd
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

TABLE_NAME = 'tszyfo_table'

class ClickhouseTransferHook(ClickHouseHook): 

    def get_pandas_df(self, url_or_path):
        """ Код который читает данные из файла
        """
        # Ваш код который читает и возвращает данные из CSV файла в pandas Data Frame
        # Используйте обычный pandas
        df = pd.read_csv(url_or_path)
        return df
        

    def insert_df_to_db(self, data_frame, table_name):
        """ Данный метод вставляет Data Frame в ClickHouse
        """ 
        # Объект ClickHouseHook может используя подключение к бд выполнить SQL код
        self.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))


class ClickhouseTransferOperator(BaseOperator):

    def __init__(self, path, table_name, **kwargs):
        super().__init__(**kwargs)
        self.hook = None 
        self.path = path 
        self.table_name= table_name 


    def execute(self, context):
        
        # Создание объекта хука
        self.hook = ClickhouseTransferHook(clickhouse_conn_id='clickhouse_default')
        
        self.hook.insert_df_to_db(self.hook.get_pandas_df(self.path), TABLE_NAME)
        


# DAG
dag = DAG('tszyfo', schedule_interval='@daily',  start_date=days_ago(1))


# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql=f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (campaign String, cost Int64, date  String) ENGINE Log',
    clickhouse_conn_id='clickhouse_default', 
    dag=dag,
)


# Оператор для трансфера данных
transfer_data = ClickhouseTransferOperator(
  task_id='transfer_data', 
  path=f'{os.getenv("PATH")}', 
  table_name = 'campaign_table',
  dag=dag)


create_table >> transfer_data