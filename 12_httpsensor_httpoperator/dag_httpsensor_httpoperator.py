from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor


import json


dag = DAG(
    dag_id='eklfr',
    schedule_interval='@daily',  
    start_date=days_ago(1)
)

def response_operator(response, **kwargs):
    if 'report_id' in response.json():
        kwargs['ti'].xcom_push(key='report_id', value=response.json()['report_id'])
        return True
    
def response_sensor(response, **kwargs):
    return response.json()['message'] == 'The report is ready!'

# HTTP-оператор для отправки запроса на создание отчёта
start_report_task = HttpOperator(
    task_id='start_report_task',
    http_conn_id='report_api', 
    endpoint='start_report', 
    method='GET',
    response_check=response_operator,  # Проверка на наличие Report ID
    dag=dag,
)

# HTTP-сенсор для проверки готовности отчёта
check_report_task = HttpSensor(  
    task_id='check_report_task',
    http_conn_id='report_api',  # Укажите ваше соединение
    endpoint='check_report/' +"{{ti.xcom_pull(key='report_id')}}",
    response_check=response_sensor, # Проверка что отчет готов
    method='GET',
    poke_interval=2,  
    timeout=60,  
    mode='poke',  
    dag=dag,
)

# Определение порядка выполнения задач
start_report_task >> check_report_task