from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago



def on_failure_callback(context):
  send_message = TelegramOperator(
    task_id = 'send_message_telegram',
    telegram_conn_id = 'airflow_notification_telegram_189613857',
    chat_id = '-5193668403',
    text='The DAG crashed again—fix it immediately, we are losing time.',
    dag = dag
  )
  return send_message.execute(context=context)

default_args = {
  'on_failure_callback':on_failure_callback
}

def failed_task():
  raise AirflowException

dag = DAG(
  dag_id = 'dag_189613857',
  default_args = default_args,
  schedule_interval = None,
  start_date = days_ago(1)
)

python_task = PythonOperator(
  task_id = 'failute_task',
  python_callable = failed_task,
  dag = dag
)

python_task
