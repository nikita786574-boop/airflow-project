from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

import random


# Кастомный хук
class CustomHook(BaseHook):
      def random_number(self):
            random.seed(10, version = 2)
            
            return random.randint(0,10)

# Кастомный оператор
class CustomOperator(BaseOperator):
      def __init__(self,**kwargs,):
            super().__init__(**kwargs)
            self.hook=None

# Метод отправляет в Xcom некотрое значение
      def execute(self, context):

        self.hook = CustomHook()
        print(self.hook.random_number())

dag=DAG('189613857', schedule_interval=timedelta(days=1), start_date=days_ago(1))

t1=CustomOperator(task_id='task_1',dag=dag)
