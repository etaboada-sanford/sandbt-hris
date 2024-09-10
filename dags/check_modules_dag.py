from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
from pprint import pprint

def check_modules():
    pprint(sys.modules)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('check_modules_dag', default_args=default_args, schedule_interval='@once')

check_modules_task = PythonOperator(
    task_id='check_modules',
    python_callable=check_modules,
    dag=dag,
)
