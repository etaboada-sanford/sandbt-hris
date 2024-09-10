from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import the function
from your_script import list_installed_packages

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'list_installed_packages_dag',
    default_args=default_args,
    description='A DAG to list installed Python packages',
    schedule_interval='@once',
)

# Define the task
list_packages_task = PythonOperator(
    task_id='list_packages',
    python_callable=list_installed_packages,
    dag=dag,
)
