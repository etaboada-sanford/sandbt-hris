from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Instantiate the DAG object
with DAG(
    'the_new_ls_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=None,
    catchup=False
) as dag:


    # Define the tasks
    pwd_task = BashOperator(
        task_id='pwd_task',
        bash_command='pwd'
    )

    # Define the tasks
    ls_task = BashOperator(
        task_id='ls_task',
        bash_command='ls'
    )

    # Define the tasks
    ls_airflow_task = BashOperator(
        task_id='ls_airflow_task',
        bash_command='ls /opt/airflow'
    )

    # Set the task dependencies
    pwd_task > ls_task > ls_airflow_task
