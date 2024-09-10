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
    outer_task = BashOperator(
        task_id='outer_ls_task',
        bash_command='ls'
    )

    # Set the task dependencies
    outer_task
