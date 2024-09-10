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
    description='A simple DAG World DAG',
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

    # Define the tasks
    ls_airflow_git_task = BashOperator(
        task_id='ls_airflow_git_task',
        bash_command='ls /opt/airflow/git'
    )

    # Define the tasks
    ls_airflow_dags_task = BashOperator(
        task_id='ls_airflow_dags_task',
        bash_command='ls /opt/airflow/dags'
    )

    # Define the tasks
    ls_echo_dagsfolder_task = BashOperator(
        task_id='ls_echo_dagsfolder_task',
        bash_command='ls /opt/airflow/git/sandbt-hris.git/dags'
    )

    # Define the tasks
    ls_cd_dagsfolder_task = BashOperator(
        task_id='ls_cd_dagsfolder_task',
        bash_command='cd $AIRFLOW__CORE__DAGS_FOLDER'
    )

    # Define the tasks
    ls_dbt_debug_task = BashOperator(
        task_id='ls_dbt_debug_task',
        bash_command='dbt debug'
    )

    # Set the task dependencies
    ls_airflow_dags_task >> ls_echo_dagsfolder_task >> ls_cd_dagsfolder_task >> ls_dbt_debug_task