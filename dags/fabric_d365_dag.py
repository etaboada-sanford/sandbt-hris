import os
from pathlib import Path
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dags" / "fabric_d365"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
profile_config = ProfileConfig(
    profile_name="fabric_d365",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml",
)

dbt_fabric_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH,),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_fabric_dag",
)




# Define the task
dbt_fabric_task = BashOperator(
    task_id='run_dbt_model1',
    bash_command='dbt deps',
    dag=dbt_fabric_dag
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with the specified schedule interval
dag = DAG('dbt_dag', default_args=default_args, schedule_interval=timedelta(days=1))
# Define the dbt run command as a BashOperator
run_dbt_model = BashOperator(
    task_id='run_dbt_model2',
    bash_command='dbt run',
    dag=dag
)