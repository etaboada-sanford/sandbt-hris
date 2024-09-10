import os
from pathlib import Path
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

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
