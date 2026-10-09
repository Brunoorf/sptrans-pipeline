from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt_sptrans"


@dag(
    dag_id="sptrans_transform_dbt",
    start_date=datetime(2026, 9, 1),
    schedule="15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["sptrans", "dbt"],
)
def sptrans_transform_dbt():

    BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --no-use-colors",
    )


sptrans_transform_dbt()