from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from cosmos import dbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import psycopg2
import os

#config postgres db via cosmos
profile_config = ProfileConfig(
    profile_name = "pipeda_pipeline",
    target_name = "dev",
    profile_mapping = PostgresUserPasswordProfileMapping(
        conn_id = "pipeda_postgres",
        profile_args={"schema": "dbt_dev"},
    ),
)

def ingest_raw_data():
    import subprocess
    subprocess.run(["python", "../../scripts/gen_consent_events.py"], check=True)
    subprocess.run(["python", "../../scripts/gen_dsr_requests.py"], check=True)
    print("Raw data ingested successfully.")


#DAG def
default_args = {
    'owner': 'pipeda_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='pipeda_compliance_pipeline',
    default_args=default_args,
    description='daily PIPEDA consent audit pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags = ['pipeda', 'compliance', 'audit', 'dbt'],
) as dag:

    ingest = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_raw_data,
    )

    #cosmos dbt operator to run dbt models after data ingestion
    dbt_run = DbtDag(
        project_config = ProjectConfig("../../pipeda_pipeline"),
        profile_config = profile_config,
        dag_id="pipeda_dbt", 
        schedule_interval=None,
    )

    ingest >> dbt_run
