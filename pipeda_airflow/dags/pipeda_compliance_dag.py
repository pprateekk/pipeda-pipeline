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

#function to check for SLA breaches and push results to XCom for alerting
def check_sla_breaches(**context):
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, breached_dsr_count, days_until_next_breach
        FROM dbt_dev.marts_pipeda_compliance_report
        WHERE breached_dsr_count > 0
        ORDER BY days_until_next_breach
        """)
    breaches = cur.fetchall()
    cur.close()
    conn.close()

    if breaches:
        context['ti'].xcom_push(key='breaches', value=breaches)
        print(f"{len(breaches)} users with SLA breaches found.")
        return True #ShortCircuitOperator will continue to next task = ALERT
    else:
        print("No SLA breaches found.")
        return False #ShortCircuitOperator will skip next task

def send_sla_alert(**context):
    breaches = context['ti'].xcom_pull(key='breaches', task_ids='check_sla_breaches')
    print("PIPEDA SLA Breach Alert:")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("User info: ")
    for user_id, breach_count, days_until in breaches:
        print(f"ALERT: User {user_id} has {breach_count} breached DSRs. Next breach in {days_until} days.")
    
    #add code here to send email or Slack alert with breach details

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

    check_breaches = ShortCircuitOperator(
        task_id='check_sla_breaches',
        python_callable=check_sla_breaches,
        provide_context=True,
    )

    alert = PythonOperator(
        task_id='send_sla_alert',
        python_callable=send_sla_alert,
        provide_context=True,
    )

    ingest >> dbt_run >> check_breaches >> alert
