from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

WORKDIR = "/opt/airflow"

with DAG(
    dag_id="airbnb_elt_pipeline",
    description="ELT pipeline: download -> Kafka -> bronze -> dbt silver -> dbt gold",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["airbnb", "elt", "medallion", "kafka"],
) as dag:

    download_csvs = BashOperator(
        task_id="download_csvs",
        bash_command=f"cd {WORKDIR} && python spark/ingest.py download",
    )

    produce_to_kafka = BashOperator(
        task_id="produce_to_kafka",
        bash_command=f"cd {WORKDIR} && python streaming/producer.py",
    )

    consume_to_bronze = BashOperator(
        task_id="consume_to_bronze",
        bash_command=f"cd {WORKDIR} && python streaming/consumer.py",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {WORKDIR}/dbt_project && dbt run --select staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {WORKDIR}/dbt_project && dbt run --select marts --profiles-dir .",
    )

    download_csvs >> produce_to_kafka >> consume_to_bronze >> dbt_run_staging >> dbt_run_marts
