from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

WORKDIR = "/opt/airflow"

with DAG(
    dag_id="airbnb_elt_pipeline",
    description="ELT pipeline: download -> PySpark bronze -> dbt silver -> dbt gold",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airbnb", "elt", "medallion"],
) as dag:

    download_csvs = BashOperator(
        task_id="download_csvs",
        bash_command=f"cd {WORKDIR} && python spark/ingest.py download",
    )

    spark_load_bronze = BashOperator(
        task_id="spark_load_bronze",
        bash_command=f"cd {WORKDIR} && python spark/ingest.py bronze",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {WORKDIR}/dbt_project && dbt run --select staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {WORKDIR}/dbt_project && dbt run --select marts --profiles-dir .",
    )

    download_csvs >> spark_load_bronze >> dbt_run_staging >> dbt_run_marts
