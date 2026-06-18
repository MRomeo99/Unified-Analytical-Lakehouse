"""
Airflow DAG equivalent of the Beacon Lakehouse Dagster pipeline.

This stub exists for Airflow keyword coverage (see ADR 003).
The production implementation uses Dagster SDAs, but the same pipeline
can be expressed as an Airflow DAG as shown below.

To run: copy this file into your Airflow DAGs folder and install the operators.
"""

from datetime import datetime, timedelta

# Airflow imports (requires apache-airflow installed)
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    raise ImportError("Install apache-airflow to use this DAG: pip install apache-airflow")

default_args = {
    "owner": "beacon",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="beacon_full_pipeline",
    description="Bronze → Silver → Gold pipeline (Airflow equivalent of Dagster SDAs)",
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC (mirrors Dagster schedule)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["beacon", "lakehouse"],
) as dag:

    # Bronze layer — dlt ingestion
    ingest_postgres = BashOperator(
        task_id="ingest_postgres",
        bash_command="python ingestion/pipelines.py --source postgres",
    )

    ingest_seo = BashOperator(
        task_id="ingest_seo",
        bash_command="python ingestion/pipelines.py --source seo",
    )

    ingest_ad_spend = BashOperator(
        task_id="ingest_ad_spend",
        bash_command="python ingestion/pipelines.py --source ad_spend",
    )

    # Silver layer — dbt staging
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="dbt run --select staging --project-dir transform/beacon",
    )

    # Great Expectations quality gate (equivalent to Dagster AssetChecks)
    gx_checkpoint = BashOperator(
        task_id="great_expectations_checkpoint",
        bash_command=(
            'python -c "'
            "import great_expectations as gx; "
            "ctx = gx.get_context(context_root_dir='quality'); "
            "r = ctx.run_checkpoint(checkpoint_name='beacon_checkpoint'); "
            'exit(0 if r.success else 1)"'
        ),
    )

    # Gold layer — dbt marts
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="dbt run --select marts --project-dir transform/beacon && dbt test --select marts --project-dir transform/beacon",
    )

    # DAG dependency graph mirrors Dagster asset dependencies
    [ingest_postgres, ingest_seo, ingest_ad_spend] >> dbt_silver >> gx_checkpoint >> dbt_gold

    # Airflow SLA miss callback (equivalent to Dagster FreshnessPolicy)
    # In production, replace with SLAMissCallback or use Airflow sensors
    # to trigger alerts when Gold tables are stale > 26 hours.
