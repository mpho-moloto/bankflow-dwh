"""
bankflow_pipeline.py
--------------------
AIRFLOW DAG — BankFlow DWH Pipeline

This DAG runs the full Medallion pipeline once a day at midnight:

    generate_data → bronze_ingest → spark_silver → gold_load → dbt_run → dbt_test → export_reports

Each task is independent, which means if one fails you can rerun it
without rerunning everything before it (assuming the previous outputs
are still there).

Task descriptions:
  generate_data   — runs the Faker script to produce new CSV data
  bronze_ingest   — loads CSVs into Parquet (bronze layer)
  spark_silver    — Spark job: type casting, dedup, feature engineering
  gold_load       — loads Silver into PostgreSQL star schema (SCD2 + incremental)
  dbt_run         — builds staging views + mart tables in PostgreSQL
  dbt_test        — runs dbt data quality tests (uniqueness, not_null, etc.)
  export_reports  — exports mart tables to CSV in /exports/
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv('/opt/airflow/project/.env')

log = logging.getLogger(__name__)

default_args = {
    'owner':             'bankflow',
    'depends_on_past':   False,
    'retries':           2,
    'retry_delay':       timedelta(minutes=5),
    'email_on_failure':  False,
}


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def task_generate_data(**context):
    """
    Generate a daily batch of synthetic banking data.
    No --fresh flag = data is appended, not replaced.
    Transactions get new IDs starting from where we left off.
    """
    import sys
    sys.argv = [sys.argv[0]]
    sys.path.append('/opt/airflow/project')
    

    from data_generator.generate_banking_data import (
        generate_branches, generate_merchants,
        generate_customers, generate_accounts, generate_transactions
    )

    log.info("Generating daily batch of synthetic data...")
    branches_df  = generate_branches(n=50)
    merchants_df = generate_merchants()
    customers_df = generate_customers(n=50)       # 50 new customers per day
    accounts_df  = generate_accounts(customers_df, branches_df, n=100)
    txn_df       = generate_transactions(accounts_df, merchants_df, n=5_000)

    # Push counts to XCom so downstream tasks can log them
    context['ti'].xcom_push(key='stats', value={
        'customers':    len(customers_df),
        'accounts':     len(accounts_df),
        'transactions': len(txn_df),
    })


def task_bronze_ingest(**context):
    import sys
    sys.path.append('/opt/airflow/project')
    from ingestion.bronze_loader import run
    run()


def task_gold_load(**context):
    import sys
    sys.path.append('/opt/airflow/project')

    # Make sure the loader uses the correct internal Docker hostnames
    os.environ['SILVER_DATA_PATH'] = '/opt/airflow/project/data/silver'
    os.environ['POSTGRES_HOST']    = 'postgres'
    os.environ['POSTGRES_PORT']    = '5432'

    from warehouse.loader.db_manager import GoldLoader
    GoldLoader().run_all()


def task_export_reports(**context):
    """
    Dump the three mart tables to CSV so stakeholders can open them in Excel
    without needing database access.
    """
    import sys
    import pandas as pd
    sys.path.append('/opt/airflow/project')

    from sqlalchemy import create_engine
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER','postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD','postgres')}@"
        f"postgres:5432/"
        f"{os.getenv('POSTGRES_DB','bankflow_dwh')}"
    )

    os.makedirs('/opt/airflow/project/exports', exist_ok=True)
    date_str = datetime.now().strftime('%Y%m%d')

    reports = {
        'customer_360':   'SELECT * FROM marts.mart_customer_360 LIMIT 5000',
        'fraud_signals':  "SELECT * FROM marts.mart_fraud_signals WHERE alert_level IN ('CRITICAL','HIGH')",
        'daily_summary':  'SELECT * FROM marts.mart_daily_transaction_summary WHERE transaction_date >= CURRENT_DATE - 30',
    }

    for name, query in reports.items():
        try:
            df = pd.read_sql(query, engine)
            path = f'/opt/airflow/project/exports/{name}_{date_str}.csv'
            df.to_csv(path, index=False)
            log.info(f"Exported {name}: {len(df):,} rows -> {path}")
        except Exception as e:
            log.warning(f"Export {name} failed: {e}")


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

# Shared env vars for dbt tasks (picks up .env values)
dbt_env = {
    'POSTGRES_USER':     os.getenv('POSTGRES_USER', 'postgres'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'POSTGRES_DB':       os.getenv('POSTGRES_DB', 'bankflow_dwh'),
}

with DAG(
    dag_id='bankflow_dwh_pipeline',
    default_args=default_args,
    description='BankFlow DWH — Medallion pipeline (Bronze → Silver → Gold → dbt)',
    schedule_interval='0 0 * * *',   # midnight every day
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'dwh', 'pyspark', 'medallion'],
) as dag:

    t_generate = PythonOperator(
        task_id='generate_synthetic_data',
        python_callable=task_generate_data,
    )

    t_bronze = PythonOperator(
        task_id='bronze_ingest',
        python_callable=task_bronze_ingest,
    )

    t_silver = BashOperator(
        task_id='spark_silver_transform',
        bash_command=(
            'docker exec bankflow_spark_master '
            '/opt/spark/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '/opt/spark/project/processing/silver_transformer.py'
        ),
        execution_timeout=timedelta(minutes=30),
    )

    t_gold = PythonOperator(
        task_id='gold_load_kimball',
        python_callable=task_gold_load,
        execution_timeout=timedelta(minutes=60),
    )

    t_dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/project/transformation && dbt run --profiles-dir . --target prod',
        env=dbt_env,
        append_env=True,
    )

    t_dbt_test = BashOperator(
        task_id='dbt_test_quality',
        bash_command='cd /opt/airflow/project/transformation && dbt test --profiles-dir . --target prod',
        env=dbt_env,
        append_env=True,
    )

    t_export = PythonOperator(
        task_id='export_csv_reports',
        python_callable=task_export_reports,
    )

    # Pipeline order — each arrow is a dependency
    t_generate >> t_bronze >> t_silver >> t_gold >> t_dbt_run >> t_dbt_test >> t_export
