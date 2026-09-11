# -*- coding: utf-8 -*-
"""
Apache Airflow DAG: Biến Đổi dbt (Phase 4)
Xây dựng các model Staging, Intermediate và Marts trên BigQuery và chạy kiểm thử schema.
Lịch chạy: Kích hoạt tự động bởi dag_ingest_to_bigquery sau khi batch load hoàn thành.
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def run_dbt_command(command: str):
    """
    Thực thi lệnh dbt CLI sử dụng Python virtual environment của dự án.
    """
    dbt_bin = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "dbt.exe")
    if not os.path.exists(dbt_bin):
        dbt_bin = "dbt"

    full_cmd = [
        dbt_bin,
        command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR
    ]

    print(f"[*] Executing dbt command: {' '.join(full_cmd)}")
    result = subprocess.run(full_cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")


def execute_dbt_run(**kwargs):
    run_dbt_command("run")


def execute_dbt_test(**kwargs):
    run_dbt_command("test")


default_args = {
    "owner": "xombank_data_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="dag_dbt_transform",
        default_args=default_args,
        description="Thực thi mô hình biến đổi dbt và kiểm thử chất lượng dữ liệu trên BigQuery",
        schedule_interval=None,  # Được kích hoạt tự động bởi dag_ingest_to_bigquery
        catchup=False,
        tags=["xombank", "dbt", "transformation", "bigquery"]
    )

    task_dbt_run = PythonOperator(
        task_id="dbt_run_models",
        python_callable=execute_dbt_run,
        dag=dag
    )

    task_dbt_test = PythonOperator(
        task_id="dbt_test_models",
        python_callable=execute_dbt_test,
        dag=dag
    )

    task_dbt_run >> task_dbt_test
else:
    dag = None
