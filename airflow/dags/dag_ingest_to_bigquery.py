# -*- coding: utf-8 -*-
"""
Apache Airflow DAG: Nạp dữ liệu MinIO Parquet -> Google BigQuery Batch Load (Phase 3)
Lịch chạy: Mỗi 15 phút
Sau khi nạp xong, tự động kích hoạt dag_dbt_transform.
"""

import os
import sys
from datetime import datetime, timedelta

# Thêm thư mục gốc dự án vào sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def execute_batch_ingestion(**kwargs):
    """
    Hàm thực thi nạp dữ liệu batch vào BigQuery.
    """
    from data_lake_client.bigquery_loader import BigQueryBatchLoader

    loader = BigQueryBatchLoader(
        key_path=os.path.join(PROJECT_ROOT, "gcp_key.json"),
        project_id=os.getenv("GCP_PROJECT_ID", "xombank-fraud-analytics"),
        dataset_id=os.getenv("BIGQUERY_DATASET", "xombank_dw"),
        table_id="raw_transactions"
    )

    result = loader.load_parquet_from_minio()
    print(f"Ingestion result: {result}")
    return result


default_args = {
    "owner": "xombank_data_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="dag_ingest_to_bigquery",
        default_args=default_args,
        description="Batch load transactions from MinIO Data Lake to BigQuery raw_transactions table",
        schedule_interval="*/15 * * * *",
        catchup=False,
        tags=["xombank", "batch", "minio", "bigquery"]
    )

    task_ingest = PythonOperator(
        task_id="ingest_minio_parquet_to_bigquery",
        python_callable=execute_batch_ingestion,
        dag=dag
    )

    task_trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="dag_dbt_transform",
        wait_for_completion=False,
        dag=dag
    )

    task_ingest >> task_trigger_dbt
else:
    dag = None
