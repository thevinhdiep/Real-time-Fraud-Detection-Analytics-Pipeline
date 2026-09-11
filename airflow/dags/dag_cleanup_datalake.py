# -*- coding: utf-8 -*-
"""
Apache Airflow DAG: Dọn Dẹp MinIO Data Lake (Phase 3 bổ sung)
Xóa các file Parquet cũ hơn 7 ngày khỏi MinIO bucket để quản lý lưu trữ.
Lịch chạy: Hàng ngày lúc 02:00 UTC
"""

import os
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def execute_cleanup(**kwargs):
    """
    Quét MinIO bucket tìm file Parquet cũ hơn số ngày lưu trữ và xóa chúng.
    Đồng thời dọn dẹp các thư mục lưu trữ local fallback rỗng.
    """
    from data_lake_client.minio_client import MinIODataLakeClient

    retention_days = int(os.getenv("DATALAKE_RETENTION_DAYS", "7"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    minio_client = MinIODataLakeClient()
    deleted_count = 0

    if minio_client.is_connected():
        print(f"[*] Scanning MinIO for Parquet files older than {retention_days} days (before {cutoff.isoformat()})...")
        try:
            objects = minio_client.client.list_objects(
                minio_client.bucket_name,
                prefix="raw/transactions/",
                recursive=True
            )
            for obj in objects:
                if obj.object_name.endswith(".parquet") and obj.last_modified < cutoff:
                    minio_client.client.remove_object(minio_client.bucket_name, obj.object_name)
                    deleted_count += 1
                    print(f"   [🗑️] Deleted: {obj.object_name} (modified: {obj.last_modified.isoformat()})")
        except Exception as e:
            print(f"[!] Error during MinIO cleanup: {e}")
    else:
        print("[!] MinIO not reachable, skipping MinIO cleanup.")

    # Dọn dẹp lưu trữ local fallback nếu tồn tại
    local_dir = os.path.join(PROJECT_ROOT, "data_lake_storage", "raw", "transactions")
    local_deleted = 0
    if os.path.exists(local_dir):
        print(f"\n[*] Scanning local fallback storage: {local_dir}")
        for root, dirs, files in os.walk(local_dir, topdown=False):
            for f in files:
                fpath = os.path.join(root, f)
                if f.endswith(".parquet"):
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath), tz=timezone.utc)
                    if mtime < cutoff:
                        os.remove(fpath)
                        local_deleted += 1
                        print(f"   [🗑️] Deleted local: {fpath}")
            # Xóa các thư mục rỗng
            if not os.listdir(root) and root != local_dir:
                os.rmdir(root)

    print(f"\n[+] Cleanup complete! Deleted {deleted_count} MinIO files + {local_deleted} local files.")
    return {"minio_deleted": deleted_count, "local_deleted": local_deleted}


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
        dag_id="dag_cleanup_datalake",
        default_args=default_args,
        description="Remove Parquet files older than 7 days from MinIO Data Lake",
        schedule_interval="0 2 * * *",
        catchup=False,
        tags=["xombank", "cleanup", "minio", "maintenance"]
    )

    task_cleanup = PythonOperator(
        task_id="cleanup_old_parquet_files",
        python_callable=execute_cleanup,
        dag=dag
    )
else:
    dag = None
