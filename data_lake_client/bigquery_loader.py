# -*- coding: utf-8 -*-
"""
Bộ Nạp Dữ Liệu Hàng Loạt (Batch Loader) Lên BigQuery cho Xóm Bank (Phase 3)
Nạp các file Parquet từ MinIO Data Lake vào Google BigQuery thông qua Batch Load Jobs.
"""

import os
import io
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from google.cloud import bigquery
from google.oauth2 import service_account

# Thêm thư mục gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_lake_client.minio_client import MinIODataLakeClient


class BigQueryBatchLoader:
    """
    Xử lý nạp dữ liệu hàng loạt từ các file Parquet trong MinIO vào bảng raw_transactions của BigQuery.
    """

    RAW_TRANSACTIONS_SCHEMA = [
        bigquery.SchemaField("trans_num", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trans_date_trans_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("unix_time", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("cc_num", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("merchant", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("amt", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("first", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("street", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("zip", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("long", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("city_pop", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("job", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dob", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merch_lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("merch_long", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("is_fraud", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("produced_at", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    def __init__(
        self,
        key_path: str = os.getenv("GCP_KEY_PATH", "gcp_key.json"),
        project_id: str = "xombank-fraud-analytics",
        dataset_id: str = "xombank_dw",
        table_id: str = "raw_transactions"
    ):
        self.key_path = key_path
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        if os.path.exists(self.key_path):
            self.credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)
        else:
            self.credentials = None
            self.client = None

        self.minio_client = MinIODataLakeClient()

    def ensure_raw_table(self):
        """
        Đảm bảo bảng raw_transactions tồn tại với phân vùng thời gian (time partitioning) theo cột ingested_at.
        """
        if not self.client:
            raise RuntimeError("BigQuery client chua duoc khoi tao do thieu file key.")

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id),
            self.table_id
        )

        try:
            self.client.get_table(table_ref)
            print(f"[+] Bang BigQuery '{self.full_table_id}' da ton tai.")
        except Exception:
            print(f"[*] Dang tao moi bang '{self.full_table_id}' tren BigQuery...")
            table = bigquery.Table(table_ref, schema=self.RAW_TRANSACTIONS_SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingested_at"
            )
            table.description = "Bảng dữ liệu giao dịch thô nạp từ MinIO Data Lake (Hive Parquet)"
            created_table = self.client.create_table(table)
            print(f"[+] Da tao bang '{self.full_table_id}' thanh cong (Partitioned by DAY on ingested_at).")

    def load_parquet_from_minio(
        self,
        prefix: str = "raw/transactions/",
        write_disposition: str = "WRITE_APPEND",
        local_fallback_dir: str = "data_lake_storage"
    ) -> Dict[str, Any]:
        """
        Quét các file Parquet trong MinIO (hoặc lưu trữ cục bộ dự phòng), đọc và nạp vào BigQuery bằng Batch Load Job.
        """
        if not self.client:
            raise RuntimeError("BigQuery client chua duoc khoi tao.")

        self.ensure_raw_table()

        all_dfs = []
        parquet_files = []
        source_desc = ""

        if self.minio_client.is_connected():
            objects = self.minio_client.list_objects(prefix=prefix)
            parquet_files = [obj for obj in objects if obj.endswith(".parquet")]
            source_desc = f"MinIO Data Lake (s3://{self.minio_client.bucket_name}/{prefix})"

            if parquet_files:
                print(f"\n[*] Tim thay {len(parquet_files)} file Parquet trong MinIO. Dang tong hop...")
                for obj_name in parquet_files:
                    try:
                        response = self.minio_client.client.get_object(self.minio_client.bucket_name, obj_name)
                        parquet_bytes = io.BytesIO(response.read())
                        df = pd.read_parquet(parquet_bytes)
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"[!] Loi khi doc file {obj_name}: {e}")
                    finally:
                        response.close()
                        response.release_conn()
        elif os.path.exists(local_fallback_dir):
            source_desc = f"Local Fallback Storage ({local_fallback_dir})"
            local_files = []
            for root, _, files in os.walk(os.path.join(local_fallback_dir, "raw", "transactions")):
                for f in files:
                    if f.endswith(".parquet"):
                        local_files.append(os.path.join(root, f))
            if local_files:
                print(f"\n[*] MinIO offline. Tim thay {len(local_files)} file Parquet trong local storage. Dang tong hop...")
                for fpath in local_files:
                    try:
                        df = pd.read_parquet(fpath)
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"[!] Loi doc file local {fpath}: {e}")
        else:
            raise RuntimeError("Khong tim thay MinIO hoac thu muc local storage de doc Parquet.")

        if not all_dfs:
            print("[*] Khong co file Parquet nao co du lieu de nap.")
            return {"status": "EMPTY", "files_loaded": 0, "rows_loaded": 0}

        combined_df = pd.concat(all_dfs, ignore_index=True)
        # Loại bỏ trùng lặp bản ghi theo mã giao dịch trans_num
        combined_df = combined_df.drop_duplicates(subset=["trans_num"])
        # Gán nhãn thời gian nạp dữ liệu ingested_at
        combined_df["ingested_at"] = pd.Timestamp.now(tz="UTC")

        print(f"[*] Tong so dong chuan bi nap vao BigQuery: {len(combined_df):,} rows.")

        job_config = bigquery.LoadJobConfig(
            schema=self.RAW_TRANSACTIONS_SCHEMA,
            write_disposition=write_disposition
        )

        load_job = self.client.load_table_from_dataframe(
            combined_df,
            self.full_table_id,
            job_config=job_config
        )
        print(f"[*] Batch Load Job ID: {load_job.job_id}. Dang thuc thi tren Cloud...")

        load_job.result()  # Chờ job hoàn tất tải dữ liệu
        table = self.client.get_table(self.full_table_id)

        print(f"[+] Batch Load THANH CONG!")
        print(f" • File da doc tu MinIO:   {len(parquet_files)}")
        print(f" • Dong da nap lan nay:    {len(combined_df):,}")
        print(f" • Tong so dong hien tai:  {table.num_rows:,} rows trong '{self.full_table_id}'")

        return {
            "status": "SUCCESS",
            "files_loaded": len(parquet_files),
            "rows_loaded": len(combined_df),
            "total_table_rows": table.num_rows
        }


def run_batch_load():
    loader = BigQueryBatchLoader()
    print("=" * 70)
    print("🏦 XÓM BANK — BATCH LOAD: MINIO PARQUET -> BIGQUERY")
    print("=" * 70)
    result = loader.load_parquet_from_minio()
    print("=" * 70)


if __name__ == "__main__":
    run_batch_load()
