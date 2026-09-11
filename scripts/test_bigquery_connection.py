# -*- coding: utf-8 -*-
"""
Kiểm Tra Kết Nối Google BigQuery & Khởi Tạo Dataset cho Xóm Bank
"""

import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

KEY_PATH = "gcp_key.json"
DATASET_ID = "xombank_dw"


def test_connection():
    print("=" * 70)
    print("🏦 XÓM BANK — GOOGLE BIGQUERY CONNECTION CHECK")
    print("=" * 70)

    if not os.path.exists(KEY_PATH):
        print(f"[x] Khong tim thay file key tai: {KEY_PATH}")
        sys.exit(1)

    print(f" • File Key: {KEY_PATH} (Ton tai)")

    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        project_id = credentials.project_id
        print(f" • Project ID: {project_id}")
        print(f" • Service Account: {credentials.service_account_email}")

        client = bigquery.Client(credentials=credentials, project=project_id)
        print("\n[*] Dang ket noi va kiem tra quyen tren BigQuery...")

        # Truy vấn kiểm tra thử
        query_job = client.query("SELECT 1 as test_val")
        results = list(query_job.result())
        print(" [+] Test Query: THANH CONG! (Ket qua: 1)")

        # Đảm bảo dataset đã tồn tại
        dataset_ref = bigquery.DatasetReference(project_id, DATASET_ID)
        try:
            dataset = client.get_dataset(dataset_ref)
            print(f" [+] Dataset '{DATASET_ID}' da ton tai tai location '{dataset.location}'.")
        except Exception:
            print(f" [*] Dataset '{DATASET_ID}' chua ton tai. Dang tao moi tai location 'US'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset.description = "Xom Bank Fraud Detection & Credit Analytics Data Warehouse"
            created_dataset = client.create_dataset(dataset, timeout=30)
            print(f" [+] Da tao moi Dataset '{DATASET_ID}' thanh cong!")

        print("\n" + "=" * 70)
        print("🎉 XAC THUC BIGQUERY HOAN TOAN THANH CONG!")
        print("=" * 70)

    except Exception as e:
        print(f"\n[x] Loi xac thuc BigQuery: {e}")
        sys.exit(1)


if __name__ == "__main__":
    test_connection()
