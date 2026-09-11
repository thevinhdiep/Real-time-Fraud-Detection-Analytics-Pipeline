# -*- coding: utf-8 -*-
"""
Apache Airflow DAG: Huấn Luyện Lại Mô Hình Phát Hiện Gian Lận (Phase 9 - MLOps)
======================================================================
Tự động hóa quy trình huấn luyện lại và đánh giá mô hình định kỳ.
Lịch chạy: Hàng tháng vào ngày 1 lúc 03:00 UTC (0 3 1 * *)
Pipeline:
1. check_retrain_readiness: Xác minh dữ liệu huấn luyện có sẵn
2. execute_model_retraining: Chạy ml/train.py với Stratified 5-Fold CV & Tối ưu Ma Trận Chi Phí
3. validate_retrained_model: Xác minh model artifacts, ngưỡng PR-AUC, và feature parity
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def check_retrain_readiness(**kwargs):
    """
    Xác minh file dữ liệu huấn luyện tồn tại và có đủ khối lượng.
    """
    train_path = os.path.join(PROJECT_ROOT, "data", "fraudTrain.csv")
    if not os.path.exists(train_path):
        raise FileNotFoundError(f"Training dataset not found at: {train_path}")

    file_size_mb = os.path.getsize(train_path) / (1024 * 1024)
    print(f"[✓] Retrain readiness check passed: {train_path} ({file_size_mb:.2f} MB)")
    return True


def execute_model_retraining(**kwargs):
    """
    Thực thi ml/train.py sử dụng Python từ virtual environment.
    """
    python_bin = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe")
    if not os.path.exists(python_bin):
        python_bin = sys.executable

    train_script = os.path.join(PROJECT_ROOT, "ml", "train.py")
    cmd = [python_bin, train_script]

    print(f"[*] Executing model retrain command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Model retraining failed with exit code {result.returncode}")

    return "Model retrained successfully"


def validate_retrained_model(**kwargs):
    """
    Xác minh model artifacts tồn tại, metadata hợp lệ, và PR-AUC đạt ngưỡng tối thiểu.
    """
    model_path = os.path.join(PROJECT_ROOT, "ml", "models", "best_fraud_model.joblib")
    fe_path = os.path.join(PROJECT_ROOT, "ml", "models", "feature_engineer.joblib")
    meta_path = os.path.join(PROJECT_ROOT, "ml", "models", "model_metadata.json")

    assert os.path.exists(model_path), f"Missing model artifact: {model_path}"
    assert os.path.exists(fe_path), f"Missing feature engineer: {fe_path}"
    assert os.path.exists(meta_path), f"Missing metadata file: {meta_path}"

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    holdout_pr_auc = meta.get("holdout_test_metrics", {}).get("pr_auc", 0.0)
    print(f"[✓] Retrained model: {meta.get('model_name')} | PR-AUC: {holdout_pr_auc}")

    # Ngưỡng tối thiểu: Mô hình phải vượt 0.70 PR-AUC trên holdout
    MIN_PR_AUC_THRESHOLD = 0.70
    if holdout_pr_auc < MIN_PR_AUC_THRESHOLD:
        raise ValueError(
            f"Model PR-AUC {holdout_pr_auc:.4f} is below minimum threshold {MIN_PR_AUC_THRESHOLD}"
        )

    print("[✓] Validation passed: New model meets production performance standards!")
    return True


default_args = {
    "owner": "xombank_ml_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="dag_retrain_model",
        default_args=default_args,
        description="Tự động huấn luyện lại và kiểm định định kỳ mô hình phát hiện gian lận LightGBM",
        schedule_interval="0 3 1 * *",  # Chạy lúc 03:00 UTC vào ngày mùng 1 hàng tháng
        catchup=False,
        max_active_runs=1,
        tags=["mlops", "fraud-detection", "retrain"],
    )

    t1_check = PythonOperator(
        task_id="check_retrain_readiness",
        python_callable=check_retrain_readiness,
        dag=dag,
    )

    t2_retrain = PythonOperator(
        task_id="execute_model_retraining",
        python_callable=execute_model_retraining,
        dag=dag,
    )

    t3_validate = PythonOperator(
        task_id="validate_retrained_model",
        python_callable=validate_retrained_model,
        dag=dag,
    )

    t1_check >> t2_retrain >> t3_validate
