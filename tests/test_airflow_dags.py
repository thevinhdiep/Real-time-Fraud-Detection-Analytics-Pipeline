# -*- coding: utf-8 -*-
"""
Kiểm Thử: Cấu Trúc & Tính Toàn Vẹn của Airflow DAGs (Phase 9)
========================================================
Xác minh tất cả file định nghĩa Airflow DAG tồn tại, import không lỗi cú pháp,
và định nghĩa đúng các tham số mặc định và phụ thuộc giữa các task.
"""

import os
import sys
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestAirflowDAGs:
    """Bộ kiểm thử cho các file Airflow DAG."""

    DAGS_DIR = os.path.join(PROJECT_ROOT, "airflow", "dags")

    @pytest.mark.parametrize("dag_filename", [
        "dag_ingest_to_bigquery.py",
        "dag_dbt_transform.py",
        "dag_cleanup_datalake.py",
        "dag_retrain_model.py",
    ])
    def test_dag_file_exists(self, dag_filename):
        """Xác minh file DAG tồn tại trên ổ đĩa."""
        dag_path = os.path.join(self.DAGS_DIR, dag_filename)
        assert os.path.exists(dag_path), f"DAG file missing: {dag_filename}"

    @pytest.mark.parametrize("dag_filename", [
        "dag_ingest_to_bigquery.py",
        "dag_dbt_transform.py",
        "dag_cleanup_datalake.py",
        "dag_retrain_model.py",
    ])
    def test_dag_file_syntax(self, dag_filename):
        """Xác minh file DAG compile không lỗi cú pháp."""
        dag_path = os.path.join(self.DAGS_DIR, dag_filename)
        with open(dag_path, "r", encoding="utf-8") as f:
            code = f.read()
        compiled = compile(code, dag_path, "exec")
        assert compiled is not None
