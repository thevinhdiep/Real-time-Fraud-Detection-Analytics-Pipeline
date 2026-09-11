# -*- coding: utf-8 -*-
"""
Kiểm thử BigQuery Batch Loader (Phase 3)
"""

import os
import sys
import unittest

# Đảm bảo thư mục gốc dự án trong sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_lake_client.bigquery_loader import BigQueryBatchLoader


class TestBigQueryBatchLoader(unittest.TestCase):

    def setUp(self):
        self.loader = BigQueryBatchLoader(
            key_path="gcp_key.json",
            project_id="xombank-fraud-analytics",
            dataset_id="xombank_dw",
            table_id="raw_transactions"
        )

    def test_schema_definition(self):
        schema_names = [f.name for f in self.loader.RAW_TRANSACTIONS_SCHEMA]
        expected_cols = [
            "trans_num", "trans_date_trans_time", "unix_time", "cc_num",
            "merchant", "category", "amt", "first", "last", "gender",
            "street", "city", "state", "zip", "lat", "long", "city_pop",
            "job", "dob", "merch_lat", "merch_long", "is_fraud",
            "produced_at", "ingested_at"
        ]

        for col in expected_cols:
            self.assertIn(col, schema_names)

    def test_full_table_id(self):
        self.assertEqual(
            self.loader.full_table_id,
            "xombank-fraud-analytics.xombank_dw.raw_transactions"
        )


if __name__ == "__main__":
    unittest.main()
