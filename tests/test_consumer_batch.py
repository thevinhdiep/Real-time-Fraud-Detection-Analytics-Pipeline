# -*- coding: utf-8 -*-
"""
Kiểm thử Consumer Group A (Nạp Batch & Phân Vùng Parquet)
"""

import os
import sys
import shutil
import unittest
import pandas as pd
import pyarrow.parquet as pq

# Đảm bảo thư mục gốc dự án trong sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from consumers.consumer_batch_analytics import extract_partition_keys, flush_partitioned_records


class TestConsumerBatch(unittest.TestCase):

    def setUp(self):
        self.test_storage_dir = "test_data_lake_storage"
        os.makedirs(self.test_storage_dir, exist_ok=True)

        self.sample_records = [
            {
                "trans_num": "tx001",
                "trans_date_trans_time": "2019-01-01 02:15:30",
                "unix_time": 1546308930,
                "cc_num": 12345678,
                "merchant": "fraud_MerchantA",
                "category": "grocery_pos",
                "amt": 45.50,
                "is_fraud": 0
            },
            {
                "trans_num": "tx002",
                "trans_date_trans_time": "2019-01-01 02:45:10",
                "unix_time": 1546310710,
                "cc_num": 87654321,
                "merchant": "fraud_MerchantB",
                "category": "shopping_net",
                "amt": 500.00,
                "is_fraud": 1
            },
            {
                "trans_num": "tx003",
                "trans_date_trans_time": "2019-01-01 05:10:00",
                "unix_time": 1546319400,
                "cc_num": 11223344,
                "merchant": "fraud_MerchantC",
                "category": "misc_pos",
                "amt": 12.30,
                "is_fraud": 0
            }
        ]

    def tearDown(self):
        if os.path.exists(self.test_storage_dir):
            shutil.rmtree(self.test_storage_dir, ignore_errors=True)

    def test_extract_partition_keys(self):
        dt, hour = extract_partition_keys({"trans_date_trans_time": "2020-05-18 14:30:25"})
        self.assertEqual(dt, "2020-05-18")
        self.assertEqual(hour, "14")

    def test_flush_partitioned_records_local(self):
        files_written = flush_partitioned_records(
            self.sample_records,
            minio_client=None,
            local_storage_dir=self.test_storage_dir
        )

        # 3 bản ghi chia thành 2 partition: (2019-01-01, hour=02) với 2 bản ghi, (2019-01-01, hour=05) với 1 bản ghi
        self.assertEqual(files_written, 2)

        part_dir_02 = os.path.join(self.test_storage_dir, "raw", "transactions", "dt=2019-01-01", "hour=02")
        part_dir_05 = os.path.join(self.test_storage_dir, "raw", "transactions", "dt=2019-01-01", "hour=05")

        self.assertTrue(os.path.exists(part_dir_02))
        self.assertTrue(os.path.exists(part_dir_05))

        # Kiểm tra nội dung Parquet trong hour=02
        parquet_files_02 = os.listdir(part_dir_02)
        self.assertEqual(len(parquet_files_02), 1)

        parquet_path = os.path.join(part_dir_02, parquet_files_02[0])
        df_read = pd.read_parquet(parquet_path)
        self.assertEqual(len(df_read), 2)
        self.assertListEqual(list(df_read["trans_num"]), ["tx001", "tx002"])


if __name__ == "__main__":
    unittest.main()
