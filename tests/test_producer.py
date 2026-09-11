# -*- coding: utf-8 -*-
"""
Kiểm thử Python Producer (Phase 1)
"""

import os
import sys
import unittest
import pandas as pd
from datetime import datetime

# Đảm bảo thư mục gốc dự án trong sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from producer.produce_transactions import serialize_transaction, stream_csv_records


class TestProducer(unittest.TestCase):

    def setUp(self):
        self.sample_row = pd.Series({
            "trans_num": "28913840293a",
            "trans_date_trans_time": "2019-01-01 00:00:18",
            "unix_time": 1546299618,
            "cc_num": 27031940,
            "merchant": "fraud_Rippin, Kub and Mertz",
            "category": "misc_net",
            "amt": 4.97,
            "first": "Jennifer",
            "last": "Banks",
            "gender": "F",
            "street": "561 Perry Cove",
            "city": "Moravian Falls",
            "state": "NC",
            "zip": 28654,
            "lat": 36.0788,
            "long": -81.1781,
            "city_pop": 3495,
            "job": "Psychologist, counselling",
            "dob": "1988-03-09",
            "merch_lat": 36.011293,
            "merch_long": -82.048315,
            "is_fraud": 0
        })

    def test_serialize_transaction(self):
        payload = serialize_transaction(self.sample_row)

        self.assertEqual(payload["trans_num"], "28913840293a")
        self.assertEqual(payload["cc_num"], 27031940)
        self.assertEqual(payload["amt"], 4.97)
        self.assertEqual(payload["is_fraud"], 0)
        self.assertEqual(payload["category"], "misc_net")
        self.assertIn("produced_at", payload)
        # Xác minh định dạng ISO 8601 hợp lệ với timezone
        parsed_dt = datetime.fromisoformat(payload["produced_at"])
        self.assertIsNotNone(parsed_dt.tzinfo)

    def test_stream_csv_records_limit(self):
        csv_path = os.path.join("data", "fraudTrain.csv")
        if os.path.exists(csv_path):
            records = list(stream_csv_records(csv_path, chunk_size=10, limit=5))
            self.assertEqual(len(records), 5)
            self.assertIn("trans_num", records[0])
            self.assertIn("cc_num", records[0])
            self.assertIn("amt", records[0])


if __name__ == "__main__":
    unittest.main()
