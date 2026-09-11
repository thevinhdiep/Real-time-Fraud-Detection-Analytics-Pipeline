# -*- coding: utf-8 -*-
"""
Kiểm thử Consumer Group B: Suy Luận Thời Gian Thực (Phase 6)
"""

import os
import pytest
import duckdb
from consumers.consumer_realtime_inference import RealtimeInferenceEngine, RealtimeFraudStore


class TestRealtimeInference:

    @pytest.fixture
    def test_db_path(self, tmp_path):
        return str(tmp_path / "test_fraud.duckdb")

    def test_realtime_store_init_and_insert(self, test_db_path):
        store = RealtimeFraudStore(db_path=test_db_path)
        sample_record = {
            "trans_num": "test_txn_001",
            "trans_date_trans_time": "2026-08-22 12:00:00",
            "unix_time": 1787385600,
            "cc_num": 123456789,
            "merchant": "fraud_TestMerchant",
            "category": "shopping_net",
            "amt": 950.00,
            "city": "Austin",
            "state": "TX",
            "geo_distance_km": 150.5,
            "txn_freq_24h": 3,
            "amt_zscore_by_customer": 4.25,
            "fraud_score": 0.88,
            "is_fraud_predicted": 1,
            "is_fraud_ground_truth": 1,
            "decision": "BLOCK",
            "latency_ms": 1.45,
            "processed_at": "2026-08-22T12:00:01"
        }

        store.insert_scored_transaction(sample_record)

        conn = duckdb.connect(test_db_path)
        row = conn.execute("SELECT trans_num, fraud_score, decision FROM fraud_scores WHERE trans_num = 'test_txn_001'").fetchone()
        conn.close()

        assert row is not None
        assert row[0] == "test_txn_001"
        assert row[1] == 0.88
        assert row[2] == "BLOCK"

    def test_inference_engine_process_transaction(self, test_db_path):
        engine = RealtimeInferenceEngine(db_path=test_db_path)

        txn = {
            "trans_num": "test_txn_002",
            "trans_date_trans_time": "2020-08-21 20:30:00",
            "unix_time": 1598041800,
            "cc_num": 123456789,
            "merchant": "fraud_StoreXYZ",
            "category": "grocery_pos",
            "amt": 45.00,
            "first": "Bob",
            "last": "Jones",
            "gender": "M",
            "street": "100 Broadway",
            "city": "New York",
            "state": "NY",
            "zip": 10001,
            "lat": 40.7128,
            "long": -74.0060,
            "city_pop": 8000000,
            "job": "Accountant",
            "dob": "1980-01-01",
            "merch_lat": 40.7200,
            "merch_long": -74.0100,
            "is_fraud": 0
        }

        res = engine.process_transaction(txn)

        assert res["trans_num"] == "test_txn_002"
        assert 0.0 <= res["fraud_score"] <= 1.0
        assert res["decision"] in ["APPROVE", "ALERT", "BLOCK"]
        assert res["latency_ms"] > 0
        assert res["latency_ms"] < 50.0  # Độ trễ nằm trong ngưỡng cho phép (< 50ms)
