# -*- coding: utf-8 -*-
"""
Kiểm thử Model Artifacts và Pipeline Suy Luận (Phase 5)
"""

import os
import json
import pytest
import numpy as np
import joblib


class TestModelPipeline:

    def test_artifacts_exist(self):
        assert os.path.exists("ml/models/best_fraud_model.joblib"), "Model artifact missing"
        assert os.path.exists("ml/models/feature_engineer.joblib"), "Feature engineer artifact missing"
        assert os.path.exists("ml/models/model_metadata.json"), "Metadata missing"

    def test_model_inference_pipeline(self):
        model = joblib.load("ml/models/best_fraud_model.joblib")
        fe = joblib.load("ml/models/feature_engineer.joblib")

        with open("ml/models/model_metadata.json", "r", encoding="utf-8") as f:
            metadata = json.load(f)

        assert 0.0 < metadata["optimal_threshold"] < 1.0
        assert metadata["holdout_test_metrics"]["pr_auc"] > 0.50

        # Kiểm thử suy luận một giao dịch đơn lẻ
        sample_txn = {
            "trans_date_trans_time": "2020-08-21 23:45:00",
            "unix_time": 1598053500,
            "cc_num": 123456789,
            "merchant": "fraud_HighRiskStore",
            "category": "shopping_net",
            "amt": 850.00,
            "first": "Alice",
            "last": "Smith",
            "gender": "F",
            "street": "456 Elm St",
            "city": "Austin",
            "state": "TX",
            "zip": 78701,
            "lat": 30.2672,
            "long": -97.7431,
            "city_pop": 950000,
            "job": "Doctor",
            "dob": "1985-04-12",
            "merch_lat": 38.8951,
            "merch_long": -77.0364,
            "is_fraud": 0
        }

        x_vec = fe.transform_single(sample_txn, update_state=True)
        prob = float(model.predict_proba(x_vec)[0, 1])
        is_fraud_decision = int(prob >= metadata["optimal_threshold"])

        assert 0.0 <= prob <= 1.0
        assert is_fraud_decision in [0, 1]
        assert x_vec.shape == (1, 27)
