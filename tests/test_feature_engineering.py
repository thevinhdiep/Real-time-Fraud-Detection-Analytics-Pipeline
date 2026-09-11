# -*- coding: utf-8 -*-
"""
Kiểm thử Module Biến Đổi Đặc Trưng ML và Feature Parity (Phase 5)
"""

import pytest
import numpy as np
import pandas as pd
from ml.feature_engineering import haversine_distance_km, FraudFeatureEngineer


class TestFeatureEngineering:

    def test_haversine_distance(self):
        # Khoảng cách New York City đến Los Angeles (~3,940 km)
        lat_nyc, lon_nyc = 40.7128, -74.0060
        lat_la, lon_la = 34.0522, -118.2437
        dist = haversine_distance_km(lat_nyc, lon_nyc, lat_la, lon_la)
        assert 3900 < dist < 4000

        # Khoảng cách bằng 0 khi tọa độ trùng nhau
        dist_zero = haversine_distance_km(10.0, 20.0, 10.0, 20.0)
        assert dist_zero == 0.0

    def test_feature_parity_batch_vs_single(self):
        # Tạo bộ dữ liệu nhỏ tổng hợp
        data = {
            "trans_date_trans_time": ["2019-01-01 00:00:18", "2019-01-01 00:05:00"],
            "cc_num": [123456789, 123456789],
            "merchant": ["fraud_TestMerch", "fraud_TestMerch"],
            "category": ["grocery_pos", "entertainment"],
            "amt": [100.0, 250.0],
            "first": ["John", "John"],
            "last": ["Doe", "Doe"],
            "gender": ["M", "M"],
            "street": ["123 Main St", "123 Main St"],
            "city": ["Dallas", "Dallas"],
            "state": ["TX", "TX"],
            "zip": [75001, 75001],
            "lat": [32.7767, 32.7767],
            "long": [-96.7970, -96.7970],
            "city_pop": [1300000, 1300000],
            "job": ["Engineer", "Engineer"],
            "dob": ["1990-05-15", "1990-05-15"],
            "trans_num": ["t001", "t002"],
            "unix_time": [1546300818, 1546301100],
            "merch_lat": [32.8000, 32.8500],
            "merch_long": [-96.8000, -96.8500],
            "is_fraud": [0, 1]
        }
        df = pd.DataFrame(data)
        fe = FraudFeatureEngineer().fit(df)
        X_batch, y_batch = fe.transform_batch(df)

        assert X_batch.shape == (2, 27)
        assert list(y_batch) == [0, 1]

        # Kiểm thử biến đổi streaming đơn lẻ trên dòng 0
        row0 = df.iloc[0].to_dict()
        x_single0 = fe.transform_single(row0, update_state=False)

        assert x_single0.shape == (1, 27)
        diff = np.abs(X_batch.iloc[0].values - x_single0.iloc[0].values)
        assert np.max(diff) < 1e-5, f"Feature parity failure! Max diff: {np.max(diff)}"
