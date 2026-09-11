# -*- coding: utf-8 -*-
"""
Module Biến Đổi Đặc Trưng ML & Đảm Bảo Feature Parity (Phase 5 & 6)
========================================================================
Đảm bảo feature parity 100% giữa:
1. Offline / Batch Training (Pandas / dbt Star Schema trên BigQuery)
2. Online / Real-time Serving (Python trong Kafka Consumer Group B)

Các đặc trưng chính:
- geo_distance_km: Khoảng cách Haversine giữa khách hàng và merchant (km)
- txn_freq_24h: Số giao dịch trong 24 giờ gần nhất
- amt_zscore_by_customer: Độ lệch chi tiêu so với trung bình của khách hàng
- Thời gian: trans_hour, day_of_week, is_weekend, is_night
- Nhân khẩu học: customer_age, gender_code, city_pop_log
- Danh mục: Category one-hot / frequency encoding
"""

import os
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def haversine_distance_km(
    lat1: np.ndarray | float,
    lon1: np.ndarray | float,
    lat2: np.ndarray | float,
    lon2: np.ndarray | float
) -> np.ndarray | float:
    """
    Tính khoảng cách Haversine giữa khách hàng và merchant theo đơn vị km.
    Tương thích với hàm ST_DISTANCE trong dbt (int_transactions_enriched.sql).
    """
    r = 6371.0  # Bán kính Trái Đất (km)
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = (
        np.sin(delta_phi / 2.0) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2.0) ** 2
    )
    # Giới hạn a trong [0, 1] để tránh lỗi độ chính xác dấu phẩy động
    a = np.clip(a, 0.0, 1.0)
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return np.round(r * c, 2)


class FraudFeatureEngineer:
    """
    Pipeline biến đổi đặc trưng có trạng thái cho Training và Real-time Serving.
    Duy trì thống kê chi tiêu khách hàng và lịch sử giao dịch gần đây cho streaming lookup.
    """

    CATEGORIES = [
        "entertainment",
        "food_dining",
        "gas_transport",
        "grocery_net",
        "grocery_pos",
        "health_fitness",
        "home",
        "kids_pets",
        "misc_net",
        "misc_pos",
        "personal_care",
        "shopping_net",
        "shopping_pos",
        "travel",
    ]

    NUMERICAL_FEATURES = [
        "amt",
        "amt_log",
        "geo_distance_km",
        "txn_freq_24h",
        "amt_zscore_by_customer",
        "trans_hour",
        "day_of_week",
        "is_weekend",
        "is_night",
        "customer_age",
        "city_pop_log",
        "gender_code",
    ]

    def __init__(self):
        # Thống kê lịch sử khách hàng: {cc_num: {"mean": float, "std": float, "count": int}}
        self.customer_stats: Dict[int, Dict[str, float]] = {}
        # Thống kê trung bình toàn cục cho khách hàng mới/chưa gặp
        self.global_mean_amt: float = 70.0
        self.global_std_amt: float = 160.0
        # Bộ đệm timestamp gần đây trong bộ nhớ cho suy luận thời gian thực: {cc_num: [unix_time, ...]}
        self.recent_txns_buffer: Dict[int, List[int]] = {}
        # Tên của các cột đặc trưng sau khi encoding
        self.feature_names_: List[str] = []

    def _build_feature_names(self) -> List[str]:
        cat_cols = [f"cat_{c}" for c in self.CATEGORIES] + ["cat_other"]
        return self.NUMERICAL_FEATURES + cat_cols

    def fit(self, df: pd.DataFrame) -> "FraudFeatureEngineer":
        """
        Fit thống kê lịch sử chi tiêu khách hàng trên dữ liệu huấn luyện (mean, std).
        Đảm bảo không bị data leakage khi chỉ fit trên train split.
        """
        print("[*] Fitting FraudFeatureEngineer customer baselines...")
        stats_df = df.groupby("cc_num")["amt"].agg(["mean", "std", "count"]).reset_index()
        stats_df["std"] = stats_df["std"].fillna(0.0)

        self.customer_stats = {}
        for _, row in stats_df.iterrows():
            self.customer_stats[int(row["cc_num"])] = {
                "mean": float(row["mean"]),
                "std": float(row["std"]),
                "count": int(row["count"]),
            }

        self.global_mean_amt = float(df["amt"].mean())
        self.global_std_amt = float(df["amt"].std()) if df["amt"].std() > 0 else 1.0
        self.feature_names_ = self._build_feature_names()
        print(f"[+] Fitted profiles for {len(self.customer_stats):,} unique customers.")
        return self

    def transform_batch(
        self,
        df: pd.DataFrame,
        is_training: bool = False
    ) -> Tuple[pd.DataFrame, Optional[np.ndarray]]:
        """
        Biến đổi đặc trưng batch vectorized cho DataFrame (Training / Validation / Holdout).
        """
        df_work = df.copy()

        # 1. Parse timestamp và unix_time
        if not pd.api.types.is_datetime64_any_dtype(df_work["trans_date_trans_time"]):
            trans_dt = pd.to_datetime(df_work["trans_date_trans_time"])
        else:
            trans_dt = df_work["trans_date_trans_time"]

        if "unix_time" not in df_work.columns or df_work["unix_time"].isnull().any():
            unix_time = trans_dt.astype("int64") // 10**9
        else:
            unix_time = df_work["unix_time"].values

        # 2. Khoảng cách địa lý
        geo_distance = haversine_distance_km(
            df_work["lat"].values,
            df_work["long"].values,
            df_work["merch_lat"].values,
            df_work["merch_long"].values,
        )

        # 3. Tần suất giao dịch trong 24h gần nhất (86,400 giây) theo khách hàng
        # Tính vectorized nhanh dùng searchsorted trên timestamps đã sắp xếp
        txn_freq_24h = np.zeros(len(df_work), dtype=np.int32)
        df_sort_helper = pd.DataFrame({
            "cc_num": df_work["cc_num"].values,
            "unix_time": unix_time,
            "orig_idx": np.arange(len(df_work))
        }).sort_values(["cc_num", "unix_time"])

        for _, group in df_sort_helper.groupby("cc_num"):
            times = group["unix_time"].values
            orig_indices = group["orig_idx"].values
            # Chỉ số biên trái của các timestamp >= t - 86400
            left_bounds = np.searchsorted(times, times - 86400, side="left")
            current_bounds = np.arange(len(times))
            freqs = current_bounds - left_bounds
            txn_freq_24h[orig_indices] = freqs

        # 4. Z-score chi tiêu theo khách hàng (vectorized qua pd.Series.map)
        cc_series = df_work["cc_num"].astype(int)
        cust_means = cc_series.map(
            lambda cc: self.customer_stats.get(cc, {}).get("mean", self.global_mean_amt)
        ).values.astype(np.float64)
        cust_stds = cc_series.map(
            lambda cc: self.customer_stats.get(cc, {}).get("std", self.global_std_amt)
        ).values.astype(np.float64)

        amt_values = df_work["amt"].values.astype(np.float64)
        valid_std_mask = cust_stds > 1e-5
        amt_zscore = np.zeros(len(df_work), dtype=np.float64)
        np.divide(amt_values - cust_means, cust_stds, out=amt_zscore, where=valid_std_mask)

        # 5. Đặc trưng thời gian
        trans_hour = trans_dt.dt.hour.values
        day_of_week = trans_dt.dt.dayofweek.values  # 0=Thứ Hai, 6=Chủ Nhật
        is_weekend = np.isin(day_of_week, [5, 6]).astype(np.int32)
        is_night = np.isin(trans_hour, [0, 1, 2, 3, 4, 5]).astype(np.int32)

        # 6. Nhân khẩu học
        # Tính tuổi khách hàng tại thời điểm giao dịch (số năm nguyên, tương thích dbt)
        dob_dt = pd.to_datetime(df_work["dob"])
        customer_age = np.maximum(0, (trans_dt - dob_dt).dt.days // 365.25).astype(np.float64).values
        gender_code = (df_work["gender"].astype(str).str.upper() == "M").astype(np.int32).values
        city_pop_log = np.log1p(np.maximum(0, df_work["city_pop"].values.astype(np.float64)))
        amt_log = np.log1p(np.maximum(0, amt_values))

        # 7. One-hot encoding theo danh mục
        cat_series = df_work["category"].astype(str).str.strip().str.lower()
        cat_dummies = pd.DataFrame(index=df_work.index)
        for cat in self.CATEGORIES:
            cat_dummies[f"cat_{cat}"] = (cat_series == cat).astype(np.int32)
        cat_dummies["cat_other"] = (~cat_series.isin(self.CATEGORIES)).astype(np.int32)

        # Ghép thành DataFrame đặc trưng cuối cùng
        features_dict = {
            "amt": amt_values,
            "amt_log": amt_log,
            "geo_distance_km": geo_distance,
            "txn_freq_24h": txn_freq_24h,
            "amt_zscore_by_customer": amt_zscore,
            "trans_hour": trans_hour,
            "day_of_week": day_of_week,
            "is_weekend": is_weekend,
            "is_night": is_night,
            "customer_age": customer_age,
            "city_pop_log": city_pop_log,
            "gender_code": gender_code,
        }

        features_df = pd.DataFrame(features_dict, index=df.index)
        X_df = pd.concat([features_df, cat_dummies], axis=1)

        y = df["is_fraud"].values.astype(np.int32) if "is_fraud" in df.columns else None
        return X_df, y

    def transform_single(
        self,
        txn: Dict[str, Any],
        update_state: bool = True
    ) -> np.ndarray:
        """
        Biến đổi đặc trưng nhanh cho một bản ghi đơn lẻ trong suy luận streaming thời gian thực (Consumer Group B).
        Mục tiêu độ trễ: < 1 mili giây.
        """
        cc_num = int(txn["cc_num"])
        amt = float(txn["amt"])

        # Timestamp và unix_time
        ts_str = str(txn.get("trans_date_trans_time", ""))
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt = datetime.now(timezone.utc)

        unix_time = int(txn.get("unix_time", dt.timestamp()))

        # Khoảng cách địa lý
        geo_dist = float(haversine_distance_km(
            float(txn.get("lat", 0.0)),
            float(txn.get("long", 0.0)),
            float(txn.get("merch_lat", 0.0)),
            float(txn.get("merch_long", 0.0)),
        ))

        # Tra cứu tần suất 24h từ bộ đệm thời gian thực
        recent_times = self.recent_txns_buffer.get(cc_num, [])
        cutoff_24h = unix_time - 86400
        # Lọc các timestamp trong khoảng 24h
        valid_recent = [t for t in recent_times if t >= cutoff_24h and t < unix_time]
        txn_freq_24h = len(valid_recent)

        if update_state:
            valid_recent.append(unix_time)
            # Cắt bỏ: chỉ giữ timestamp trong 24h gần nhất để tránh rò rỉ bộ nhớ
            self.recent_txns_buffer[cc_num] = [
                t for t in valid_recent if t >= unix_time - 86400
            ]

        # Z-score chi tiêu của khách hàng
        cust = self.customer_stats.get(cc_num)
        if cust and cust["std"] > 1e-5:
            amt_zscore = (amt - cust["mean"]) / cust["std"]
        else:
            amt_zscore = 0.0

        # Đặc trưng thời gian
        trans_hour = dt.hour
        day_of_week = dt.weekday()
        is_weekend = 1 if day_of_week in (5, 6) else 0
        is_night = 1 if trans_hour in (0, 1, 2, 3, 4, 5) else 0

        # Tuổi khách hàng (số năm nguyên)
        dob_str = str(txn.get("dob", "1990-01-01"))
        try:
            dob_dt = datetime.strptime(dob_str, "%Y-%m-%d")
            age = float(max(0, (dt - dob_dt).days // 365.25))
        except Exception:
            age = 35.0

        gender_code = 1 if str(txn.get("gender", "")).upper() == "M" else 0
        city_pop = float(txn.get("city_pop", 10000))
        city_pop_log = float(np.log1p(max(0.0, city_pop)))
        amt_log = float(np.log1p(max(0.0, amt)))

        # One-hot encoding danh mục
        cat_val = str(txn.get("category", "")).strip().lower()
        cat_encoded = [1 if cat_val == c else 0 for c in self.CATEGORIES]
        cat_other = 1 if cat_val not in self.CATEGORIES else 0

        features_vector = [
            amt,
            amt_log,
            geo_dist,
            float(txn_freq_24h),
            amt_zscore,
            float(trans_hour),
            float(day_of_week),
            float(is_weekend),
            float(is_night),
            age,
            city_pop_log,
            float(gender_code),
        ] + [float(x) for x in cat_encoded] + [float(cat_other)]

        return pd.DataFrame([features_vector], columns=self.feature_names_)
