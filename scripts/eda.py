# -*- coding: utf-8 -*-
"""
Script Phân Tích Khám Phá Dữ Liệu (EDA) cho Bộ Dữ Liệu Xom Bank (Phase 0)
"""

import os
import sys
import pandas as pd
import numpy as np

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Thêm thư mục gốc dự án vào sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ml.feature_engineering import haversine_distance_km as haversine_np


def run_eda():
    train_path = os.path.join("data", "fraudTrain.csv")
    test_path = os.path.join("data", "fraudTest.csv")

    print("=" * 75)
    print("XOM BANK - EXPLORATORY DATA ANALYSIS (PHASE 0)")
    print("=" * 75)

    print("\n[1] Doc du lieu...")
    df_train = pd.read_csv(train_path, index_col=0)
    df_test = pd.read_csv(test_path, index_col=0)

    print(f" - Train shape: {df_train.shape[0]:,} rows x {df_train.shape[1]} cols")
    print(f" - Test shape:  {df_test.shape[0]:,} rows x {df_test.shape[1]} cols")
    print(f" - Tong cong:   {df_train.shape[0] + df_test.shape[0]:,} transactions")

    print("\n[2] Schema va Thong tin Cot:")
    for col in df_train.columns:
        print(f"   * {col:25s} | Type: {str(df_train[col].dtype):10s} | Nulls: {df_train[col].isnull().sum()}")

    print("\n[3] Khoang thoi gian giao dich (Time Window):")
    train_min_t = df_train['trans_date_trans_time'].min()
    train_max_t = df_train['trans_date_trans_time'].max()
    test_min_t = df_test['trans_date_trans_time'].min()
    test_max_t = df_test['trans_date_trans_time'].max()

    print(f"   * Train Period: {train_min_t} ---> {train_max_t}")
    print(f"   * Test Period:  {test_min_t} ---> {test_max_t}")

    print("\n[4] Ty le mat can bang nhan (Fraud Class Imbalance):")
    train_fraud_cnt = df_train['is_fraud'].sum()
    train_total = len(df_train)
    train_fraud_pct = (train_fraud_cnt / train_total) * 100

    test_fraud_cnt = df_test['is_fraud'].sum()
    test_total = len(df_test)
    test_fraud_pct = (test_fraud_cnt / test_total) * 100

    print(f"   * Train Fraud: {train_fraud_cnt:,} / {train_total:,} ({train_fraud_pct:.4f}%)")
    print(f"   * Test Fraud:  {test_fraud_cnt:,} / {test_total:,} ({test_fraud_pct:.4f}%)")

    print("\n[5] Thong ke Thuc the (Entities):")
    print(f"   * Unique Customers (cc_num): {df_train['cc_num'].nunique():,} (Train) | {df_test['cc_num'].nunique():,} (Test)")
    print(f"   * Unique Merchants:          {df_train['merchant'].nunique():,} (Train) | {df_test['merchant'].nunique():,} (Test)")
    print(f"   * Unique Categories:         {df_train['category'].nunique():,} (Train)")
    print(f"   * Unique States:             {df_train['state'].nunique():,} (Train)")
    print(f"   * Unique Jobs:               {df_train['job'].nunique():,} (Train)")

    print("\n[6] Phan tich So tien giao dich (Amount - amt):")
    legit_amt = df_train[df_train['is_fraud'] == 0]['amt']
    fraud_amt = df_train[df_train['is_fraud'] == 1]['amt']

    print(f"   * Legit: Mean=${legit_amt.mean():.2f}, Median=${legit_amt.median():.2f}, 95th=${legit_amt.quantile(0.95):.2f}, Max=${legit_amt.max():.2f}")
    print(f"   * Fraud: Mean=${fraud_amt.mean():.2f}, Median=${fraud_amt.median():.2f}, 95th=${fraud_amt.quantile(0.95):.2f}, Max=${fraud_amt.max():.2f}")

    print("\n[7] Ty le Gian lan theo Danh muc (Category Fraud Rate):")
    cat_summary = df_train.groupby('category').agg(
        total_txn=('is_fraud', 'count'),
        fraud_txn=('is_fraud', 'sum'),
        fraud_rate=('is_fraud', 'mean')
    ).sort_values(by='fraud_rate', ascending=False)

    for cat, row in cat_summary.iterrows():
        print(f"   * {cat:22s}: {row['fraud_txn']:5.0f} frauds / {row['total_txn']:7.0f} txns ({row['fraud_rate']*100:6.2f}%)")

    print("\n[8] Phan tich Khoang cach Dia ly (Haversine Distance km):")
    train_dist = haversine_np(df_train['lat'].values, df_train['long'].values, df_train['merch_lat'].values, df_train['merch_long'].values)
    df_train['distance_km'] = train_dist

    legit_dist = df_train[df_train['is_fraud'] == 0]['distance_km']
    fraud_dist = df_train[df_train['is_fraud'] == 1]['distance_km']

    print(f"   * Legit Dist (km): Mean={legit_dist.mean():.2f} km, Median={legit_dist.median():.2f} km, Max={legit_dist.max():.2f} km")
    print(f"   * Fraud Dist (km): Mean={fraud_dist.mean():.2f} km, Median={fraud_dist.median():.2f} km, Max={fraud_dist.max():.2f} km")

    print("\n[9] Ty le Gian lan theo Bang (State Fraud Rate - Top 15):")
    state_summary = df_train.groupby('state').agg(
        total_txn=('is_fraud', 'count'),
        fraud_txn=('is_fraud', 'sum'),
        fraud_rate=('is_fraud', 'mean'),
        avg_amount=('amt', 'mean')
    ).sort_values(by='fraud_rate', ascending=False)

    for i, (state, row) in enumerate(state_summary.iterrows()):
        if i >= 15:
            break
        print(f"   * {state:4s}: {row['fraud_txn']:5.0f} frauds / {row['total_txn']:7.0f} txns ({row['fraud_rate']*100:6.3f}%) | Avg Amt: ${row['avg_amount']:.2f}")

    print(f"\n   Tong so bang (states): {len(state_summary)}")
    print(f"   State co fraud rate cao nhat: {state_summary.index[0]} ({state_summary.iloc[0]['fraud_rate']*100:.3f}%)")
    print(f"   State co fraud rate thap nhat: {state_summary.index[-1]} ({state_summary.iloc[-1]['fraud_rate']*100:.3f}%)")

    print("\n" + "=" * 75)
    print("EDA HOAN TAT THANH CONG!")
    print("=" * 75)


if __name__ == "__main__":
    run_eda()
