# -*- coding: utf-8 -*-
"""
Xuất Toàn Bộ 1.85 Triệu Giao Dịch Star Schema cho Power BI Desktop
====================================================================
Ghép fraudTrain.csv (1.296M) + fraudTest.csv (555K) = 1,852,394 giao dịch
Tạo 3 bảng chiều (dimension):
1. dim_customers.csv
2. dim_merchants.csv
3. fct_transactions_fraud_features.csv (Toàn bộ 1.85M dòng)
"""

import os
import sys
import hashlib
import numpy as np
import pandas as pd

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ml.feature_engineering import haversine_distance_km


def md5_hash(val: str) -> str:
    return hashlib.md5(str(val).encode("utf-8")).hexdigest()


def export_full_star_schema(output_dir: str = "powerbi/data"):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print("🏦 XÓM BANK — EXPORTING FULL 1.85M TRANSACTIONS STAR SCHEMA")
    print("=" * 80)

    # 1. Tải cả hai file CSV (Train và Test)
    print("\n[1/4] Reading full datasets (fraudTrain.csv + fraudTest.csv)...")
    train_path = os.path.join(PROJECT_ROOT, "data", "fraudTrain.csv")
    test_path = os.path.join(PROJECT_ROOT, "data", "fraudTest.csv")

    df_train = pd.read_csv(train_path, index_col=0)
    df_test = pd.read_csv(test_path, index_col=0)
    df = pd.concat([df_train, df_test], ignore_index=True)
    total_rows = len(df)
    print(f"[+] Loaded Train: {len(df_train):,} | Test: {len(df_test):,} | Total: {total_rows:,} rows.")

    df["trans_timestamp"] = pd.to_datetime(df["trans_date_trans_time"])
    df["dob_dt"] = pd.to_datetime(df["dob"])
    df["customer_age"] = np.maximum(0, (df["trans_timestamp"] - df["dob_dt"]).dt.days // 365.25).astype(int)

    # 2. Chiều Khách hàng: Khử trùng lặp theo cc_num trên toàn bộ 1.85M bản ghi
    print("\n[2/4] Building dim_customers.csv...")
    df_sorted_cust = df.sort_values("trans_timestamp", ascending=False)
    dim_cust = df_sorted_cust.drop_duplicates(subset=["cc_num"]).copy()
    dim_cust["customer_id"] = dim_cust["cc_num"].apply(lambda cc: md5_hash(str(cc)))
    dim_cust["full_name"] = dim_cust["first"] + " " + dim_cust["last"]

    dim_cust_cols = [
        "customer_id", "cc_num", "first", "last", "full_name", "gender",
        "street", "city", "state", "zip", "lat", "long", "city_pop",
        "job", "dob", "customer_age"
    ]
    dim_cust_final = dim_cust[dim_cust_cols].rename(columns={
        "first": "first_name",
        "last": "last_name",
        "zip": "zip_code",
        "lat": "customer_lat",
        "long": "customer_long",
        "city_pop": "city_population",
        "job": "job_title",
        "dob": "date_of_birth"
    })
    cust_csv_path = os.path.join(output_dir, "dim_customers.csv")
    dim_cust_final.to_csv(cust_csv_path, index=False)
    print(f" [+] Saved dim_customers.csv ({len(dim_cust_final):,} unique customers) -> {cust_csv_path}")

    # 3. Chiều Merchant: Nhóm theo tên merchant và danh mục
    print("\n[3/4] Building dim_merchants.csv...")
    dim_merch = df.groupby(["merchant", "category"]).agg(
        avg_merchant_lat=("merch_lat", "mean"),
        avg_merchant_long=("merch_long", "mean"),
        total_historical_transactions=("trans_num", "count")
    ).reset_index()
    dim_merch["merchant_id"] = dim_merch.apply(lambda r: md5_hash(f"{r['merchant']}_{r['category']}"), axis=1)
    dim_merch_cols = ["merchant_id", "merchant", "category", "avg_merchant_lat", "avg_merchant_long", "total_historical_transactions"]
    dim_merch_final = dim_merch[dim_merch_cols].rename(columns={"merchant": "merchant_name"})
    merch_csv_path = os.path.join(output_dir, "dim_merchants.csv")
    dim_merch_final.to_csv(merch_csv_path, index=False)
    print(f" [+] Saved dim_merchants.csv ({len(dim_merch_final):,} unique merchants) -> {merch_csv_path}")

    # 4. Bảng Fact: Giao dịch với các đặc trưng Fraud (1.85M dòng)
    print("\n[4/4] Building fct_transactions_fraud_features.csv (Full 1.85M rows)...")
    fct = df.copy()
    fct["customer_id"] = fct["cc_num"].apply(lambda cc: md5_hash(str(cc)))
    fct["merchant_id"] = fct.apply(lambda r: md5_hash(f"{r['merchant']}_{r['category']}"), axis=1)

    # Khoảng cách địa lý
    print("  -> Calculating Haversine distances...")
    fct["geo_distance_km"] = haversine_distance_km(
        fct["lat"].values, fct["long"].values, fct["merch_lat"].values, fct["merch_long"].values
    )

    # Đặc trưng thời gian
    print("  -> Extracting temporal features...")
    fct["trans_hour"] = fct["trans_timestamp"].dt.hour
    fct["day_of_week"] = fct["trans_timestamp"].dt.dayofweek + 1  # 1=Thứ Hai, 7=Chủ Nhật
    fct["is_weekend"] = fct["trans_timestamp"].dt.dayofweek.isin([5, 6]).astype(int)
    fct["is_night"] = fct["trans_hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    # Tính tần suất 24h (vectorized searchsorted)
    print("  -> Computing rolling 24h frequency...")
    if "unix_time" not in fct.columns or fct["unix_time"].isnull().any():
        unix_time = fct["trans_timestamp"].astype("int64") // 10**9
    else:
        unix_time = fct["unix_time"].values

    txn_freq_24h = np.zeros(len(fct), dtype=np.int32)
    df_sort_helper = pd.DataFrame({
        "cc_num": fct["cc_num"].values,
        "unix_time": unix_time,
        "orig_idx": np.arange(len(fct))
    }).sort_values(["cc_num", "unix_time"])

    for _, group in df_sort_helper.groupby("cc_num"):
        times = group["unix_time"].values
        orig_indices = group["orig_idx"].values
        left_bounds = np.searchsorted(times, times - 86400, side="left")
        current_bounds = np.arange(len(times))
        txn_freq_24h[orig_indices] = current_bounds - left_bounds

    fct["txn_freq_24h"] = txn_freq_24h

    # Z-score chi tiêu theo khách hàng
    print("  -> Computing spending Z-scores...")
    cust_stats = fct.groupby("cc_num")["amt"].agg(["mean", "std"]).reset_index()
    cust_stats["std"] = cust_stats["std"].fillna(0.0)
    fct_merged = fct.merge(cust_stats, on="cc_num", how="left")
    valid_std_mask = fct_merged["std"] > 1e-5
    amt_zscore = np.zeros(len(fct), dtype=np.float64)
    np.divide(fct_merged["amt"] - fct_merged["mean"], fct_merged["std"], out=amt_zscore, where=valid_std_mask)
    fct["amt_zscore_by_customer"] = np.round(amt_zscore, 4)

    fct_cols = [
        "trans_num", "customer_id", "merchant_id", "trans_timestamp", "unix_time",
        "amt", "category", "is_fraud", "geo_distance_km", "txn_freq_24h",
        "amt_zscore_by_customer", "trans_hour", "day_of_week", "is_weekend", "is_night"
    ]
    fct_final = fct[fct_cols].rename(columns={"amt": "amount"})
    fct_csv_path = os.path.join(output_dir, "fct_transactions_fraud_features.csv")
    fct_final.to_csv(fct_csv_path, index=False)

    fraud_cnt = int(fct_final["is_fraud"].sum())
    fraud_pct = fraud_cnt / len(fct_final) * 100
    print(f"\n[+] Successfully exported fct_transactions_fraud_features.csv:")
    print(f"    - Total Rows:    {len(fct_final):,} transactions (100% Full Dataset)")
    print(f"    - Fraud Events:  {fraud_cnt:,} frauds ({fraud_pct:.3f}%)")
    print(f"    - Saved Path:    {fct_csv_path}")
    print("\n🎉 FULL 1.85M STAR SCHEMA EXPORT COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    export_full_star_schema()
