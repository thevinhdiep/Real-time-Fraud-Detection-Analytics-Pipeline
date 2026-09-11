# 📊 Xóm Bank — Machine Learning Model Evaluation Report

> **Trained At:** 2026-08-21 14:22:25 UTC
> **Selected Production Model:** `LightGBM`
> **Optimal Decision Threshold:** `0.7500`

## 1. Cross-Validation Benchmark Results (Stratified 5-Fold)

| Model | PR-AUC (Mean ± Std) | ROC-AUC | Avg Cost/Fold ($) | Training Time (s) |
|---|---|---|---|---|
| **Logistic Regression** | **0.3621 ± 0.0313** | 0.9609 | $44,302 | 5.1s |
| **Random Forest** | **0.8946 ± 0.0114** | 0.9961 | $12,304 | 11.6s |
| **LightGBM** | **0.9177 ± 0.0115** | 0.9966 | $11,929 | 6.0s |

## 2. Temporal Hold-Out Test Evaluation (555,719 Transactions)

| Metric | Default Threshold (0.50) | Optimal Threshold (0.75) | Business Impact |
|---|---|---|---|
| **PR-AUC** | 0.7572 | 0.7572 | Baseline for Imbalanced Fraud |
| **Recall (Tỷ lệ tóm gian lận)** | 90.16% | **85.45%** | Cân bằng tỷ lệ bắt gian lận |
| **Precision (Độ chuẩn xác)** | 20.66% | **32.80%** | Tăng +12.14% (giảm báo động giả) |
| **F1-Score** | 0.3361 | **0.4741** | Tăng đáng kể chất lượng phân loại |
| **False Negatives (Bỏ sót)** | 211 txns | **312 txns** | Chấp nhận đánh đổi để giảm FP |
| **False Positives (Chặn nhầm)** | 7,428 txns | **3,755 txns** | **Giảm 3,673 cuộc gọi CSKH phiền hà** |
| **Total Business Cost** | $216,920.00 | **$212,325.00** | **Tiết kiệm $4,595.00 (-2.1%)** |

## 3. Top 10 Feature Importances

| Rank | Feature | Importance Score |
|---|---|---|
| 1 | `amt` | 994.0000 |
| 2 | `customer_age` | 555.0000 |
| 3 | `amt_zscore_by_customer` | 510.0000 |
| 4 | `trans_hour` | 458.0000 |
| 5 | `city_pop_log` | 438.0000 |
| 6 | `txn_freq_24h` | 220.0000 |
| 7 | `day_of_week` | 178.0000 |
| 8 | `geo_distance_km` | 159.0000 |
| 9 | `cat_grocery_pos` | 109.0000 |
| 10 | `cat_food_dining` | 107.0000 |
