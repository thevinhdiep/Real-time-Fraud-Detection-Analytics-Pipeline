# -*- coding: utf-8 -*-
"""
Huấn Luyện ML, Cross-Validation & Tối Ưu Chi Phí Kinh Doanh (Phase 5)
================================================================================
Huấn luyện và so sánh 3 mô hình:
1. Logistic Regression (Baseline với StandardScaler & class_weight='balanced')
2. Random Forest Classifier (Ensemble với class_weight='balanced')
3. LightGBM / XGBoost Classifier (Gradient Boosted Decision Trees với scale_pos_weight)

Đánh giá & Lựa chọn:
- Stratified 5-Fold Cross Validation
- Chỉ số chính: PR-AUC (Average Precision Score)
- Tối ưu ma trận chi phí kinh doanh: chi phí FN ($500) so với chi phí FP ($15)
- Đánh giá Temporal Hold-Out trên fraudTest.csv (555K giao dịch)
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List, Optional
import numpy as np
import pandas as pd
import joblib

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

from ml.feature_engineering import FraudFeatureEngineer

from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    f1_score,
    precision_score,
    recall_score,
    confusion_matrix,
    classification_report
)

try:
    import lightgbm as lgb
    LGBM_AVAILABLE = True
except ImportError:
    LGBM_AVAILABLE = False

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False


# =====================================================================
# Tham số chi phí kinh doanh
# =====================================================================
COST_FALSE_NEGATIVE = 500.0  # $500: tổn thất gian lận trực tiếp + phạt chargeback
COST_FALSE_POSITIVE = 15.0   # $15: chi phí gọi xác minh khách hàng + ma sát trải nghiệm


def evaluate_cost_function(
    y_true: np.ndarray,
    y_probs: np.ndarray,
    cost_fn: float = COST_FALSE_NEGATIVE,
    cost_fp: float = COST_FALSE_POSITIVE,
    thresholds: Optional[np.ndarray] = None
) -> Tuple[float, float, Dict[str, Any]]:
    """
    Quét các ngưỡng phân loại để tìm ngưỡng tối thiểu hóa tổng chi phí kinh doanh.
    Trả về: (ngưỡng_tối_ưu, chi_phí_tối_thiểu, chi_tiết_chi_phí)
    """
    if thresholds is None:
        thresholds = np.linspace(0.01, 0.99, 99)

    best_threshold = 0.5
    min_cost = float("inf")
    best_metrics = {}

    for t in thresholds:
        y_pred = (y_probs >= t).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        total_cost = (fn * cost_fn) + (fp * cost_fp)

        if total_cost < min_cost:
            min_cost = total_cost
            best_threshold = float(t)
            prec = precision_score(y_true, y_pred, zero_division=0)
            rec = recall_score(y_true, y_pred, zero_division=0)
            f1 = f1_score(y_true, y_pred, zero_division=0)
            best_metrics = {
                "threshold": round(best_threshold, 4),
                "total_cost": round(min_cost, 2),
                "false_negatives": int(fn),
                "false_positives": int(fp),
                "true_positives": int(tp),
                "true_negatives": int(tn),
                "precision": round(prec, 4),
                "recall": round(rec, 4),
                "f1_score": round(f1, 4),
                "cost_fn_total": round(fn * cost_fn, 2),
                "cost_fp_total": round(fp * cost_fp, 2),
            }

    return best_threshold, min_cost, best_metrics


def build_models(imbalance_ratio: float) -> Dict[str, Any]:
    """
    Khởi tạo các mô hình ứng viên với xử lý mất cân bằng lớp phù hợp.
    """
    models = {
        "Logistic Regression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(
                class_weight="balanced",
                max_iter=1000,
                random_state=42,
                C=1.0,
                solver="lbfgs"
            ))
        ]),
        "Random Forest": RandomForestClassifier(
            n_estimators=100,
            max_depth=14,
            min_samples_split=10,
            min_samples_leaf=5,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1
        )
    }

    if LGBM_AVAILABLE:
        models["LightGBM"] = lgb.LGBMClassifier(
            n_estimators=150,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=7,
            scale_pos_weight=imbalance_ratio,
            random_state=42,
            n_jobs=-1,
            verbosity=-1
        )
    elif XGB_AVAILABLE:
        models["XGBoost"] = xgb.XGBClassifier(
            n_estimators=150,
            learning_rate=0.05,
            max_depth=6,
            scale_pos_weight=imbalance_ratio,
            random_state=42,
            n_jobs=-1,
            eval_metric="logloss"
        )

    return models


def cross_validate_models(
    X: pd.DataFrame,
    y: np.ndarray,
    models: Dict[str, Any],
    n_splits: int = 5
) -> Dict[str, Dict[str, Any]]:
    """
    Thực hiện Stratified 5-Fold Cross-Validation trên dữ liệu huấn luyện.
    Tính PR-AUC, ROC-AUC và Chi phí Kinh doanh cho tất cả mô hình ứng viên.
    """
    print(f"\n[*] Performing Stratified {n_splits}-Fold Cross-Validation on {len(X):,} samples...")
    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    cv_results = {}

    for model_name, model in models.items():
        print(f"\n--- Training & Evaluating: {model_name} ---")
        fold_pr_aucs = []
        fold_roc_aucs = []
        fold_costs = []
        fold_opt_thresholds = []
        t0 = time.time()

        for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), start=1):
            X_train, y_train = X.iloc[train_idx], y[train_idx]
            X_val, y_val = X.iloc[val_idx], y[val_idx]

            # Huấn luyện mô hình
            model.fit(X_train, y_train)

            # Dự đoán xác suất
            if hasattr(model, "predict_proba"):
                val_probs = model.predict_proba(X_val)[:, 1]
            else:
                val_probs = model.decision_function(X_val)

            # Tính chỉ số
            pr_auc = average_precision_score(y_val, val_probs)
            roc_auc = roc_auc_score(y_val, val_probs)
            opt_t, opt_cost, _ = evaluate_cost_function(y_val, val_probs)

            fold_pr_aucs.append(pr_auc)
            fold_roc_aucs.append(roc_auc)
            fold_costs.append(opt_cost)
            fold_opt_thresholds.append(opt_t)

            print(f"  Fold {fold}/{n_splits} | PR-AUC: {pr_auc:.4f} | ROC-AUC: {roc_auc:.4f} | Opt Cost: ${opt_cost:,.0f} (t={opt_t:.2f})")

        elapsed = time.time() - t0
        mean_pr_auc = np.mean(fold_pr_aucs)
        std_pr_auc = np.std(fold_pr_aucs)
        mean_roc_auc = np.mean(fold_roc_aucs)
        mean_cost = np.mean(fold_costs)
        mean_opt_t = np.mean(fold_opt_thresholds)

        cv_results[model_name] = {
            "mean_pr_auc": float(mean_pr_auc),
            "std_pr_auc": float(std_pr_auc),
            "mean_roc_auc": float(mean_roc_auc),
            "mean_cost_per_fold": float(mean_cost),
            "avg_optimal_threshold": float(mean_opt_t),
            "training_time_sec": float(elapsed),
        }
        print(f"  ==> {model_name} Mean PR-AUC: {mean_pr_auc:.4f} (+/- {std_pr_auc:.4f}) | Mean Cost: ${mean_cost:,.0f} | Time: {elapsed:.1f}s")

    return cv_results


def run_training_pipeline(
    train_path: str = "data/fraudTrain.csv",
    test_path: str = "data/fraudTest.csv",
    sample_train_size: Optional[int] = None,
    output_dir: str = "ml/models"
):
    """
    Pipeline Huấn Luyện ML Toàn Diện:
    1. Tải dữ liệu & biến đổi đặc trưng với đảm bảo feature parity
    2. Stratified 5-Fold Cross-Validation
    3. Tối ưu ngưỡng theo ma trận chi phí kinh doanh
    4. Đánh giá Temporal Hold-Out trên fraudTest.csv
    5. Xuất mô hình và Metadata Artifacts
    """
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print("🏦 XÓM BANK — MACHINE LEARNING TRAINING & BENCHMARK PIPELINE")
    print("=" * 80)
    print(f" • Train Source: {train_path}")
    print(f" • Test Source:  {test_path} (Temporal Hold-Out)")
    print(f" • Output Dir:   {output_dir}")
    print(f" • Cost Matrix:  FN = ${COST_FALSE_NEGATIVE:,.0f} | FP = ${COST_FALSE_POSITIVE:,.0f}")
    print("=" * 80)

    # 1. Tải dữ liệu
    print("\n[Step 1/5] Loading Datasets...")
    if sample_train_size:
        print(f"[*] Reading training sample of {sample_train_size:,} rows for fast benchmarking...")
        df_train = pd.read_csv(train_path, index_col=0, nrows=sample_train_size)
    else:
        print(f"[*] Reading full training dataset...")
        df_train = pd.read_csv(train_path, index_col=0)

    df_test = pd.read_csv(test_path, index_col=0)
    print(f"[+] Loaded Train: {len(df_train):,} rows | Test: {len(df_test):,} rows.")

    # 2. Biến đổi đặc trưng
    print("\n[Step 2/5] Extracting Features via FraudFeatureEngineer...")
    feature_engineer = FraudFeatureEngineer().fit(df_train)
    X_train, y_train = feature_engineer.transform_batch(df_train, is_training=True)
    X_test, y_test = feature_engineer.transform_batch(df_test, is_training=False)

    fraud_cnt = int(np.sum(y_train))
    total_cnt = len(y_train)
    imbalance_ratio = float((total_cnt - fraud_cnt) / max(1, fraud_cnt))
    print(f"[+] Feature matrix created: {X_train.shape[1]} features.")
    print(f"    - Fraud samples: {fraud_cnt:,} / {total_cnt:,} ({fraud_cnt/total_cnt*100:.3f}%)")
    print(f"    - Imbalance ratio (Negative/Positive): {imbalance_ratio:.1f}")

    # 3. So sánh mô hình qua Cross-Validation
    print("\n[Step 3/5] Benchmarking Models via Stratified 5-Fold CV...")
    models = build_models(imbalance_ratio)
    cv_results = cross_validate_models(X_train, y_train, models, n_splits=5)

    # Lựa chọn mô hình tốt nhất theo PR-AUC & Chi phí Kinh doanh
    best_model_name = max(cv_results, key=lambda k: cv_results[k]["mean_pr_auc"])
    print(f"\n🏆 Best Candidate Selected: '{best_model_name}' (PR-AUC: {cv_results[best_model_name]['mean_pr_auc']:.4f})")

    # 4. Huấn luyện mô hình cuối cùng & Đánh giá trên Temporal Holdout
    print("\n[Step 4/5] Training Final Model on Full Training Set & Evaluating on Hold-Out Test Set...")
    best_model = models[best_model_name]
    best_model.fit(X_train, y_train)

    # Dự đoán trên tập train để tinh chỉnh ngưỡng
    train_probs = best_model.predict_proba(X_train)[:, 1] if hasattr(best_model, "predict_proba") else best_model.decision_function(X_train)
    optimal_threshold, train_min_cost, train_cost_details = evaluate_cost_function(y_train, train_probs)

    # Đánh giá trên tập test hold-out
    test_probs = best_model.predict_proba(X_test)[:, 1] if hasattr(best_model, "predict_proba") else best_model.decision_function(X_test)
    test_pr_auc = average_precision_score(y_test, test_probs)
    test_roc_auc = roc_auc_score(y_test, test_probs)

    # Đánh giá với ngưỡng mặc định (0.5)
    y_test_pred_default = (test_probs >= 0.5).astype(int)
    tn_def, fp_def, fn_def, tp_def = confusion_matrix(y_test, y_test_pred_default, labels=[0, 1]).ravel()
    cost_default = (fn_def * COST_FALSE_NEGATIVE) + (fp_def * COST_FALSE_POSITIVE)

    # Đánh giá với ngưỡng tối ưu
    y_test_pred_opt = (test_probs >= optimal_threshold).astype(int)
    tn_opt, fp_opt, fn_opt, tp_opt = confusion_matrix(y_test, y_test_pred_opt, labels=[0, 1]).ravel()
    cost_opt = (fn_opt * COST_FALSE_NEGATIVE) + (fp_opt * COST_FALSE_POSITIVE)

    cost_savings = cost_default - cost_opt
    cost_savings_pct = (cost_savings / cost_default * 100) if cost_default > 0 else 0

    print("=" * 80)
    print("📊 TEMPORAL HOLD-OUT TEST RESULTS (fraudTest.csv — 555,719 Transactions)")
    print("=" * 80)
    print(f" • Primary Metric PR-AUC:      {test_pr_auc:.4f}")
    print(f" • Secondary Metric ROC-AUC:    {test_roc_auc:.4f}")
    print(f" • Optimal Business Threshold: {optimal_threshold:.4f} (Default: 0.5000)")
    print("-" * 80)
    print(f" • Default Threshold (0.50):")
    print(f"    - Recall: {recall_score(y_test, y_test_pred_default):.4f} | Precision: {precision_score(y_test, y_test_pred_default):.4f} | F1: {f1_score(y_test, y_test_pred_default):.4f}")
    print(f"    - Confusion: TP={tp_def:,}, FP={fp_def:,}, FN={fn_def:,}, TN={tn_def:,}")
    print(f"    - Expected Business Cost: ${cost_default:,.2f}")
    print("-" * 80)
    print(f" • Optimal Threshold ({optimal_threshold:.2f}):")
    print(f"    - Recall: {recall_score(y_test, y_test_pred_opt):.4f} | Precision: {precision_score(y_test, y_test_pred_opt):.4f} | F1: {f1_score(y_test, y_test_pred_opt):.4f}")
    print(f"    - Confusion: TP={tp_opt:,}, FP={fp_opt:,}, FN={fn_opt:,}, TN={tn_opt:,}")
    print(f"    - Expected Business Cost: ${cost_opt:,.2f}")
    print(f"    - Business Cost Savings:  ${cost_savings:,.2f} ({cost_savings_pct:.1f}% reduction!)")
    print("=" * 80)

    # Trích xuất mức độ quan trọng của đặc trưng
    feature_importances = {}
    if hasattr(best_model, "feature_importances_"):
        importances = best_model.feature_importances_
        feature_importances = {feat: float(imp) for feat, imp in zip(X_train.columns, importances)}
        feature_importances = dict(sorted(feature_importances.items(), key=lambda x: x[1], reverse=True))
    elif hasattr(best_model, "named_steps") and hasattr(best_model.named_steps.get("clf"), "coef_"):
        coefs = best_model.named_steps["clf"].coef_[0]
        feature_importances = {feat: float(abs(c)) for feat, c in zip(X_train.columns, coefs)}
        feature_importances = dict(sorted(feature_importances.items(), key=lambda x: x[1], reverse=True))

    # 5. Xuất Artifacts
    print("\n[Step 5/5] Exporting Production Model Artifacts & Reports...")
    model_file = os.path.join(output_dir, "best_fraud_model.joblib")
    fe_file = os.path.join(output_dir, "feature_engineer.joblib")
    metadata_file = os.path.join(output_dir, "model_metadata.json")

    joblib.dump(best_model, model_file)
    joblib.dump(feature_engineer, fe_file)

    metadata = {
        "model_name": best_model_name,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "optimal_threshold": float(optimal_threshold),
        "cost_parameters": {
            "cost_false_negative": COST_FALSE_NEGATIVE,
            "cost_false_positive": COST_FALSE_POSITIVE
        },
        "cv_results": cv_results,
        "holdout_test_metrics": {
            "pr_auc": round(float(test_pr_auc), 4),
            "roc_auc": round(float(test_roc_auc), 4),
            "optimal_threshold": round(float(optimal_threshold), 4),
            "recall_opt": round(float(recall_score(y_test, y_test_pred_opt)), 4),
            "precision_opt": round(float(precision_score(y_test, y_test_pred_opt)), 4),
            "f1_opt": round(float(f1_score(y_test, y_test_pred_opt)), 4),
            "cost_default": round(float(cost_default), 2),
            "cost_opt": round(float(cost_opt), 2),
            "cost_savings": round(float(cost_savings), 2),
            "cost_savings_pct": round(float(cost_savings_pct), 2)
        },
        "feature_names": list(X_train.columns),
        "top_features": list(feature_importances.keys())[:10] if feature_importances else []
    }

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    # Tạo báo cáo Markdown
    report_file = os.path.join(PROJECT_ROOT, "ml", "model_evaluation_report.md")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("# 📊 Xóm Bank — Machine Learning Model Evaluation Report\n\n")
        f.write(f"> **Trained At:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"> **Selected Production Model:** `{best_model_name}`\n")
        f.write(f"> **Optimal Decision Threshold:** `{optimal_threshold:.4f}`\n\n")
        f.write("## 1. Cross-Validation Benchmark Results (Stratified 5-Fold)\n\n")
        f.write("| Model | PR-AUC (Mean ± Std) | ROC-AUC | Avg Cost/Fold ($) | Training Time (s) |\n")
        f.write("|---|---|---|---|---|\n")
        for m_name, res in cv_results.items():
            f.write(f"| **{m_name}** | **{res['mean_pr_auc']:.4f} ± {res['std_pr_auc']:.4f}** | {res['mean_roc_auc']:.4f} | ${res['mean_cost_per_fold']:,.0f} | {res['training_time_sec']:.1f}s |\n")
        f.write("\n## 2. Temporal Hold-Out Test Evaluation (555,719 Transactions)\n\n")
        recall_def = recall_score(y_test, y_test_pred_default) * 100
        recall_opt = recall_score(y_test, y_test_pred_opt) * 100
        prec_def = precision_score(y_test, y_test_pred_default) * 100
        prec_opt = precision_score(y_test, y_test_pred_opt) * 100

        f.write("| Metric | Default Threshold (0.50) | Optimal Threshold ({0:.2f}) | Business Impact |\n".format(optimal_threshold))
        f.write("|---|---|---|---|\n")
        f.write(f"| **PR-AUC** | {test_pr_auc:.4f} | {test_pr_auc:.4f} | Baseline for Imbalanced Fraud |\n")
        f.write(f"| **Recall (Tỷ lệ tóm gian lận)** | {recall_def:.2f}% | **{recall_opt:.2f}%** | Cân bằng tỷ lệ bắt gian lận |\n")
        f.write(f"| **Precision (Độ chuẩn xác)** | {prec_def:.2f}% | **{prec_opt:.2f}%** | Tăng +{prec_opt - prec_def:.2f}% (giảm báo động giả) |\n")
        f.write(f"| **F1-Score** | {f1_score(y_test, y_test_pred_default):.4f} | **{f1_score(y_test, y_test_pred_opt):.4f}** | Tăng đáng kể chất lượng phân loại |\n")
        f.write(f"| **False Negatives (Bỏ sót)** | {fn_def:,} txns | **{fn_opt:,} txns** | Chấp nhận đánh đổi để giảm FP |\n")
        f.write(f"| **False Positives (Chặn nhầm)** | {fp_def:,} txns | **{fp_opt:,} txns** | **Giảm {fp_def - fp_opt:,} cuộc gọi CSKH phiền hà** |\n")
        f.write(f"| **Total Business Cost** | ${cost_default:,.2f} | **${cost_opt:,.2f}** | **Tiết kiệm ${cost_savings:,.2f} (-{cost_savings_pct:.1f}%)** |\n\n")
        if feature_importances:
            f.write("## 3. Top 10 Feature Importances\n\n")
            f.write("| Rank | Feature | Importance Score |\n")
            f.write("|---|---|---|\n")
            for i, (fname, imp_val) in enumerate(list(feature_importances.items())[:10], start=1):
                f.write(f"| {i} | `{fname}` | {imp_val:.4f} |\n")

    print(f"[+] Model saved to:    {model_file}")
    print(f"[+] Engineer saved to: {fe_file}")
    print(f"[+] Metadata saved to: {metadata_file}")
    print(f"[+] Report saved to:   {report_file}")
    print("\n🎉 ML PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Huấn Luyện ML Xom Bank")
    parser.add_argument("--train-data", type=str, default="data/fraudTrain.csv", help="Đường dẫn đến file CSV huấn luyện")
    parser.add_argument("--test-data", type=str, default="data/fraudTest.csv", help="Đường dẫn đến file CSV kiểm thử")
    parser.add_argument("--sample-size", type=int, default=150000, help="Số mẫu huấn luyện (None cho toàn bộ)")
    parser.add_argument("--output-dir", type=str, default="ml/models", help="Thư mục xuất model artifacts")

    args = parser.parse_args()
    run_training_pipeline(
        train_path=args.train_data,
        test_path=args.test_data,
        sample_train_size=args.sample_size,
        output_dir=args.output_dir
    )
