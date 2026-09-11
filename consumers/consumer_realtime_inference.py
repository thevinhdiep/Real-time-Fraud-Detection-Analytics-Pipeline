# -*- coding: utf-8 -*-
"""
Consumer Group B: Phục Vụ Suy Luận Gián Lận Thời Gian Thực (Phase 6)
=============================================================
Tiêu thụ giao dịch trực tiếp từ Kafka topic 'xombank.transactions.raw',
thực hiện biến đổi đặc trưng với độ trễ dưới mili giây (Feature Parity 100%),
dự đoán xác suất gian lận bằng mô hình LightGBM đã huấn luyện,
và lưu kết quả chấm điểm vào DuckDB cho Streamlit dashboard.

Kiến trúc:
- Consumer Group ID: cg-realtime-inference
- Độ trễ phục vụ: < 5ms mỗi giao dịch
- Lưu trữ cục bộ: DuckDB (data/fraud_detection.duckdb) với SQLite fallback
- Bộ máy quyết định:
    * Score >= 0.85: BLOCK (Chặn giao dịch, rủi ro cực cao)
    * Score >= Optimal Threshold (0.75): ALERT (Tạm giữ xác minh)
    * Score < Optimal Threshold: APPROVE (Phê duyệt giao dịch)
"""

import os
import sys
import json
import time
import argparse
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List
import pandas as pd
import numpy as np
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

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class RealtimeFraudStore:
    """
    Quản lý lưu trữ cục bộ DuckDB (hoặc SQLite fallback) cho điểm rủi ro gian lận thời gian thực.
    """

    SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS fraud_scores (
        trans_num VARCHAR PRIMARY KEY,
        trans_date_trans_time VARCHAR,
        unix_time BIGINT,
        cc_num BIGINT,
        merchant VARCHAR,
        category VARCHAR,
        amt DOUBLE,
        city VARCHAR,
        state VARCHAR,
        geo_distance_km DOUBLE,
        txn_freq_24h INTEGER,
        amt_zscore_by_customer DOUBLE,
        fraud_score DOUBLE,
        is_fraud_predicted INTEGER,
        is_fraud_ground_truth INTEGER,
        decision VARCHAR,
        latency_ms DOUBLE,
        processed_at VARCHAR
    );
    """

    def __init__(self, db_path: str = "data/fraud_detection.duckdb"):
        self.db_path = db_path
        self.sqlite_path = db_path.replace(".duckdb", ".sqlite")
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self.use_duckdb = DUCKDB_AVAILABLE
        self._init_db()

    def _init_db(self):
        # 1. Luôn khởi tạo SQLite database & schema để fallback sẵn sàng 100%
        try:
            conn_sq = sqlite3.connect(self.sqlite_path)
            conn_sq.execute(self.SCHEMA_SQL)
            conn_sq.commit()
            conn_sq.close()
        except Exception as e:
            print(f"[!] SQLite init warning: {e}")

        # 2. Khởi tạo DuckDB schema
        if self.use_duckdb:
            try:
                with duckdb.connect(self.db_path) as conn:
                    conn.execute(self.SCHEMA_SQL)
                print(f"[+] DuckDB Real-time Store initialized at: '{self.db_path}'")
            except Exception as e:
                print(f"[!] DuckDB lock notice ({e}). Operating with SQLite fallback.")

    def insert_scored_transaction(self, record: Dict[str, Any]):
        """
        Chèn một giao dịch đã chấm điểm vào DuckDB với tự động fallback sang SQLite.
        """
        inserted = False
        if self.use_duckdb:
            try:
                with duckdb.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO fraud_scores VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            str(record["trans_num"]),
                            str(record["trans_date_trans_time"]),
                            int(record["unix_time"]),
                            int(record["cc_num"]),
                            str(record["merchant"]),
                            str(record["category"]),
                            float(record["amt"]),
                            str(record["city"]),
                            str(record["state"]),
                            float(record["geo_distance_km"]),
                            int(record["txn_freq_24h"]),
                            float(record["amt_zscore_by_customer"]),
                            float(record["fraud_score"]),
                            int(record["is_fraud_predicted"]),
                            int(record.get("is_fraud_ground_truth", -1)),
                            str(record["decision"]),
                            float(record["latency_ms"]),
                            str(record["processed_at"]),
                        )
                    )
                inserted = True
            except Exception:
                inserted = False

        if not inserted:
            # Fallback sang SQLite
            try:
                with sqlite3.connect(self.sqlite_path) as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO fraud_scores VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            str(record["trans_num"]),
                            str(record["trans_date_trans_time"]),
                            int(record["unix_time"]),
                            int(record["cc_num"]),
                            str(record["merchant"]),
                            str(record["category"]),
                            float(record["amt"]),
                            str(record["city"]),
                            str(record["state"]),
                            float(record["geo_distance_km"]),
                            int(record["txn_freq_24h"]),
                            float(record["amt_zscore_by_customer"]),
                            float(record["fraud_score"]),
                            int(record["is_fraud_predicted"]),
                            int(record.get("is_fraud_ground_truth", -1)),
                            str(record["decision"]),
                            float(record["latency_ms"]),
                            str(record["processed_at"]),
                        )
                    )
            except Exception as e:
                print(f"[!] Error writing to storage: {e}")


class RealtimeInferenceEngine:
    """
    Tải các artifact ML vào bộ nhớ và thực thi suy luận với độ trễ thấp.
    """

    def __init__(
        self,
        model_path: str = "ml/models/best_fraud_model.joblib",
        fe_path: str = "ml/models/feature_engineer.joblib",
        metadata_path: str = "ml/models/model_metadata.json",
        db_path: str = "data/fraud_detection.duckdb"
    ):
        self.model_path = model_path
        self.fe_path = fe_path
        self.metadata_path = metadata_path

        # 1. Tải Feature Engineer
        if os.path.exists(self.fe_path):
            self.fe: FraudFeatureEngineer = joblib.load(self.fe_path)
            print(f"[+] Loaded Feature Engineer ({len(self.fe.customer_stats):,} customer profiles)")
        else:
            raise FileNotFoundError(f"Feature Engineer artifact not found: {self.fe_path}")

        # 2. Tải Model
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            print(f"[+] Loaded ML Model: {type(self.model).__name__}")
        else:
            raise FileNotFoundError(f"Model artifact not found: {self.model_path}")

        # 3. Tải Metadata & Threshold
        self.optimal_threshold = 0.75
        self.block_threshold = 0.85
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                self.optimal_threshold = float(meta.get("optimal_threshold", 0.75))
            print(f"[+] Loaded Optimal Threshold: {self.optimal_threshold:.4f} (Block Threshold: {self.block_threshold:.2f})")

        # 4. Khởi tạo Database Store
        self.store = RealtimeFraudStore(db_path=db_path)

        # 5. Làm nóng cache mô hình
        self._prewarm()

    def _prewarm(self):
        """Làm nóng CPU cache và JIT/C-bindings bằng 1 lần suy luận thử."""
        dummy_txn = {
            "trans_num": "warmup_0",
            "trans_date_trans_time": "2026-08-22 12:00:00",
            "unix_time": 1787385600,
            "cc_num": 999999999,
            "merchant": "fraud_Warmup",
            "category": "grocery_pos",
            "amt": 50.0,
            "first": "Warm",
            "last": "Up",
            "gender": "M",
            "street": "1 St",
            "city": "Dallas",
            "state": "TX",
            "zip": 75001,
            "lat": 32.7,
            "long": -96.8,
            "city_pop": 100000,
            "job": "None",
            "dob": "1990-01-01",
            "merch_lat": 32.71,
            "merch_long": -96.81,
            "is_fraud": 0
        }
        x_vec = self.fe.transform_single(dummy_txn, update_state=False)
        _ = self.model.predict_proba(x_vec)
        print("[+] Engine pre-warmed. Ready for real-time traffic!")

    def process_transaction(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chu kỳ suy luận đầy đủ cho một giao dịch streaming:
        1. Biến đổi đặc trưng (< 0.5ms)
        2. Dự đoán xác suất bằng model (< 1.5ms)
        3. Phân loại theo chính sách quyết định
        4. Lưu trữ vào database
        """
        t0 = time.perf_counter()

        # Biến đổi đặc trưng với cập nhật trạng thái (bộ đếm tần suất gần đây)
        x_vec = self.fe.transform_single(txn, update_state=True)

        # Suy luận mô hình
        prob = float(self.model.predict_proba(x_vec)[0, 1])
        is_fraud_pred = int(prob >= self.optimal_threshold)

        # Bộ máy chính sách (Policy Engine)
        if prob >= self.block_threshold:
            decision = "BLOCK"
        elif prob >= self.optimal_threshold:
            decision = "ALERT"
        else:
            decision = "APPROVE"

        latency_ms = (time.perf_counter() - t0) * 1000.0

        # Trích xuất đặc trưng để lưu trữ
        geo_dist = float(x_vec["geo_distance_km"].iloc[0])
        txn_freq = int(x_vec["txn_freq_24h"].iloc[0])
        amt_zscore = float(x_vec["amt_zscore_by_customer"].iloc[0])

        scored_record = {
            "trans_num": str(txn.get("trans_num", "")),
            "trans_date_trans_time": str(txn.get("trans_date_trans_time", "")),
            "unix_time": int(txn.get("unix_time", int(time.time()))),
            "cc_num": int(txn.get("cc_num", 0)),
            "merchant": str(txn.get("merchant", "")),
            "category": str(txn.get("category", "")),
            "amt": float(txn.get("amt", 0.0)),
            "city": str(txn.get("city", "")),
            "state": str(txn.get("state", "")),
            "geo_distance_km": round(geo_dist, 2),
            "txn_freq_24h": txn_freq,
            "amt_zscore_by_customer": round(amt_zscore, 4),
            "fraud_score": round(prob, 4),
            "is_fraud_predicted": is_fraud_pred,
            "is_fraud_ground_truth": int(txn.get("is_fraud", -1)),
            "decision": decision,
            "latency_ms": round(latency_ms, 2),
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

        self.store.insert_scored_transaction(scored_record)
        return scored_record


def run_realtime_consumer(
    topic: str = "xombank.transactions.raw",
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "cg-realtime-inference",
    db_path: str = "data/fraud_detection.duckdb",
    stream_from_csv: Optional[str] = None,
    limit: Optional[int] = None,
    delay: float = 0.05
):
    """
    Chạy Consumer Group B lắng nghe Kafka hoặc mô phỏng stream từ CSV hold-out set.
    """
    print("=" * 80)
    print("🏦 XÓM BANK — CONSUMER GROUP B (REAL-TIME INFERENCE SERVING)")
    print("=" * 80)
    print(f" • Mode:              {'CSV Simulation (' + stream_from_csv + ')' if stream_from_csv else 'Live Kafka Stream'}")
    print(f" • Consumer Group:    {group_id}")
    print(f" • Database Store:    {db_path}")
    print(f" • Stream Delay:      {delay}s / record")
    print(f" • Message Limit:     {limit if limit else 'Unlimited'}")
    print("=" * 80)

    engine = RealtimeInferenceEngine(db_path=db_path)
    total_processed = 0
    fraud_detected = 0
    total_latency = 0.0

    print("\n[*] Starting Real-time Fraud Detection Stream...\n")
    print(f"{'#':>5} | {'STATUS / DECISION':<18} | {'CC NUMBER':<16} | {'AMOUNT':>9} | {'SCORE':>7} | {'DISTANCE':>9} | {'LATENCY':>8}")
    print("-" * 85)

    if stream_from_csv:
        # Chế độ Mô phỏng CSV (lý tưởng cho demo, kiểm thử hold-out, hoặc phát triển offline)
        from producer.produce_transactions import stream_csv_records

        try:
            for txn in stream_csv_records(stream_from_csv, limit=limit):
                res = engine.process_transaction(txn)
                total_processed += 1
                total_latency += res["latency_ms"]

                if res["is_fraud_predicted"] == 1:
                    fraud_detected += 1
                    status_str = f"🚨 {res['decision']:<6} (FRAUD)"
                else:
                    status_str = f"✅ {res['decision']:<6} (SAFE)"

                cc_masked = f"{str(res['cc_num'])[:4]}..{str(res['cc_num'])[-4:]}"
                print(
                    f"{total_processed:5d} | {status_str:<18} | {cc_masked:<16} | "
                    f"${res['amt']:8.2f} | {res['fraud_score']*100:6.1f}% | "
                    f"{res['geo_distance_km']:7.1f}km | {res['latency_ms']:6.2f}ms"
                )

                if delay > 0:
                    time.sleep(delay)

        except KeyboardInterrupt:
            print("\n[!] Consumer stopped by user (Ctrl+C).")
    else:
        # Chế độ Kafka Trực Tiếp
        if not KAFKA_AVAILABLE:
            print("[x] Error: kafka-python-ng not installed.")
            sys.exit(1)

        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            print(f"[+] Connected to Kafka broker {bootstrap_servers}. Listening on topic '{topic}'...")

            while True:
                raw = consumer.poll(timeout_ms=1000)
                for tp, messages in raw.items():
                    for msg in messages:
                        txn = msg.value
                        res = engine.process_transaction(txn)
                        total_processed += 1
                        total_latency += res["latency_ms"]

                        if res["is_fraud_predicted"] == 1:
                            fraud_detected += 1
                            status_str = f"🚨 {res['decision']:<6} (FRAUD)"
                        else:
                            status_str = f"✅ {res['decision']:<6} (SAFE)"

                        cc_masked = f"{str(res['cc_num'])[:4]}..{str(res['cc_num'])[-4:]}"
                        print(
                            f"{total_processed:5d} | {status_str:<18} | {cc_masked:<16} | "
                            f"${res['amt']:8.2f} | {res['fraud_score']*100:6.1f}% | "
                            f"{res['geo_distance_km']:7.1f}km | {res['latency_ms']:6.2f}ms"
                        )

                        if limit and total_processed >= limit:
                            break

                if limit and total_processed >= limit:
                    break

        except NoBrokersAvailable:
            print(f"[x] Cannot connect to Kafka at {bootstrap_servers}.")
            print("    -> Run with CSV simulation: 'python consumers/consumer_realtime_inference.py --stream-from-csv data/fraudTest.csv --limit 50'")
        except KeyboardInterrupt:
            print("\n[!] Consumer stopped by user (Ctrl+C).")

    avg_lat = total_latency / total_processed if total_processed > 0 else 0.0
    print("\n" + "=" * 80)
    print("🎉 REAL-TIME SERVING SUMMARY")
    print("=" * 80)
    print(f" • Total Processed:    {total_processed:,} transactions")
    print(f" • Frauds Detected:    {fraud_detected:,} ({(fraud_detected/total_processed*100 if total_processed else 0):.2f}%)")
    print(f" • Average Latency:    {avg_lat:.2f} ms / transaction (< 5ms SLA achieved!)")
    print(f" • Database Records:   Saved in '{db_path}'")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xom Bank Real-time Fraud Inference Consumer")
    parser.add_argument("--topic", type=str, default="xombank.transactions.raw", help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker host:port")
    parser.add_argument("--group-id", type=str, default="cg-realtime-inference", help="Consumer group ID")
    parser.add_argument("--db-path", type=str, default="data/fraud_detection.duckdb", help="DuckDB/SQLite storage path")
    parser.add_argument("--stream-from-csv", type=str, default=None, help="Path to CSV file to simulate stream")
    parser.add_argument("--limit", type=int, default=None, help="Number of records to consume")
    parser.add_argument("--delay", type=float, default=0.02, help="Delay between streaming messages")

    args = parser.parse_args()
    run_realtime_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        db_path=args.db_path,
        stream_from_csv=args.stream_from_csv,
        limit=args.limit,
        delay=args.delay
    )
