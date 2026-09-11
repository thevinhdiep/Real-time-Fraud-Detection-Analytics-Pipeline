# -*- coding: utf-8 -*-
"""
Producer Giao Dịch Xóm Bank (Phase 1)
Đọc giao dịch thẻ tín dụng từ CSV và truyền trực tuyến dưới dạng JSON vào Apache Kafka.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Generator, Optional
import pandas as pd

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


def serialize_transaction(row: pd.Series) -> Dict[str, Any]:
    """
    Chuyển đổi một dòng pandas DataFrame thành dictionary JSON sạch.
    """
    return {
        "trans_num": str(row["trans_num"]),
        "trans_date_trans_time": str(row["trans_date_trans_time"]),
        "unix_time": int(row["unix_time"]),
        "cc_num": int(row["cc_num"]),
        "merchant": str(row["merchant"]),
        "category": str(row["category"]),
        "amt": float(row["amt"]),
        "first": str(row["first"]),
        "last": str(row["last"]),
        "gender": str(row["gender"]),
        "street": str(row["street"]),
        "city": str(row["city"]),
        "state": str(row["state"]),
        "zip": int(row["zip"]),
        "lat": float(row["lat"]),
        "long": float(row["long"]),
        "city_pop": int(row["city_pop"]),
        "job": str(row["job"]),
        "dob": str(row["dob"]),
        "merch_lat": float(row["merch_lat"]),
        "merch_long": float(row["merch_long"]),
        "is_fraud": int(row["is_fraud"]),
        "produced_at": datetime.now(timezone.utc).isoformat()
    }


def stream_csv_records(
    file_path: str,
    chunk_size: int = 5000,
    limit: Optional[int] = None
) -> Generator[Dict[str, Any], None, None]:
    """
    Đọc và truyền từng dòng từ file CSV theo từng chunk để tránh tràn bộ nhớ.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    total_yielded = 0
    with pd.read_csv(file_path, index_col=0, chunksize=chunk_size) as reader:
        for chunk in reader:
            # Đảm bảo sắp xếp theo thứ tự thời gian
            if "trans_date_trans_time" in chunk.columns:
                chunk = chunk.sort_values(by="trans_date_trans_time")

            for _, row in chunk.iterrows():
                yield serialize_transaction(row)
                total_yielded += 1
                if limit and total_yielded >= limit:
                    return


def ensure_topic_exists(bootstrap_servers: str, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    """
    Tạo topic Kafka với số partition chỉ định nếu chưa tồn tại.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="xombank-admin-client",
            request_timeout_ms=5000
        )
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            admin_client.create_topics([topic])
            print(f"[*] Topic '{topic_name}' da duoc tao thanh cong ({num_partitions} partitions).")
        else:
            print(f"[*] Topic '{topic_name}' da ton tai.")
        admin_client.close()
    except Exception as e:
        print(f"[!] Canh bao khi kiem tra/tao topic: {e}")


def run_producer(
    file_path: str,
    topic: str,
    bootstrap_servers: str,
    delay: float = 0.01,
    limit: Optional[int] = None,
    dry_run: bool = False
):
    """
    Chạy Kafka Producer để truyền giao dịch vào topic.
    """
    print("=" * 75)
    print("🏦 XÓM BANK — PYTHON TRANSACTION PRODUCER")
    print("=" * 75)
    print(f" • File nguon:        {file_path}")
    print(f" • Kafka Topic:       {topic}")
    print(f" • Bootstrap Servers: {bootstrap_servers}")
    print(f" • Do tre (delay):    {delay}s / record")
    print(f" • Gioi han (limit):  {limit if limit else 'Khong gioi han (toan bo dataset)'}")
    print(f" • Che do Dry-run:    {'BAT (Khong day vao Kafka)' if dry_run else 'TAT (Day vao Kafka)'}")
    print("=" * 75)

    producer = None
    if not dry_run:
        if not KAFKA_AVAILABLE:
            print("[x] Loi: Thu vien kafka-python chua duoc cai dat. Hay chay: pip install kafka-python-ng")
            sys.exit(1)

        print("\n[*] Dang ket noi toi Kafka Cluster...")
        try:
            ensure_topic_exists(bootstrap_servers, topic, num_partitions=3)
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print("[+] Ket noi Kafka thanh cong!\n")
        except NoBrokersAvailable:
            print(f"[x] Khong the ket noi toi Kafka tai {bootstrap_servers}.")
            print("    -> Vui long khoi dong Docker Compose: 'docker compose up -d'")
            print("    -> Hoac chay che do test thu khong can Kafka: 'python producer/produce_transactions.py --dry-run --limit 10'\n")
            sys.exit(1)
        except Exception as e:
            print(f"[x] Loi khoi tao Producer: {e}")
            sys.exit(1)

    sent_count = 0
    fraud_count = 0
    start_time = time.time()

    print("[*] Bat dau stream giao dich... (Nhan Ctrl+C de dung)\n")
    try:
        for record in stream_csv_records(file_path, limit=limit):
            cc_num = record["cc_num"]
            is_fraud = record["is_fraud"]

            if is_fraud:
                fraud_count += 1

            if not dry_run and producer:
                # Key là cc_num để đảm bảo cùng 1 khách hàng luôn vào cùng 1 partition
                future = producer.send(topic, key=cc_num, value=record)

            sent_count += 1

            if sent_count % 100 == 0 or sent_count == 1 or is_fraud or (limit and sent_count == limit):
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                status_icon = "🚨 FRAUD" if is_fraud else "✅ LEGIT"
                print(
                    f"[{sent_count:7d}] {status_icon} | CC: {str(cc_num)[:4]}..{str(cc_num)[-4:]} | "
                    f"Amt: ${record['amt']:7.2f} | Category: {record['category']:15s} | "
                    f"Speed: {rate:6.1f} msg/s | Frauds: {fraud_count}"
                )

            if delay > 0:
                time.sleep(delay)

        if producer:
            print("\n[*] Dang flush toan bo message con lai vao Kafka...")
            producer.flush()

        elapsed = time.time() - start_time
        avg_rate = sent_count / elapsed if elapsed > 0 else 0
        print("\n" + "=" * 75)
        print("🎉 STREAMING HOAN TAT!")
        print(f" • Tong giao dich da gui: {sent_count:,}")
        print(f" • So giao dich gian lan:  {fraud_count:,} ({(fraud_count/sent_count*100 if sent_count else 0):.3f}%)")
        print(f" • Thoi gian thuc hien:    {elapsed:.2f}s (Trung binh {avg_rate:.1f} msg/s)")
        print("=" * 75)

    except KeyboardInterrupt:
        print("\n\n[!] Nguoi dung da tam dung stream (Ctrl+C).")
        if producer:
            producer.flush()
            producer.close()
        print("[*] Da dong ket noi an toan.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xom Bank Kafka Transaction Producer")
    parser.add_argument("--file", type=str, default="data/fraudTrain.csv", help="Duong dan toi file CSV nguon")
    parser.add_argument("--topic", type=str, default="xombank.transactions.raw", help="Ten Kafka Topic")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker host:port")
    parser.add_argument("--delay", type=float, default=0.01, help="Do tre giua cac message (giay)")
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so record gui (None = gui het)")
    parser.add_argument("--dry-run", action="store_true", help="Chay thu doc va serialize ma khong day vao Kafka")

    args = parser.parse_args()
    run_producer(
        file_path=args.file,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        delay=args.delay,
        limit=args.limit,
        dry_run=args.dry_run
    )
