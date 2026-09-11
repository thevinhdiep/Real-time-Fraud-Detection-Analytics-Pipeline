# -*- coding: utf-8 -*-
"""
Consumer Group A: Nạp dữ liệu Batch Analytics cho Xóm Bank (Phase 2)
Tiêu thụ giao dịch từ Kafka, gom thành các batch phân vùng theo thời gian,
và ghi file Parquet nén Snappy xuống MinIO Data Lake.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Thêm thư mục gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_lake_client.minio_client import MinIODataLakeClient

try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


def extract_partition_keys(record: Dict[str, Any]) -> tuple[str, str]:
    """
    Trích xuất ngày (YYYY-MM-DD) và giờ (HH) từ timestamp giao dịch.
    """
    ts_str = record.get("trans_date_trans_time", "")
    try:
        dt_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        dt = dt_obj.strftime("%Y-%m-%d")
        hour = dt_obj.strftime("%H")
        return dt, hour
    except Exception:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%H")


def flush_partitioned_records(
    records: List[Dict[str, Any]],
    minio_client: Optional[MinIODataLakeClient],
    local_storage_dir: str = "data_lake_storage"
) -> int:
    """
    Gom bản ghi theo phân vùng Hive (ngày, giờ) và ghi file Parquet xuống MinIO hoặc lưu trữ local.
    Trả về số lượng file Parquet đã tạo.
    """
    if not records:
        return 0

    # Nhóm bản ghi theo (ngày, giờ)
    partitions = defaultdict(list)
    for r in records:
        key = extract_partition_keys(r)
        partitions[key].append(r)

    files_written = 0
    now_ts = int(time.time() * 1000)

    for (dt, hour), part_records in partitions.items():
        df = pd.DataFrame(part_records)
        part_filename = f"part-{now_ts}-{len(part_records)}.parquet"
        object_key = f"raw/transactions/dt={dt}/hour={hour}/{part_filename}"


        uploaded = False
        if minio_client and minio_client.is_connected():
            uploaded = minio_client.upload_parquet_dataframe(df, object_key)

        if not uploaded:
            # Lưu trữ local nếu MinIO không khả dụng
            local_target_dir = os.path.join(local_storage_dir, "raw", "transactions", f"dt={dt}", f"hour={hour}")
            os.makedirs(local_target_dir, exist_ok=True)
            local_file_path = os.path.join(local_target_dir, part_filename)

            table = pa.Table.from_pandas(df)
            pq.write_table(table, local_file_path, compression="snappy")
            print(f"   [💾 LOCAL] Da ghi {len(part_records):,} records -> {local_file_path}")
        else:
            print(f"   [☁️ MINIO] Da upload {len(part_records):,} records -> s3://{minio_client.bucket_name}/{object_key}")

        files_written += 1

    return files_written


def run_batch_consumer(
    topic: str = "xombank.transactions.raw",
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "cg-batch-analytics",
    batch_size: int = 500,
    flush_interval: float = 15.0,
    max_messages: Optional[int] = None,
    local_storage_dir: str = "data_lake_storage"
):
    """
    Chạy Kafka Consumer Group A để nạp batch giao dịch vào MinIO Parquet.
    """
    print("=" * 75)
    print("🏦 XÓM BANK — CONSUMER GROUP A (BATCH INGESTION -> MINIO)")
    print("=" * 75)
    print(f" • Kafka Topic:       {topic}")
    print(f" • Bootstrap Servers: {bootstrap_servers}")
    print(f" • Consumer Group:    {group_id}")
    print(f" • Batch Size:        {batch_size} messages")
    print(f" • Flush Interval:    {flush_interval}s")
    print(f" • Local Fallback:    {local_storage_dir}")
    print("=" * 75)

    if not KAFKA_AVAILABLE:
        print("[x] Loi: Thu vien kafka-python chua duoc cai dat.")
        sys.exit(1)

    minio_client = MinIODataLakeClient()
    connected_minio = minio_client.is_connected()
    if connected_minio:
        minio_client.ensure_bucket()
        print("[+] MinIO Data Lake: SAN SANG (Bucket: " + minio_client.bucket_name + ")")
    else:
        print("[!] MinIO chua khoi dong -> Se su dung local storage fallback (" + local_storage_dir + ")")

    print("\n[*] Dang ket noi toi Kafka Broker...")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        print("[+] Ket noi Kafka thanh cong! Dang lang nghe giao dich...\n")
    except NoBrokersAvailable:
        print(f"[x] Khong the ket noi toi Kafka tai {bootstrap_servers}.")
        print("    -> Vui long khoi dong Docker: 'docker compose up -d'\n")
        sys.exit(1)
    except Exception as e:
        print(f"[x] Loi khoi tao Consumer: {e}")
        sys.exit(1)

    buffer: List[Dict[str, Any]] = []
    last_flush_time = time.time()
    total_consumed = 0
    total_files_written = 0

    try:
        while True:
            raw_messages = consumer.poll(timeout_ms=1000)

            for topic_partition, msgs in raw_messages.items():
                for msg in msgs:
                    buffer.append(msg.value)
                    total_consumed += 1

            now = time.time()
            time_since_flush = now - last_flush_time
            should_flush = len(buffer) >= batch_size or (buffer and time_since_flush >= flush_interval)

            if should_flush:
                print(f"\n[*] Dang flush batch {len(buffer):,} records (da gom {time_since_flush:.1f}s)...")
                files_cnt = flush_partitioned_records(buffer, minio_client, local_storage_dir)
                total_files_written += files_cnt

                # Commit offset đồng bộ sau khi upload Parquet thành công (đảm bảo At-Least-Once)
                consumer.commit()
                buffer.clear()
                last_flush_time = time.time()
                print(f"[+] Commit offset thanh cong! Tong da xu ly: {total_consumed:,} records | {total_files_written} files Parquet.")

            if max_messages and total_consumed >= max_messages:
                print(f"\n[*] Da dat gioi han max_messages={max_messages}. Dung consumer.")
                break

        if buffer:
            print(f"\n[*] Dang flush {len(buffer)} records con lai trong buffer...")
            files_cnt = flush_partitioned_records(buffer, minio_client, local_storage_dir)
            total_files_written += files_cnt
            consumer.commit()
            buffer.clear()
            print(f"[+] Commit offset thanh cong! Tong da xu ly: {total_consumed:,} records | {total_files_written} files Parquet.")

    except KeyboardInterrupt:
        print("\n\n[!] Nguoi dung da dung Consumer (Ctrl+C).")
        if buffer:
            print(f"[*] Dang flush {len(buffer)} records con lai trong buffer...")
            flush_partitioned_records(buffer, minio_client, local_storage_dir)
            consumer.commit()
        consumer.close()
        print("[*] Da dong consumer an toan.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xom Bank Batch Analytics Consumer (MinIO)")
    parser.add_argument("--topic", type=str, default="xombank.transactions.raw", help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker host:port")
    parser.add_argument("--group-id", type=str, default="cg-batch-analytics", help="Consumer Group ID")
    parser.add_argument("--batch-size", type=int, default=500, help="So message toi da moi batch")
    parser.add_argument("--flush-interval", type=float, default=15.0, help="Khoang thoi gian toi da flush batch (giay)")
    parser.add_argument("--max-messages", type=int, default=None, help="Gioi han message consume roi thoat")
    parser.add_argument("--local-dir", type=str, default="data_lake_storage", help="Thu muc fallback local")

    args = parser.parse_args()
    run_batch_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        max_messages=args.max_messages,
        local_storage_dir=args.local_dir
    )
