# -*- coding: utf-8 -*-
"""
MinIO Data Lake Client cho Xóm Bank (Phase 2)
Xử lý các thao tác lưu trữ đối tượng tương thích S3 cho các file Parquet.
"""

import os
import io
import sys
from typing import Optional, List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Đảm bảo hiển thị utf-8 trên Windows (tương thích CLI & Jupyter)
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from minio import Minio
from minio.error import S3Error


class MinIODataLakeClient:
    """
    Client giao tiếp với MinIO / Data Lake tương thích S3.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False,
        bucket_name: str = "xombank-datalake"
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.secure = secure
        self.bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "xombank-datalake")

        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )

    def is_connected(self) -> bool:
        """
        Kiểm tra nhanh kết nối tới MinIO server thông qua socket.
        """
        import socket
        try:
            host, port_str = self.endpoint.split(":")
            port = int(port_str)
            with socket.create_connection((host, port), timeout=1.5):
                return True
        except Exception:
            return False

    def ensure_bucket(self, bucket_name: Optional[str] = None) -> bool:
        """
        Đảm bảo bucket đã tồn tại; tự động tạo mới nếu chưa có.
        """
        target_bucket = bucket_name or self.bucket_name
        try:
            if not self.client.bucket_exists(target_bucket):
                self.client.make_bucket(target_bucket)
                print(f"[+] Da tao bucket moi: '{target_bucket}'")
            return True
        except S3Error as err:
            print(f"[!] S3 Error khi tao bucket '{target_bucket}': {err}")
            return False
        except Exception as err:
            print(f"[!] Khong the ket noi MinIO de kiem tra bucket: {err}")
            return False

    def upload_parquet_dataframe(
        self,
        df: pd.DataFrame,
        object_name: str,
        compression: str = "snappy"
    ) -> bool:
        """
        Chuyển đổi DataFrame sang định dạng Parquet và tải trực tiếp lên MinIO.
        """
        try:
            self.ensure_bucket()
            
            # Chuyển đổi DataFrame sang bộ nhớ đệm PyArrow Parquet trong RAM
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=compression)
            buffer_size = buffer.tell()
            buffer.seek(0)

            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=buffer,
                length=buffer_size,
                content_type="application/octet-stream"
            )
            return True
        except Exception as e:
            print(f"[x] Loi khi upload Parquet len MinIO '{object_name}': {e}")
            return False

    def list_objects(self, prefix: str = "") -> List[str]:
        """
        Liệt kê tất cả danh sách object key theo tiền tố (prefix) chỉ định.
        """
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except Exception as e:
            print(f"[!] Loi list objects: {e}")
            return []


if __name__ == "__main__":
    client = MinIODataLakeClient()
    print("=" * 65)
    print("🏦 XOM BANK — MINIO DATA LAKE CLIENT CHECK")
    print("=" * 65)
    print(f" • Endpoint: {client.endpoint}")
    print(f" • Bucket:   {client.bucket_name}")
    connected = client.is_connected()
    print(f" • Trang thai ket noi MinIO: {'✅ KET NOI TOT' if connected else '❌ CHUA KHOI DONG DOCKER'}")
    if connected:
        client.ensure_bucket()
    print("=" * 65)
