# 🔌 Hướng Dẫn Kết Nối Power BI Desktop Với Google BigQuery

> **Tài liệu hướng dẫn:** Kết nối Power BI Desktop với Data Warehouse `xombank_dw_marts` trên Google Cloud BigQuery.

---

## 🛠️ CÁCH 1: Kết nối trực tiếp qua Google Cloud OAuth2 (Khuyên Dùng — Nhanh & Đơn Giản Nhất)

1. Mở phần mềm **Power BI Desktop**.
2. Trên thanh Ribbon, chọn **Home ➔ Get Data ➔ More...**
3. Tìm kiếm **"Google BigQuery"** ➔ bấm **Connect**.
4. Cửa sổ đăng nhập xuất hiện:
   - Bấm **Sign in**.
   - Trình duyệt sẽ mở ra trang đăng nhập Google ➔ Chọn tài khoản Google GCP của bạn (tài khoản đã tạo project `xombank-fraud-analytics`).
   - Bấm **Allow (Cho phép)** để cấp quyền cho Power BI truy cập BigQuery.
5. Sau khi đăng nhập thành công, quay lại Power BI:
   - Bấm **Connect**.
6. Trong cửa sổ **Navigator**:
   - Mở cây thư mục: `xombank-fraud-analytics` ➔ `xombank_dw_marts`.
   - Tích chọn 3 Views đã được dbt sinh ra:
     - `dim_customers`
     - `dim_merchants`
     - `fct_transactions_fraud_features`
7. Chọn chế độ **Import** (hoặc **DirectQuery**):
   - **Khuyến nghị chọn `Import`:** Tốc độ render biểu đồ và chạy hàm DAX cực nhanh, tận dụng VertiPaq Engine của Power BI trong bộ nhớ RAM máy tính.
8. Bấm **Load**.

---

## 🛠️ CÁCH 2: Kết nối qua Service Account Key (`gcp_key.json` — Không cần đăng nhập trình duyệt)

Nếu máy tính của bạn sử dụng tài khoản doanh nghiệp hoặc muốn tự động hóa:

1. Trong Power BI, chọn **Get Data ➔ Google BigQuery**.
2. Tại màn hình xác thực, chọn tab **Service Account Key**.
3. Dán toàn bộ nội dung file JSON [`gcp_key.json`](file:///c:/Users/Dell/Desktop/Project3/gcp_key.json) hoặc chọn đường dẫn tới file key.
4. Bấm **Connect** ➔ Tải 3 bảng trong dataset `xombank_dw_marts`.

---

## 🔗 CÁCH 3: Tải file CSV xuất khẩu trực tiếp (Nếu muốn thiết kế Offline hoàn toàn)

Nếu bạn muốn làm việc offline không cần internet hoặc trình kết nối BigQuery:
1. Bạn có thể xuất 3 bảng từ BigQuery ra file CSV hoặc Parquet local.
2. Trong Power BI chọn **Get Data ➔ Text/CSV** ➔ Nạp 3 bảng và tạo quan hệ theo [`powerbi/data_model_schema.md`](file:///c:/Users/Dell/Desktop/Project3/powerbi/data_model_schema.md).
