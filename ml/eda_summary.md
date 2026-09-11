# 📊 Xóm Bank — Báo cáo Khám phá Dữ liệu (Phase 0 EDA Summary)

> **Mục tiêu:** Hiểu sâu cấu trúc dữ liệu giao dịch thẻ tín dụng Sparkov trước khi xây dựng pipeline streaming (Kafka) và batch warehouse (BigQuery + dbt).

---

## 1. Tổng quan Bộ dữ liệu

| Thông số | Tập Huấn luyện (`fraudTrain.csv`) | Tập Kiểm thử (`fraudTest.csv`) | Tổng cộng |
|---|---|---|---|
| **Số dòng** | 1,296,675 | 555,719 | **1,852,394** |
| **Số cột** | 22 | 22 | 22 |
| **Dung lượng file** | ~351 MB | ~150 MB | ~501 MB |
| **Khoảng thời gian** | `2019-01-01 00:00:18` → `2020-06-21 12:13:37` (~18 tháng) | `2020-06-21 12:14:25` → `2020-12-31 23:59:34` (~6 tháng) | **2 năm liên tục** |
| **Giá trị khuyết (Nulls)** | **0** (dữ liệu sạch hoàn toàn) | **0** | **0** |

---

## 2. Mức độ Mất cân bằng Nhãn (Class Imbalance)

- **Tập Train:** 7,506 giao dịch gian lận / 1,296,675 giao dịch (**0.5789%**)
- **Tập Test:** 2,145 giao dịch gian lận / 555,719 giao dịch (**0.3860%**)

> [!IMPORTANT]
> Tỷ lệ gian lận rất thấp (~0.58%), phản ánh đúng thực tế ngành ngân hàng.
> - **Metric chính cho ML:** Bắt buộc dùng **PR-AUC (Precision-Recall AUC)** và **Cost Function Matrix**, không dùng Accuracy hay AUC-ROC đơn thuần.

---

## 3. Thống kê Thực thể & Tiềm năng Star Schema

| Thực thể | Số lượng Unique (Train) | Ý nghĩa trong dbt Data Modeling |
|---|---|---|
| **Khách hàng (`cc_num`)** | 983 | Tách thành **`dim_customers`** (thông tin: `first`, `last`, `gender`, `street`, `city`, `state`, `zip`, `lat`, `long`, `job`, `dob`) |
| **Merchant (`merchant`)** | 693 | Tách thành **`dim_merchants`** (thông tin: `merchant`, `category`, `merch_lat`, `merch_long`) |
| **Danh mục (`category`)** | 14 | Thuộc tính của merchant |
| **Giao dịch (`trans_num`)** | 1,296,675 | Bảng sự kiện chính **`fct_transactions`** (`amt`, `unix_time`, `trans_date_trans_time`) |

---

## 4. Tín hiệu Phân biệt Gian lận (Fraud Signals)

### 4.1 Giá trị giao dịch (`amt`)
- **Giao dịch hợp lệ:** Trung bình **$67.67**, Trung vị **$47.28**, 95th percentile **$189.90**
- **Giao dịch gian lận:** Trung bình **$531.32**, Trung vị **$396.50**, 95th percentile **$1,083.99**
- 👉 **Insight:** Kẻ gian thường cố gắng quẹt các món tiền lớn gấp ~8-10 lần mức chi tiêu thông thường.

### 4.2 Nhóm danh mục có rủi ro cao nhất (`category`)
1. **`shopping_net`**: 1,713 gian lận / 97,543 giao dịch (**1.76%**)
2. **`misc_net`**: 915 gian lận / 63,287 giao dịch (**1.45%**)
3. **`grocery_pos`**: 1,743 gian lận / 123,638 giao dịch (**1.41%**)
4. **`shopping_pos`**: 843 gian lận / 116,672 giao dịch (**0.72%**)
- 👉 **Insight:** Các giao dịch trực tuyến qua mạng (`_net`) và siêu thị lớn (`grocery_pos`) là đích ngắm gian lận hàng đầu.
