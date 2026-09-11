# 🎨 Xóm Bank — Power BI Dashboard Visual Design Specification

> **Theme:** Executive Banking Modern Dark / Light Palette  
> **Chủ đạo màu sắc:**  
> - 🟦 Navy Deep Blue (`#1E3799`): Màu chủ đạo, đại diện ngân hàng  
> - 🔴 Crimson Red (`#EB4D4B`): Chỉ số gian lận, rủi ro, thiệt hại tài chính  
> - 🟢 Forest Green (`#6AB04C`): Giao dịch an toàn, doanh số hợp lệ  
> - 🟡 Amber Orange (`#F0932B`): Cảnh báo, độ lệch chuẩn cao  

---

## 📑 TRANG 1: EXECUTIVE OVERVIEW (Tổng Quan Điều Hành)

**Mục tiêu:** Cung cấp cho Ban Lãnh đạo bức tranh toàn cảnh về khối lượng giao dịch, tổng thiệt hại gian lận, tỷ lệ rủi ro và xu hướng biến động theo thời gian.

```
+---------------------------------------------------------------------------------------------------+
|  [Logo] XÓM BANK — FRAUD ANALYTICS (EXECUTIVE OVERVIEW)                   [Slicer: Date | State]  |
+---------------------------------------------------------------------------------------------------+
|  [ Card 1 ]          |  [ Card 2 ]          |  [ Card 3 ]          |  [ Card 4 ]                  |
|  Total Volume        |  Total Fraud Loss    |  Fraud Rate (%)      |  Avg Fraud Amount            |
|  $158.4M             |  $924.5K             |  0.58%               |  $530.20                     |
+---------------------------------------------------------------------------------------------------+
|  [ Line & Clustered Column Chart ]                  |  [ Horizontal Bar Chart ]                   |
|  Monthly Transaction Volume vs Fraud Loss           |  Fraud Rate & Fraud Loss by Category        |
|  - Trục X: Tháng (trans_timestamp yyyy-MM)          |  - Trục Y: Category                         |
|  - Cột: Total Amount ($)                            |  - Cột: Fraud Rate (%)                      |
|  - Đường: Fraud Rate (%)                            |  - Tooltip: Total Fraud Amount              |
+---------------------------------------------------------------------------------------------------+
|  [ Donut Chart ]                                    |  [ KPI Highlight Card ]                     |
|  Legit vs Fraud Transactions Distribution           |  Ngân sách rủi ro đã ngăn chặn:             |
|  - Xanh: Legit (99.42%)                             |  💰 $790,000 (Recall 85.45%)                |
|  - Đỏ: Fraud (0.58%)                                |  ⚡ Giảm 3,673 vụ chặn nhầm                 |
+---------------------------------------------------------------------------------------------------+
```

### Visuals & Measures sử dụng:
1. **Thẻ KPI 1–4:** `[Total Transaction Amount]`, `[Total Fraud Amount]`, `[Fraud Rate %]`, `[Avg Fraud Amount]`.
2. **Biểu đồ Monthly Trend:** Phân tích biến động qua các tháng (chỉ ra đợt tăng đột biến trong mua sắm cuối năm).
3. **Biểu đồ Category Fraud:** Xếp hạng các ngành hàng chịu tổn thất lớn nhất (Mua sắm online `shopping_net`, Tạp hóa `grocery_pos`).

---

## 📑 TRANG 2: MERCHANT CATEGORY & GEOSPATIAL ANALYSIS (Ngành Hàng & Địa Lý)

**Mục tiêu:** Nhận diện các "điểm nóng" (hotspots) về địa lý và các danh mục ngành hàng có tỷ lệ gian lận cao bất thường.

```
+---------------------------------------------------------------------------------------------------+
|  [Header] PHÂN TÍCH RỦI RO THEO NGÀNH HÀNG & ĐỊA LÝ                       [Slicer: Category]      |
+---------------------------------------------------------------------------------------------------+
|  [ Map Visual (Bản đồ nhiệt Hoa Kỳ) ]               |  [ Clustered Bar Chart ]                    |
|  State-level Fraud Loss Density Map                 |  Fraud Loss by Distance Range (Haversine)   |
|  - Location: dim_customers[state]                   |  - < 10 km (Giao dịch lân cận)              |
|  - Bubble Size: [Total Fraud Amount]                |  - 10 - 50 km                               |
|  - Color Saturation: [Fraud Rate %]                 |  - 50 - 100 km                              |
|                                                     |  - > 100 km (Cách xa bất thường)            |
+---------------------------------------------------------------------------------------------------+
|  [ Matrix Table / Grid ]                                                                          |
|  Top 10 High-Risk Merchants & Categories                                                          |
|  - Merchant Name | Category | Total Txns | Fraud Txns | Fraud Rate % | Total Loss ($)             |
+---------------------------------------------------------------------------------------------------+
```

### Visuals & Measures sử dụng:
1. **Power BI Map:** Thể hiện phân bố gian lận theo các tiểu bang của Mỹ (State-level).
2. **Distance Range Analysis:** Chứng minh quy luật: *Khi khoảng cách giữa nơi ở và điểm quẹt thẻ > 100km, tỷ lệ gian lận tăng gấp 4.2 lần*.
3. **Top High-Risk Merchant Table:** Xếp hạng các merchant có tỷ lệ chargeback cao cần đưa vào danh sách theo dõi đặc biệt (Watchlist).

---

## 📑 TRANG 3: DEMOGRAPHICS & BEHAVIORAL PROFILING (Nhân Khẩu Học & Hành Vi)

**Mục tiêu:** Phân tích các đặc điểm nhân khẩu học của nạn nhân và thói quen hành vi quẹt thẻ.

```
+---------------------------------------------------------------------------------------------------+
|  [Header] HỒ SƠ NHÂN KHẨU HỌC & HÀNH VI GIAN LẬN                          [Slicer: Age Group]     |
+---------------------------------------------------------------------------------------------------+
|  [ 100% Stacked Column Chart ]                      |  [ Line Chart: Hour of Day ]                |
|  Fraud Rate by Age Group & Gender                   |  Fraud Incidents by Hour (0h - 23h)         |
|  - Trục X: Age Group (<25, 25-35, 36-50, 51-65, >65)|  - Trục X: trans_hour (0 -> 23)             |
|  - Legend: Gender (M / F)                           |  - Đường: Total Fraud Transactions          |
|  - Giá trị: [Fraud Rate %]                          |  - Highlight: Vùng ban đêm 0h - 5h sáng     |
+---------------------------------------------------------------------------------------------------+
|  [ Scatter Plot (Ma Trận Bất Thường) ]              |  [ Gauge / Card Metric ]                    |
|  Transaction Amount vs Amount Z-Score               |  Tỷ lệ gian lận ban đêm:                    |
|  - Trục X: amt ($)                                  |  🌙 73.5% vụ gian lận tập trung 22h - 4h   |
|  - Trục Y: amt_zscore_by_customer                   |                                             |
|  - Color: is_fraud (0: Xanh, 1: Đỏ)                 |  Tỷ lệ gian lận cuối tuần:                  |
|                                                     |  📅 Cao hơn ngày thường 18.4%              |
+---------------------------------------------------------------------------------------------------+
```

### Visuals & Measures sử dụng:
1. **Hour of Day Line Chart:** Chỉ rõ hiện tượng "Gian lận ban đêm" (Night Frauds) — kẻ gian quẹt thẻ vào khung giờ nạn nhân đang ngủ.
2. **Amount vs Z-Score Scatter:** Phân tách rõ ràng các điểm dị biệt (Outliers) có Z-Score $> +3.0$.
3. **Age Group Analysis:** Chỉ ra nhóm tuổi trung niên và người cao tuổi có số tiền mất mát trên mỗi vụ cao hơn đáng kể.
