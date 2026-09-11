# 💼 Báo Cáo Phân Tích Chiến Lược Chống Gian Lận — Xóm Bank

> **Cấu trúc báo cáo chuẩn Portfolio:** **`Số Liệu (Metrics) ➔ Ý Nghĩa Nghiệp Vụ (Business Meaning) ➔ Hành Động Đề Xuất (Actionable Strategy)`**  
> **Nguồn dữ liệu:** Google BigQuery Data Warehouse `xombank_dw_marts` (1.85 triệu giao dịch) & Mô hình AI LightGBM.

---

## 🎯 Insight 1: Quy Luật "Gian Lận Ban Đêm" (Night Frauds Pattern)

* **📊 Số liệu phân tích:**
  - Khoảng thời gian từ **22:00 đêm đến 04:00 sáng** chỉ chiếm **14.2%** tổng số lượng giao dịch, nhưng lại chiếm tới **73.5%** tổng số vụ gian lận được ghi nhận.
  - Tỷ lệ gian lận trong khung giờ 0h–3h sáng cao gấp **8.6 lần** so với khung giờ hành chính ban ngày (8h–17h).

* **💡 Ý nghĩa nghiệp vụ:**
  - Kẻ gian cố tình thực hiện các giao dịch trái phép vào lúc nạn nhân đang ngủ sâu để nạn nhân không đọc được tin nhắn SMS OTP/biến động số dư tức thời, từ đó trì hoãn thời gian báo khóa thẻ.

* **🚀 Hành động đề xuất (Actionable Strategy):**
  - **Dynamic 2FA / Sinh trắc học bắt buộc:** Tự động siết chặt chính sách bảo mật: Bất kỳ giao dịch online $> \$100$ phát sinh trong khung giờ 23:00 – 05:00 bắt buộc phải xác thực sinh trắc học khuôn mặt (FaceID) trên ứng dụng Mobile Banking thay vì chỉ gửi mã SMS thông thường.
  - **Hạ ngưỡng cảnh báo (Lower Threshold at Night):** Trong khung giờ đêm, hạ ngưỡng quyết định của LightGBM từ `0.75` xuống `0.60` để tăng tối đa độ nhạy (Recall) ngăn chặn tiền bị tẩu tán.

---

## 🎯 Insight 2: Mối Tương Quan Khoảng Cách Địa Lý (Geospatial Disparity)

* **📊 Số liệu phân tích:**
  - Khoảng cách địa lý trung bình giữa nơi ở của khách hàng và vị trí quẹt thẻ ở giao dịch hợp lệ là **`72.3 km`**.
  - Đối với các giao dịch gian lận, khoảng cách trung bình tăng vọt lên **`118.6 km`**.
  - Khi khoảng cách $> 100\text{ km}$, xác suất gian lận tăng gấp **4.2 lần**.

* **💡 Ý nghĩa nghiệp vụ:**
  - Gian lận POS/ATM thường xảy ra khi thẻ bị làm giả (skimming) và đem đi rút tiền/quẹt ở một bang hoặc thành phố khác trong khi chủ thẻ thực sự vẫn đang ở nhà.

* **🚀 Hành động đề xuất (Actionable Strategy):**
  - **Geo-fencing Smart Alerts:** So sánh vị trí GPS thời gian thực của thiết bị điện thoại khách hàng (Mobile App) với tọa độ máy POS. Nếu khoảng cách lệch nhau $> 50\text{ km}$ tại cùng thời điểm quẹt thẻ, hệ thống lập tức chuyển giao dịch sang trạng thái `ALERT` và gửi thông báo đẩy (Push Notification) yêu cầu khách hàng xác nhận "Có phải bạn đang quẹt thẻ tại [Tên Thành Phố] không?".

---

## 🎯 Insight 3: Điểm Dị Biệt Chi Tiêu Cá Nhân Hóa (Amount Z-Score Outliers)

* **📊 Số liệu phân tích:**
  - Giá trị trung bình một giao dịch hợp lệ là **`$67.20`**, trong khi giá trị trung bình một vụ gian lận là **`$530.20`** (gấp **7.8 lần**).
  - Khi chỉ số `amt_zscore_by_customer > +3.0` (số tiền quẹt vượt quá 3 lần độ lệch chuẩn so với lịch sử chi tiêu riêng của khách hàng đó), tỷ lệ gian lận đạt **89.4%**.

* **💡 Ý nghĩa nghiệp vụ:**
  - Kẻ gian khi chiếm đoạt được thẻ thường có tâm lý quẹt số tiền lớn nhất có thể trước khi thẻ bị khóa. Z-Score là chỉ số cá nhân hóa cực mạnh, phân biệt chính xác một khách hàng bình dân bỗng dưng mua đồ xa xỉ với một khách hàng thượng lưu quen mua sắm lớn.

* **🚀 Hành động đề xuất (Actionable Strategy):**
  - **Dynamic Spending Velocity Limit:** Thay vì áp dụng một hạn mức quẹt cứng cho mọi người (ví dụ cố định 50 triệu/ngày), ngân hàng nên áp dụng **Hạn mức Động dựa trên Z-score cá nhân**. Khi một giao dịch vượt quá 3 độ lệch chuẩn, hệ thống tự động tạm giữ trong 60 giây để xử lý kiểm tra bổ sung.

---

## 🎯 Insight 4: Danh Mục Ngành Hàng Rủi Ro Cao (High-Risk Merchant Categories)

* **📊 Số liệu phân tích:**
  - Hai danh mục chịu tổn thất tài chính nặng nề nhất là **Mua sắm Trực tuyến (`shopping_net`)** và **Tạp hóa / Đại siêu thị (`grocery_pos`)**, chiếm tổng cộng **61.8%** tổng thiệt hại gian lận toàn hệ thống.
  - Ngược lại, các danh mục như `food_dining` hay `gas_transport` có tần suất gian lận thấp hơn đáng kể và số tiền thiệt hại nhỏ.

* **💡 Ý nghĩa nghiệp vụ:**
  - Kẻ gian chuộng mua sắm online vì không cần thẻ vật lý và dễ mua các mặt hàng có tính thanh khoản cao (thẻ cào điện thoại, đồ điện tử, voucher quà tặng) để dễ tẩu tán thành tiền mặt.

* **🚀 Hành động đề xuất (Actionable Strategy):**
  - **Phân luồng thẩm định Merchant:** Đưa các sàn thương mại điện tử vào danh mục giám sát thời gian thực với Rule Engine chuyên biệt, yêu cầu Merchant Tokenization (3D-Secure 2.0).

---

## 💰 TỔNG KẾT TÁC ĐỘNG TÀI CHÍNH (FINANCIAL & ROI SUMMARY)

| Chỉ Số Đánh Giá | Không Có AI (Baseline) | Triển Khai Xóm Bank AI (Phase 5-7) | Hiệu Quả Đạt Được |
|---|---|---|---|
| **Số vụ gian lận bỏ sót (FN)** | 2,145 vụ/năm | **312 vụ/năm** | **Ngăn chặn 85.45% số vụ gian lận** |
| **Tổng thiệt hại thất thoát** | ~$1,072,500 | **~$156,000** | **Bảo vệ hơn $916,500 tiền của khách hàng** |
| **Số lần chặn nhầm khách thật (FP)** | ~18,500 cuộc gọi | **3,755 cuộc gọi** | **Giảm 80% áp lực tổng đài CSKH** |
| **Chi phí vận hành tối ưu** | $216,920 (ngưỡng 0.5) | **$212,325 (ngưỡng 0.75)** | **Tiết kiệm thêm $4,595 chi phí vận hành** |
