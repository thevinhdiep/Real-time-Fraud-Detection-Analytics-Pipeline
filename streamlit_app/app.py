# -*- coding: utf-8 -*-
"""
Xóm Bank — Dashboard Phát Hiện Gian Lận & Giám Sát Rủi Ro Thời Gian Thực (Phase 7)
==================================================================================
Ứng dụng web tương tác được xây dựng bằng Streamlit:
1. Live Alert Feed: Giám sát luồng streaming Kafka thời gian thực & truy vấn DuckDB
2. Transaction Deep Lookup: Hồ sơ phân tích rủi ro khách hàng & giao dịch 360 độ
3. Risk Simulator: Thử nghiệm sandbox tức thì với mô hình sản xuất LightGBM
4. System Architecture & Model Card: Kiến trúc hệ thống & thẻ báo cáo mô hình
"""

import os
import sys
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import time
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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

from ml.feature_engineering import FraudFeatureEngineer, haversine_distance_km
from consumers.consumer_realtime_inference import RealtimeInferenceEngine

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    from streamlit_autorefresh import st_autorefresh
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False


# =====================================================================
# Cấu hình Trang Streamlit
# =====================================================================
st.set_page_config(
    page_title="Xóm Bank — Real-time Fraud Analytics",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS tùy biến giao diện ngân hàng hiện đại
st.markdown("""
<style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 800;
        color: #1e3799;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #4a69bd;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 10px;
        padding: 15px;
        border-left: 5px solid #1e3799;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .badge-block {
        background-color: #eb4d4b;
        color: white;
        padding: 3px 8px;
        border-radius: 5px;
        font-weight: bold;
    }
    .badge-alert {
        background-color: #f0932b;
        color: white;
        padding: 3px 8px;
        border-radius: 5px;
        font-weight: bold;
    }
    .badge-approve {
        background-color: #6ab04c;
        color: white;
        padding: 3px 8px;
        border-radius: 5px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


# =====================================================================
# Các Hàm Tiện Ích Truy Xuất Cơ Sở Dữ Liệu
# =====================================================================
@st.cache_resource
def get_inference_engine():
    """Tải inference engine mô hình một lần vào bộ nhớ cache."""
    try:
        return RealtimeInferenceEngine(
            model_path=os.path.join(PROJECT_ROOT, "ml", "models", "best_fraud_model.joblib"),
            fe_path=os.path.join(PROJECT_ROOT, "ml", "models", "feature_engineer.joblib"),
            metadata_path=os.path.join(PROJECT_ROOT, "ml", "models", "model_metadata.json"),
            db_path=os.path.join(PROJECT_ROOT, "data", "fraud_detection.duckdb")
        )
    except Exception as e:
        st.error(f"Lỗi khởi tạo RealtimeInferenceEngine: {e}")
        return None


def fetch_scored_transactions(limit: int = 500) -> pd.DataFrame:
    """Lấy các giao dịch vừa được chấm điểm từ DuckDB hoặc dự phòng SQLite."""
    duckdb_path = os.path.join(PROJECT_ROOT, "data", "fraud_detection.duckdb")
    sqlite_path = os.path.join(PROJECT_ROOT, "data", "fraud_detection.sqlite")

    df_duck = pd.DataFrame()
    df_sqlite = pd.DataFrame()

    if DUCKDB_AVAILABLE and os.path.exists(duckdb_path):
        try:
            conn = duckdb.connect(duckdb_path, read_only=True)
            df_duck = conn.execute(f"""
                SELECT * FROM fraud_scores 
                ORDER BY processed_at DESC 
                LIMIT {limit}
            """).df()
            conn.close()
        except Exception:
            pass

    if os.path.exists(sqlite_path):
        try:
            conn = sqlite3.connect(sqlite_path)
            df_sqlite = pd.read_sql_query(f"""
                SELECT * FROM fraud_scores 
                ORDER BY processed_at DESC 
                LIMIT {limit}
            """, conn)
            conn.close()
        except Exception:
            pass

    if not df_duck.empty and not df_sqlite.empty:
        combined = pd.concat([df_duck, df_sqlite], ignore_index=True)
        res = combined.drop_duplicates(subset=["trans_num"])
    elif not df_duck.empty:
        res = df_duck
    elif not df_sqlite.empty:
        res = df_sqlite
    else:
        res = pd.DataFrame()

    if not res.empty:
        if "processed_at" in res.columns:
            res["processed_at"] = pd.to_datetime(res["processed_at"], errors="coerce", utc=True)
            res = res.sort_values("processed_at", ascending=False).head(limit)
        for num_col in ["amt", "fraud_score", "latency_ms", "geo_distance_km", "amt_zscore_by_customer"]:
            if num_col in res.columns:
                res[num_col] = pd.to_numeric(res[num_col], errors="coerce").fillna(0.0)
        if "txn_freq_24h" in res.columns:
            res["txn_freq_24h"] = pd.to_numeric(res["txn_freq_24h"], errors="coerce").fillna(0).astype(int)

    return res


# =====================================================================
# Thanh Điều Khiển Bên Hông (Sidebar Controls)
# =====================================================================
with st.sidebar:
    st.image("https://img.icons8.com/color/96/bank-building.png", width=64)
    st.markdown("### 🏦 **Xóm Bank Fraud Ops**")
    st.markdown("Hệ thống phát hiện gian lận & giám sát giao dịch thẻ thời gian thực.")
    st.divider()

    st.markdown("#### ⚙️ **Cấu hình Trực tiếp**")
    auto_refresh = st.checkbox("Bật Tự động làm mới (Auto-Refresh)", value=True)
    refresh_interval = st.slider("Tần suất làm mới (giây)", min_value=2, max_value=20, value=4, step=1)

    if auto_refresh and AUTOREFRESH_AVAILABLE:
        st_autorefresh(interval=refresh_interval * 1000, key="fraud_autorefresh")

    st.divider()
    st.markdown("#### 🎯 **Ngưỡng Quyết định (Policy)**")
    st.markdown(r"• **BLOCK (Chặn):** Score $\ge$ 85.0%")
    st.markdown(r"• **ALERT (Báo động):** Score $\ge$ 75.0%")
    st.markdown(r"• **APPROVE (Phê duyệt):** Score < 75.0%")
    st.divider()
    st.caption("Engine: LightGBM (GCP BigQuery + dbt Star Schema)")


# =====================================================================
# Nội Dung Chính Của Ứng Dụng
# =====================================================================
st.markdown('<div class="main-title">🏦 Xóm Bank — Real-time Fraud Detection Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Giám sát rủi ro thẻ tín dụng tức thời • Kafka Streaming • LightGBM Inference • DuckDB Store</div>', unsafe_allow_html=True)

# Lấy dữ liệu mới nhất
df_data = fetch_scored_transactions(limit=1000)

# Tính toán các chỉ số KPI hàng đầu
total_txns = len(df_data)
if total_txns > 0:
    fraud_alerts = len(df_data[df_data["decision"].isin(["BLOCK", "ALERT"])])
    blocked_txns = len(df_data[df_data["decision"] == "BLOCK"])
    fraud_rate = (fraud_alerts / total_txns) * 100
    avg_latency = df_data["latency_ms"].mean()
    total_volume = df_data["amt"].sum()
else:
    fraud_alerts = 0
    blocked_txns = 0
    fraud_rate = 0.0
    avg_latency = 0.0
    total_volume = 0.0

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric(label="💳 Tổng Giao Dịch", value=f"{total_txns:,}")
with col2:
    st.metric(label="🚨 Cảnh Báo Gian Lận", value=f"{fraud_alerts:,}", delta=f"{blocked_txns} Chặn Tức Thì", delta_color="inverse")
with col3:
    st.metric(label="📊 Tỷ Lệ Rủi Ro", value=f"{fraud_rate:.2f}%")
with col4:
    st.metric(label="⚡ Độ Trễ (Latency)", value=f"{avg_latency:.2f} ms")
with col5:
    st.metric(label="💰 Tổng Doanh Số ($)", value=f"${total_volume:,.2f}")

st.divider()

# Các Tab Điều Hướng
tab1, tab2, tab3, tab4 = st.tabs([
    "🚨 Live Alert Feed",
    "🔍 Tra Cứu Tức Thời (Deep Lookup)",
    "🧪 Risk Simulator (Thử Nghiệm)",
    "📈 Kiến Trúc & Model Card"
])


# =====================================================================
# TAB 1: LIVE ALERT FEED
# =====================================================================
with tab1:
    st.subheader("📡 Luồng Giao Dịch Thời Gian Thực (Live Stream)")

    if df_data.empty:
        st.info("💡 Chưa có dữ liệu giao dịch trong DuckDB. Bạn hãy bật Consumer Group B để bắt đầu stream:")
        st.code("python consumers/consumer_realtime_inference.py --stream-from-csv data/fraudTest.csv --limit 100", language="bash")
    else:
        # Bộ điều khiển lọc giao dịch
        f_col1, f_col2 = st.columns([2, 2])
        with f_col1:
            decision_filter = st.multiselect(
                "Lọc theo Quyết định:",
                options=["ALL", "BLOCK", "ALERT", "APPROVE"],
                default=["ALL"]
            )
        with f_col2:
            amt_min = st.slider("Số tiền tối thiểu ($):", 0, int(df_data["amt"].max()) if not df_data.empty else 1000, 0)

        df_filtered = df_data.copy()
        if "ALL" not in decision_filter and decision_filter:
            df_filtered = df_filtered[df_filtered["decision"].isin(decision_filter)]
        if amt_min > 0:
            df_filtered = df_filtered[df_filtered["amt"] >= amt_min]

        # Hàng biểu đồ trực quan
        chart_col1, chart_col2 = st.columns([3, 2])

        with chart_col1:
            st.markdown("##### 📈 Phân Bố Điểm Rủi Ro Theo Thời Gian")
            fig_timeline = px.scatter(
                df_filtered.head(100),
                x="processed_at",
                y="fraud_score",
                color="decision",
                size="amt",
                color_discrete_map={"BLOCK": "#eb4d4b", "ALERT": "#f0932b", "APPROVE": "#6ab04c"},
                hover_data=["trans_num", "cc_num", "category", "geo_distance_km"],
                title="Fraud Risk Score Distribution (Top 100 giao dịch mới nhất)"
            )
            fig_timeline.add_hline(y=0.75, line_dash="dash", line_color="#f0932b", annotation_text="Ngưỡng Alert (0.75)")
            fig_timeline.add_hline(y=0.85, line_dash="dot", line_color="#eb4d4b", annotation_text="Ngưỡng Block (0.85)")
            fig_timeline.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=320)
            st.plotly_chart(fig_timeline, use_container_width=True)

        with chart_col2:
            st.markdown("##### 🍩 Tỷ Trọng Quyết Định Hệ Thống")
            decision_counts = df_data["decision"].value_counts().reset_index()
            decision_counts.columns = ["Decision", "Count"]
            fig_donut = px.pie(
                decision_counts,
                values="Count",
                names="Decision",
                hole=0.5,
                color="Decision",
                color_discrete_map={"BLOCK": "#eb4d4b", "ALERT": "#f0932b", "APPROVE": "#6ab04c"}
            )
            fig_donut.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=320)
            st.plotly_chart(fig_donut, use_container_width=True)

        # Bảng dữ liệu giao dịch chi tiết
        st.markdown("##### 📋 Danh Sách Giao Dịch Chi Tiết")
        display_df = df_filtered[[
            "processed_at", "decision", "trans_num", "cc_num", "amt",
            "category", "merchant", "geo_distance_km", "fraud_score", "latency_ms"
        ]].copy()
        
        display_df["fraud_score"] = display_df["fraud_score"].apply(lambda s: f"{s*100:.1f}%")
        display_df["amt"] = display_df["amt"].apply(lambda a: f"${a:,.2f}")
        display_df["geo_distance_km"] = display_df["geo_distance_km"].apply(lambda d: f"{d:.1f} km")
        display_df["latency_ms"] = display_df["latency_ms"].apply(lambda l: f"{l:.2f} ms")

        st.dataframe(display_df, use_container_width=True, height=350)


# =====================================================================
# TAB 2: TRA CỨU HỒ SƠ RỦI RO GIAO DỊCH
# =====================================================================
with tab2:
    st.subheader("🔍 Tra Cứu Hồ Sơ Rủi Ro Giao Dịch (Risk Profiler)")

    if df_data.empty:
        st.info("Chưa có giao dịch để tra cứu.")
    else:
        trans_options = df_data["trans_num"].tolist()
        selected_trans = st.selectbox("Chọn Mã Giao Dịch (trans_num) cần kiểm tra:", options=trans_options)

        txn_record = df_data[df_data["trans_num"] == selected_trans].iloc[0]

        res_col1, res_col2, res_col3 = st.columns([1.5, 2, 1.5])

        with res_col1:
            st.markdown("##### 👤 Thông Tin Khách Hàng")
            st.markdown(f"• **Số Thẻ (Masked):** `{str(txn_record['cc_num'])[:4]} **** **** {str(txn_record['cc_num'])[-4:]}`")
            st.markdown(f"• **Địa Điểm Thường Trú:** `{txn_record['city']}, {txn_record['state']}`")
            st.markdown(f"• **Thời Gian Giao Dịch:** `{txn_record['trans_date_trans_time']}`")
            st.markdown(f"• **Danh Mục Giao Dịch:** `{txn_record['category']}`")
            st.markdown(f"• **Đối Tác Cửa Hàng:** `{txn_record['merchant']}`")
            st.markdown(f"• **Số Tiền:** **${txn_record['amt']:,.2f}**")

        with res_col2:
            st.markdown("##### 🧭 Phân Tích Hành Vi & Địa Lý")
            st.markdown(f"• **Khoảng Cách Địa Lý:** **`{txn_record['geo_distance_km']:.1f} km`** (Khoảng cách giữa nơi ở và merchant)")
            st.markdown(f"• **Tần Suất 24h:** **`{txn_record['txn_freq_24h']} giao dịch`** trong 24 giờ trước đó")
            st.markdown(f"• **Độ Lệch Chuẩn Chi Tiêu (Z-Score):** **`{txn_record['amt_zscore_by_customer']:.2f}`** (Độ bất thường so với ví khách)")
            st.markdown(f"• **Thời Gian Xử Lý:** `{txn_record['latency_ms']:.2f} ms`")

            # Đánh giá các yếu tố rủi ro
            st.markdown("##### 🚨 Đánh Giá Yếu Tố Rủi Ro (Risk Signals)")
            if txn_record["geo_distance_km"] > 100:
                st.error(f"🚩 Vị trí giao dịch cách xa nhà ở bất thường ({txn_record['geo_distance_km']:.1f} km > 100 km)!")
            else:
                st.success(f"✅ Vị trí giao dịch trong phạm vi thông thường ({txn_record['geo_distance_km']:.1f} km).")

            if abs(txn_record["amt_zscore_by_customer"]) > 2.0:
                st.warning(f"🚩 Giá trị giao dịch lệch hơn 2 độ lệch chuẩn so với thói quen chi tiêu!")

        with res_col3:
            st.markdown("##### 🎯 Đánh Giá Của Model (LightGBM)")
            score_pct = txn_record["fraud_score"] * 100
            
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=score_pct,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': f"Xác Suất Gian Lận: {txn_record['decision']}", 'font': {'size': 14}},
                number={'suffix': "%"},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "#2c3e50"},
                    'steps': [
                        {'range': [0, 75], 'color': "#6ab04c"},
                        {'range': [75, 85], 'color': "#f0932b"},
                        {'range': [85, 100], 'color': "#eb4d4b"}
                    ],
                    'threshold': {
                        'line': {'color': "black", 'width': 3},
                        'thickness': 0.75,
                        'value': score_pct
                    }
                }
            ))
            fig_gauge.update_layout(height=240, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_gauge, use_container_width=True)


# =====================================================================
# TAB 3: HỘP CÁT THỬ NGHIỆM MÔ HÌNH (RISK SIMULATOR)
# =====================================================================
with tab3:
    st.subheader("🧪 Sandbox Thử Nghiệm Mô Hình (Real-time Risk Simulator)")
    st.markdown("Nhập các thông số giao dịch giả lập để kiểm tra khả năng suy luận tức thời của mô hình:")

    engine = get_inference_engine()

    sim_col1, sim_col2, sim_col3 = st.columns(3)

    with sim_col1:
        sim_amt = st.number_input("Số Tiền Giao Dịch ($):", min_value=1.0, max_value=10000.0, value=850.0, step=10.0)
        sim_category = st.selectbox("Danh Mục Cửa Hàng:", options=FraudFeatureEngineer.CATEGORIES, index=11)
        sim_hour = st.slider("Giờ Giao Dịch (0 - 23h):", min_value=0, max_value=23, value=2)

    with sim_col2:
        sim_cust_lat = st.number_input("Tọa độ Khách (Vĩ độ - Lat):", value=32.7767, format="%.4f")
        sim_cust_long = st.number_input("Tọa độ Khách (Kinh độ - Long):", value=-96.7970, format="%.4f")
        sim_age = st.number_input("Tuổi Khách Hàng:", min_value=18, max_value=100, value=35)

    with sim_col3:
        sim_merch_lat = st.number_input("Tọa độ Cửa Hàng (Lat):", value=38.8951, format="%.4f")
        sim_merch_long = st.number_input("Tọa độ Cửa Hàng (Long):", value=-77.0364, format="%.4f")
        sim_gender = st.selectbox("Giới Tính Khách:", options=["M", "F"], index=0)

    if st.button("🚀 Chạy Kiểm Tra Rủi Ro Tức Thì (Instant Score)", type="primary"):
        if engine:
            sim_txn = {
                "trans_num": f"sim_{int(time.time())}",
                "trans_date_trans_time": f"2026-08-22 {sim_hour:02d}:30:00",
                "unix_time": int(time.time()),
                "cc_num": 999111222,
                "merchant": "fraud_SimulatedMerchant",
                "category": sim_category,
                "amt": sim_amt,
                "first": "Sim",
                "last": "User",
                "gender": sim_gender,
                "street": "Sim Street",
                "city": "SimCity",
                "state": "TX",
                "zip": 75001,
                "lat": sim_cust_lat,
                "long": sim_cust_long,
                "city_pop": 500000,
                "job": "Tester",
                "dob": f"{datetime.now().year - sim_age}-01-01",
                "merch_lat": sim_merch_lat,
                "merch_long": sim_merch_long,
                "is_fraud": 0
            }

            res = engine.process_transaction(sim_txn)

            st.success("✅ Đã hoàn tất suy luận trong " + str(res["latency_ms"]) + " ms!")
            
            sc_col1, sc_col2, sc_col3 = st.columns(3)
            with sc_col1:
                st.metric("Xác Suất Gian Lận", f"{res['fraud_score']*100:.1f}%")
            with sc_col2:
                decision_badge = "🔴 BLOCK (CHẶN)" if res["decision"] == "BLOCK" else ("🟡 ALERT (BÁO ĐỘNG)" if res["decision"] == "ALERT" else "🟢 APPROVE (HỢP LỆ)")
                st.metric("Quyết Định Hệ Thống", decision_badge)
            with sc_col3:
                st.metric("Khoảng Cách Địa Lý", f"{res['geo_distance_km']:.1f} km")


# =====================================================================
# TAB 4: KIẾN TRÚC HỆ THỐNG & MODEL CARD
# =====================================================================
with tab4:
    st.subheader("📈 Kiến Trúc Hệ Thống & Báo Cáo Huấn Luyện (Model Card)")

    st.markdown("""
    ### 🏗️ Kiến Trúc 2 Nhánh (Dual-Stream Pipeline):
    - **Nhánh A (Batch Warehouse):** Kafka ➔ MinIO Parquet ➔ BigQuery Batch Load ➔ dbt Star Schema (`dim_customers`, `dim_merchants`, `fct_transactions_fraud_features`).
    - **Nhánh B (Real-time Serving):** Kafka ➔ Python Consumer Group B (`cg-realtime-inference`) ➔ LightGBM In-Memory ➔ DuckDB Local Store.
    """)

    # Nạp Model Metadata nếu tồn tại
    meta_path = os.path.join(PROJECT_ROOT, "ml", "models", "model_metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        m_col1, m_col2 = st.columns(2)
        with m_col1:
            st.markdown("#### 🎯 Thông Số Mô Hình Tuyển Chọn")
            st.markdown(f"• **Mô hình Tốt Nhất:** `{metadata.get('model_name', 'LightGBM')}`")
            st.markdown(f"• **PR-AUC (Holdout 555k test set):** `{metadata['holdout_test_metrics']['pr_auc']:.4f}`")
            st.markdown(f"• **ROC-AUC:** `{metadata['holdout_test_metrics']['roc_auc']:.4f}`")
            st.markdown(f"• **Ngưỡng Tối Ưu Chi Phí:** **`{metadata.get('optimal_threshold', 0.75):.4f}`**")

        with m_col2:
            st.markdown("#### 💰 Hiệu Quả Kinh Doanh (Cost Matrix)")
            st.markdown("• **Chi phí Bỏ lọt Gian lận (FN):** `$500 / vụ`")
            st.markdown("• **Chi phí Chặn nhầm Khách thật (FP):** `$15 / vụ`")
            st.markdown(f"• **Tiết Kiệm Chi Phí Vận Hành:** **`${metadata['holdout_test_metrics']['cost_savings']:,.2f}`** ({metadata['holdout_test_metrics']['cost_savings_pct']:.1f}% giảm chi phí)")
