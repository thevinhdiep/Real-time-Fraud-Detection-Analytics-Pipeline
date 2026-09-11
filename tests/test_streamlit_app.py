# -*- coding: utf-8 -*-
"""
Kiểm thử Ứng Dụng Web Streamlit (Phase 7)
"""

import os
import pytest
import pandas as pd
from streamlit_app.app import fetch_scored_transactions, get_inference_engine


class TestStreamlitApp:

    def test_fetch_scored_transactions(self):
        df = fetch_scored_transactions(limit=10)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "trans_num" in df.columns
            assert "fraud_score" in df.columns
            assert "decision" in df.columns

    def test_inference_engine_loaded(self):
        engine = get_inference_engine()
        assert engine is not None
        assert engine.model is not None
        assert engine.fe is not None
