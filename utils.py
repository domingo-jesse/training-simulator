from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd
import streamlit as st


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .main .block-container {padding-top: 1.8rem; padding-bottom: 2.2rem; max-width: 1200px;}
        .panel {border: 1px solid #E8ECF3; border-radius: 16px; padding: 1rem 1.1rem; background: #FFFFFF; margin-bottom: 0.8rem;}
        .module-card {border: 1px solid #E8ECF3; border-radius: 16px; padding: 1rem; background: #FDFEFF; min-height: 200px;}
        .subtle {color: #5B6472; font-size: 0.92rem;}
        .title-chip {display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;background:#EEF4FF;color:#325BCB;font-size:0.78rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_row(items: Dict[str, Any]) -> None:
    cols = st.columns(len(items))
    for i, (label, value) in enumerate(items.items()):
        cols[i].metric(label, value)


def to_df(rows) -> pd.DataFrame:
    return pd.DataFrame([dict(r) for r in rows])


def parse_json_list(value: str):
    if not value:
        return []
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return [value]
