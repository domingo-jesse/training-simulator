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
        [data-testid="stAppViewContainer"] {background: linear-gradient(180deg, #F3F1F8 0%, #F8F7FC 100%);}
        .panel {border: 1px solid #E8ECF3; border-radius: 16px; padding: 1rem 1.1rem; background: #FFFFFF; margin-bottom: 0.8rem;}
        .module-card {border: 1px solid #E8ECF3; border-radius: 16px; padding: 1rem; background: #FDFEFF; min-height: 200px;}
        .subtle {color: #5B6472; font-size: 0.92rem;}
        .title-chip {display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;background:#EEF4FF;color:#325BCB;font-size:0.78rem;}
        .login-shell {max-width: 420px; margin: 0 auto 1rem auto; background: #FFFFFF; border: 1px solid #E8ECF3; border-radius: 18px; padding: 1.35rem 1.3rem 1.1rem 1.3rem; box-shadow: 0 8px 20px rgba(19, 19, 34, 0.06);}
        .login-logo {display: flex; justify-content: center; align-items: center; gap: 0.45rem; font-size: 1.95rem; font-weight: 700; color: #1F1F2E; margin-bottom: 0.2rem;}
        .login-logo .logo-badge {font-size: 1.1rem; line-height: 1; background: linear-gradient(135deg, #8B5CF6, #5B2DE2); color: #FFF; border-radius: 7px; padding: 0.2rem 0.32rem;}
        .login-subtitle {text-align: center; color: #24253A; margin-bottom: 1rem; font-weight: 600;}
        .login-link {text-align: center; font-size: 0.84rem; color: #6F46E8; font-weight: 600;}
        .login-muted {text-align: center; color: #72788B; font-size: 0.82rem; margin: 0.3rem 0;}
        .login-muted strong {color: #6F46E8;}
        .login-divider {display: flex; align-items: center; gap: 0.6rem; margin: 0.7rem 0 0.55rem 0; color: #9AA1B2; font-size: 0.8rem;}
        .login-divider::before, .login-divider::after {content: ""; flex: 1; border-top: 1px solid #E6EAF2;}
        .login-legal {text-align:center; color:#9AA1B2; font-size:0.73rem; margin-top:0.65rem;}
        .login-legal strong {color:#5C6478; font-weight:600;}
        .login-card [data-testid="stTextInput"] input {
            border-radius: 8px;
            border: 1px solid #DBE1EC;
            height: 2.55rem;
        }
        .login-card [data-testid="stButton"] button {
            border-radius: 8px;
            height: 2.6rem;
            font-weight: 600;
        }
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
