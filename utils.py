from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd
import streamlit as st


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --shell-bg: #d9d9d9;
            --surface: #ececec;
            --card: #f5f5f5;
            --line: #d8d8d8;
            --text: #161616;
            --muted: #6a6a6a;
            --accent: #1c1c1c;
        }
        .stApp {
            background: var(--shell-bg);
            color: var(--text);
        }
        [data-testid="stAppViewContainer"] {
            background: var(--shell-bg);
        }
        .main .block-container {
            padding-top: 1.35rem;
            padding-bottom: 2rem;
            max-width: 1280px;
        }
        [data-testid="stSidebar"] {
            background: var(--surface);
            border-right: 1px solid var(--line);
        }
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
            color: var(--muted);
        }
        [data-testid="stSidebar"] [role="radiogroup"] {
            gap: 0.25rem;
        }
        [data-testid="stSidebar"] [role="radiogroup"] > label {
            border: 1px solid transparent;
            border-radius: 12px;
            background: transparent;
            padding: 0.38rem 0.6rem;
        }
        [data-testid="stSidebar"] [role="radiogroup"] > label:has(input:checked) {
            background: #ffffff;
            border-color: var(--line);
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
        }
        [data-testid="stMetric"] {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 0.9rem 1rem;
            min-height: 115px;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.45);
        }
        [data-testid="stMetricLabel"] p {
            color: #4b4b4b;
            font-weight: 600;
        }
        [data-testid="stMetricValue"] {
            color: #181818;
            letter-spacing: -0.02em;
        }
        [data-testid="stVerticalBlock"] [data-testid="stDataFrame"],
        [data-testid="stVerticalBlock"] [data-testid="stExpander"],
        [data-testid="stVerticalBlock"] [data-testid="stAlert"],
        [data-testid="stVerticalBlock"] [data-testid="stForm"] {
            border-radius: 16px;
        }
        [data-testid="stButton"] button,
        [data-testid="stDownloadButton"] button {
            border-radius: 999px;
            border: 1px solid var(--line);
            background: #ffffff;
            color: var(--text);
            font-weight: 600;
        }
        [data-testid="stButton"] button[kind="primary"] {
            background: var(--accent);
            color: #ffffff;
            border-color: #2c2c2c;
        }
        [data-testid="stTextInput"] input,
        [data-testid="stSelectbox"] > div > div,
        [data-testid="stDateInput"] input {
            border-radius: 999px;
            border: 1px solid var(--line);
            background: #ffffff;
        }
        .dashboard-shell {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 22px;
            padding: 0.95rem 1.1rem 1.05rem 1.1rem;
            margin-bottom: 1rem;
        }
        .dashboard-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
        }
        .dashboard-title {
            font-size: 2.1rem;
            font-weight: 700;
            line-height: 1.15;
            margin: 0;
            color: var(--text);
            letter-spacing: -0.03em;
        }
        .dashboard-subtitle {
            margin: 0.25rem 0 0;
            color: var(--muted);
            font-size: 0.96rem;
        }
        .profile-chip {
            background: #ffffff;
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            font-size: 0.88rem;
            color: #3d3d3d;
            white-space: nowrap;
        }
        .panel {border: 1px solid #E1E1E1; border-radius: 18px; padding: 1rem 1.1rem; background: #F5F5F5; margin-bottom: 0.8rem;}
        .module-card {border: 1px solid #E1E1E1; border-radius: 18px; padding: 1rem; background: #F7F7F7; min-height: 200px;}
        .subtle {color: #5B6472; font-size: 0.92rem;}
        .title-chip {display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;background:#ffffff;color:#303030;font-size:0.78rem;border:1px solid #D8D8D8;}
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
