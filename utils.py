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
        .stApp,
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(145deg, #f8fafc, #f1f5f9 55%, #ffffff);
            color: #0f172a;
        }
        [data-testid="stSidebar"] > div:first-child {
            background: linear-gradient(180deg, #ffffff, #f8fafc);
            border-right: 1px solid rgba(15, 23, 42, 0.08);
        }
        .stApp [data-testid="stMarkdownContainer"],
        .stApp label,
        .stApp p,
        .stApp .st-emotion-cache-10trblm,
        .stApp .st-emotion-cache-16idsys {
            color: #0f172a;
        }

        .panel,
        .module-card,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stDataFrame"],
        .stTabs [data-baseweb="tab-panel"] {
            background: rgba(255, 255, 255, 0.88);
            backdrop-filter: blur(6px);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 12px;
            padding: 16px;
        }
        .subtle {color: #475569; font-size: 0.92rem;}
        .title-chip {display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;background:rgba(59,130,246,0.14);color:#1d4ed8;font-size:0.78rem;}
        .login-shell {
            max-width: 420px;
            margin: 0 auto 1rem auto;
            background: rgba(255, 255, 255, 0.06);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 18px;
            padding: 1.35rem 1.3rem 1.1rem 1.3rem;
            box-shadow: 0 8px 24px rgba(2, 6, 23, 0.25);
            backdrop-filter: blur(10px);
        }
        .login-logo {display: flex; justify-content: center; align-items: center; gap: 0.45rem; font-size: 1.95rem; font-weight: 700; color: #f8fafc; margin-bottom: 0.2rem;}
        .login-logo .logo-badge {font-size: 1.1rem; line-height: 1; background: linear-gradient(135deg, #3b82f6, #6366f1); color: #FFF; border-radius: 7px; padding: 0.2rem 0.32rem;}
        .login-subtitle {text-align: center; color: #dbeafe; margin-bottom: 1rem; font-weight: 600;}
        .login-link {text-align: center; font-size: 0.84rem; color: #bfdbfe; font-weight: 600;}
        .login-muted {text-align: center; color: #cbd5e1; font-size: 0.82rem; margin: 0.3rem 0;}
        .login-muted strong {color: #bfdbfe;}
        .login-divider {display: flex; align-items: center; gap: 0.6rem; margin: 0.7rem 0 0.55rem 0; color: #cbd5e1; font-size: 0.8rem;}
        .login-divider::before, .login-divider::after {content: ""; flex: 1; border-top: 1px solid rgba(255, 255, 255, 0.12);}
        .login-legal {text-align:center; color:#cbd5e1; font-size:0.73rem; margin-top:0.65rem;}
        .login-legal strong {color:#e2e8f0; font-weight:600;}

        .stApp h2, .stApp h3 {
            font-size: 1.35rem;
            background: linear-gradient(90deg, #1d4ed8, #4338ca);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .stApp [data-testid="stVerticalBlock"] > div {
            margin-bottom: 0.75rem;
        }

        .stButton > button {
            background: linear-gradient(135deg, #3b82f6, #6366f1);
            color: #ffffff;
            border-radius: 8px;
            border: none;
        }
        .stButton > button:hover {
            background: linear-gradient(135deg, #60a5fa, #818cf8);
            color: #ffffff;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stTextArea"] textarea,
        [data-testid="stDateInput"] input,
        [data-baseweb="select"] > div {
            background: #ffffff !important;
            border: 1px solid rgba(15, 23, 42, 0.16) !important;
            border-radius: 8px !important;
            color: #0f172a !important;
        }
        [data-baseweb="select"] span {
            color: #0f172a !important;
        }

        [data-testid="stDataFrame"] [role="columnheader"] {
            background: rgba(148, 163, 184, 0.16) !important;
        }
        [data-testid="stDataFrame"] [role="row"]:hover {
            background: rgba(59, 130, 246, 0.08) !important;
        }
        .login-card [data-testid="stTextInput"] input {
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.1);
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


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def is_active_status(value: Any) -> bool:
    normalized = _normalize_status(value)
    return normalized in {"active", "true", "1"}


def filter_active_learners(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if "is_active" in df.columns:
        return df[df["is_active"].apply(is_active_status)].copy()
    if "status" in df.columns:
        return df[df["status"].apply(is_active_status)].copy()
    return df.copy()


def filter_inactive_learners(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if "is_active" in df.columns:
        return df[~df["is_active"].apply(is_active_status)].copy()
    if "status" in df.columns:
        return df[~df["status"].apply(is_active_status)].copy()
    return df.iloc[0:0].copy()


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def apply_learner_filters(
    df: pd.DataFrame,
    search_text: str = "",
    team_filter: str = "All",
    org_filter: str = "All",
) -> pd.DataFrame:
    filtered = df.copy()
    if filtered.empty:
        return filtered

    filtered["name"] = filtered["name"].fillna("") if "name" in filtered.columns else ""
    filtered["team"] = filtered["team"].fillna("") if "team" in filtered.columns else ""
    filtered["organization_name"] = (
        filtered["organization_name"].fillna("Unassigned") if "organization_name" in filtered.columns else "Unassigned"
    )

    query = normalize_text(search_text).lower()
    if query:
        filtered = filtered[
            filtered["name"].str.lower().str.contains(query, na=False)
            | filtered["team"].str.lower().str.contains(query, na=False)
        ]

    if team_filter != "All" and "team" in filtered.columns:
        filtered = filtered[filtered["team"] == team_filter]

    if org_filter != "All" and "organization_name" in filtered.columns:
        filtered = filtered[filtered["organization_name"] == org_filter]

    return filtered


def build_learner_option_label(row: pd.Series) -> str:
    name = normalize_text(row.get("name")) or "Unnamed learner"
    team = normalize_text(row.get("team")) or "No team"
    return f"{name} ({team})"
