from __future__ import annotations

import json
import math
from decimal import Decimal
from html import escape
from contextlib import contextmanager
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st
import numpy as np

_APP_TABLE_STYLE_KEY = "_app_table_styles_injected"
_ADMIN_TABLE_STYLE_KEY = "_admin_table_styles_injected"
_ADMIN_SELECTION_STYLE_KEY = "_admin_selection_table_styles_injected"
_PAGE_CONTAINER_VARIANTS = {"wide", "medium", "narrow"}


def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    if pd.isna(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default




def inject_scroll_to_top(delay_ms: int = 50) -> None:
    st.markdown(
        f"""
        <script>
            setTimeout(function() {{
                window.scrollTo(0, 0);
            }}, {delay_ms});
        </script>
        """,
        unsafe_allow_html=True,
    )

def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg: #f5f7fb;
            --surface: #ffffff;
            --line: #e6eaf2;
            --text: #101828;
            --muted: #667085;
            --brand: #4f46e5;
            --brand-soft: #eef2ff;
            --success: #067647;
            --warning: #b54708;
            --danger: #b42318;
        }
        .main .block-container,
        [data-testid="stAppViewBlockContainer"],
        [data-testid="stMainBlockContainer"] {
            padding-top: 0.35rem !important;
        }
        .main .block-container {
            padding-bottom: 2rem;
            padding-left: 20px !important;
            padding-right: 20px !important;
            max-width: 1120px !important;
            margin-left: auto !important;
            margin-right: auto !important;
        }
        @media (min-width: 1600px) {
            .main .block-container {
                max-width: 1120px !important;
            }
        }
        .app-page-shell {
            width: 100%;
        }
        .app-page-container {
            margin-left: auto;
            margin-right: auto;
            padding: 0 20px 32px 20px;
            width: 100%;
            box-sizing: border-box;
        }
        .app-page-container-wide {
            max-width: 1120px;
        }
        .app-page-container-medium {
            max-width: 980px;
        }
        .app-page-container-narrow {
            max-width: 760px;
            padding-left: 18px;
            padding-right: 18px;
        }
        .stApp,
        [data-testid="stAppViewContainer"] {
            background: var(--bg);
            color: var(--text);
        }
        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
            width: 220px !important;
            min-width: 220px !important;
            max-width: 220px !important;
            flex: 0 0 220px !important;
        }
        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
            background: var(--surface);
            border-right: 1px solid var(--line);
            width: 220px !important;
            min-width: 220px !important;
            max-width: 220px !important;
            padding-top: 0.45rem;
            overflow: visible !important;
        }
        [data-testid="stSidebar"] .block-container {
            padding-top: 0.2rem;
            padding-bottom: 0.5rem;
            padding-left: 0.52rem;
            padding-right: 0.62rem;
            max-width: 100% !important;
            overflow: visible !important;
        }
        .sidebar-header {
            margin: 0.05rem 0 0.4rem 0;
            padding: 0.08rem 0.2rem 0.32rem 0.2rem;
            border-bottom: 1px solid var(--line);
        }
        .sidebar-eyebrow {
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            font-weight: 700;
            margin-bottom: 0.15rem;
        }
        .sidebar-title {
            font-size: 1.02rem;
            color: var(--text);
            font-weight: 700;
        }
        [data-testid="stSidebar"] .stButton {
            width: 100%;
            margin-bottom: 0;
            overflow: visible !important;
        }
        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"]:has(.stButton) {
            margin-bottom: 0.38rem;
        }
        [data-testid="stSidebar"] .stButton > button {
            width: 100% !important;
            max-width: 100% !important;
            border-radius: 10px;
            display: flex !important;
            align-items: center;
            justify-content: flex-start;
            text-align: left;
            white-space: nowrap;
            overflow: visible !important;
            text-overflow: clip !important;
            min-height: 1.78rem;
            padding: 0.22rem 0.52rem;
            font-weight: 500;
            font-size: 0.84rem;
            letter-spacing: 0;
            box-shadow: none;
            border: none !important;
            background: transparent !important;
            transition: background-color 0.14s ease, color 0.14s ease;
            gap: 0.26rem;
        }
        [data-testid="stSidebar"] .stButton > button > div,
        [data-testid="stSidebar"] .stButton > button > div > p,
        [data-testid="stSidebar"] .stButton > button [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] .stButton > button [data-testid="stMarkdownContainer"] p {
            width: 100% !important;
            max-width: 100% !important;
            min-width: 0 !important;
            text-align: left !important;
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            margin: 0 !important;
            line-height: 1.08 !important;
            display: block !important;
            font-size: 0.84rem !important;
        }
        [data-testid="stSidebar"] .stButton > button[kind="primary"] {
            background: var(--brand) !important;
            border: none !important;
            color: #ffffff;
            font-weight: 600;
        }
        [data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
            background: #4338ca !important;
            color: #ffffff;
            transform: none;
        }
        [data-testid="stSidebar"] .stButton > button[kind="secondary"] {
            background: transparent !important;
            border: none !important;
            color: #344054;
        }
        [data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {
            background: #f2f4f7 !important;
            color: var(--text);
            transform: none;
        }
        .stApp [data-testid="stMarkdownContainer"],
        .stApp label,
        .stApp p,
        .stApp .st-emotion-cache-10trblm,
        .stApp .st-emotion-cache-16idsys {
            color: var(--text);
        }

        .panel,
        .module-card,
        .stAppViewContainer > .main [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stDataFrame"],
        .stTabs [data-baseweb="tab-panel"] {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 16px;
        }
        .subtle {color: var(--muted); font-size: 0.92rem;}
        .title-chip {display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;background:var(--brand-soft);color:var(--brand);font-size:0.78rem;}
        .page-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.25rem;
        }
        .page-header-title { font-size: 1.5rem; font-weight: 700; color: var(--text); }
        .page-header-subtitle { color: var(--muted); font-size: 0.92rem; margin-top: 0.1rem; }
        .kpi-card {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 14px 16px;
        }
        .kpi-card.kpi-card-compact {
            padding: 10px 12px;
            border-radius: 14px;
        }
        .kpi-card.kpi-card-danger {
            border-color: rgba(180, 35, 24, 0.35);
            background: #fff7f7;
        }
        .kpi-card.kpi-card-warning {
            border-color: rgba(181, 71, 8, 0.35);
            background: #fffaf2;
        }
        .kpi-label { color: var(--muted); font-size: 0.82rem; font-weight: 600; text-transform: uppercase; letter-spacing: .02em; }
        .kpi-value { color: var(--text); font-size: 1.7rem; font-weight: 700; line-height: 1.1; margin-top: 6px; }
        .kpi-card.kpi-card-compact .kpi-value { font-size: 1.95rem; margin-top: 4px; }
        .kpi-sub { color: var(--muted); font-size: 0.82rem; margin-top: 4px; }
        .status-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 3px 10px;
            font-size: 0.78rem;
            font-weight: 600;
        }
        .status-active { background: #e7f8ef; color: #067647; }
        .status-pending { background: #fff4e5; color: #b54708; }
        .status-inactive { background: #f2f4f7; color: #344054; }
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
            font-size: 1.3rem;
            color: var(--text);
        }
        .stApp [data-testid="stVerticalBlock"] > div {
            margin-bottom: 0.75rem;
        }
        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
            margin-bottom: 0.2rem;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) {
            margin-bottom: 0.2rem !important;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) {
            border: 1px solid #e4e7ec;
            border-radius: 9px;
            background: #fcfdff;
            padding: 0.4rem 0.46rem 0.34rem;
            margin: 0 0 0.12rem;
        }
        .sidebar-profile.sidebar-profile-compact {
            border: 0;
            background: transparent;
            padding: 0;
            margin: 0;
        }
        [data-testid="stSidebar"] [data-testid="stVerticalBlockBorderWrapper"]:has([data-testid="sidebar-profile-compact"]) {
            border: 0;
            background: transparent;
            padding: 0;
            box-shadow: none;
        }
        .sidebar-profile-compact .sidebar-profile-name {
            color: var(--text);
            font-size: 0.78rem;
            font-weight: 700;
            line-height: 1.08;
            margin: 0;
        }
        .sidebar-profile-compact .sidebar-profile-label {
            color: #667085;
            font-size: 0.62rem;
            font-weight: 600;
            line-height: 1.04;
            margin: 0.08rem 0 0;
            letter-spacing: 0.01em;
        }
        .sidebar-profile-compact .sidebar-profile-email {
            color: #98a2b3;
            font-size: 0.58rem;
            line-height: 1.05;
            margin: 0.06rem 0 0;
            word-break: break-word;
        }
        .sidebar-profile-compact .sidebar-profile-actions {
            margin: 0.16rem 0 0;
            padding: 0;
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: 0.03rem;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton {
            margin: 0 !important;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton:first-of-type {
            margin-top: 0.14rem !important;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton + .stButton {
            margin-top: 0.04rem !important;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton > button {
            justify-content: flex-start;
            text-align: left;
            width: 100%;
            min-height: 1.05rem;
            padding: 0.03rem 0.06rem;
            border: 0;
            border-radius: 4px;
            background: transparent;
            color: #667085;
            font-size: 0.84rem;
            font-weight: 650;
            line-height: 1.08;
            box-shadow: none;
        }
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton > button:hover,
        section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div:has([data-testid="sidebar-profile-compact"]) .stButton > button:focus-visible {
            background: #f2f4f7;
            color: #344054;
            border: 0;
        }

        .stButton > button {
            border-radius: 10px;
            border: 1px solid #d0d5dd;
            transition: all 0.18s ease;
            box-shadow: none;
        }
        .stButton > button[kind="primary"] {
            background: var(--brand);
            color: #ffffff;
            border-color: transparent;
        }
        .stButton > button[kind="primary"]:hover {
            background: #4338ca;
            color: #ffffff;
            transform: none;
        }
        .stButton > button[kind="secondary"] {
            background: rgba(255, 255, 255, 0.9);
            color: #1e293b;
        }
        .stButton > button[kind="secondary"]:hover {
            border-color: #98a2b3;
            background: #f9fafb;
            color: #1d2939;
            transform: none;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stTextArea"] textarea,
        [data-testid="stDateInput"] input,
        [data-baseweb="select"] > div {
            background: #ffffff !important;
            border: 1px solid rgba(15, 23, 42, 0.16) !important;
            border-radius: 12px !important;
            color: #0f172a !important;
        }
        [data-baseweb="select"] span {
            color: #0f172a !important;
        }

        [data-testid="stDataFrame"] [role="columnheader"] {
            background: #f8f9fc !important;
        }
        [data-testid="stDataFrame"] [role="row"]:hover {
            background: #f9f5ff !important;
        }
        [data-testid="stMetric"] {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 12px;
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


def _normalize_container_variant(variant: str) -> str:
    normalized = str(variant or "wide").strip().lower()
    return normalized if normalized in _PAGE_CONTAINER_VARIANTS else "wide"


@contextmanager
def page_container(variant: str = "wide"):
    normalized = _normalize_container_variant(variant)
    st.markdown(
        f"<div class='app-page-shell'><div class='app-page-container app-page-container-{normalized}'>",
        unsafe_allow_html=True,
    )
    try:
        yield
    finally:
        st.markdown("</div></div>", unsafe_allow_html=True)


def metric_row(items: Dict[str, Any]) -> None:
    cols = st.columns(len(items))
    for i, (label, value) in enumerate(items.items()):
        cols[i].metric(label, value)


def render_page_header(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div class="page-header">
          <div>
            <div class="page-header-title">{title}</div>
            <div class="page-header-subtitle">{subtitle}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card(label: str, value: Any, subtext: str = "", tone: str = "default", compact: bool = False) -> None:
    tone_class = {
        "default": "",
        "warning": "kpi-card-warning",
        "danger": "kpi-card-danger",
    }.get(tone, "")
    compact_class = "kpi-card-compact" if compact else ""
    st.markdown(
        f"""
        <div class="kpi-card {tone_class} {compact_class}">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          <div class="kpi-sub">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def to_df(rows, columns: list[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame([dict(r) for r in rows])
    if columns is None:
        return df
    return ensure_dataframe_schema(df, columns)


def ensure_dataframe_schema(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=columns)
    if df.empty and not len(df.columns):
        return pd.DataFrame(columns=columns)
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df


def has_dataframe_columns(df: pd.DataFrame | None, columns: list[str]) -> bool:
    return df is not None and not df.empty and all(column in df.columns for column in columns)


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
    filtered["department"] = filtered["department"].fillna("") if "department" in filtered.columns else ""
    filtered["organization_name"] = (
        filtered["organization_name"].fillna("Unassigned") if "organization_name" in filtered.columns else "Unassigned"
    )

    query = normalize_text(search_text).lower()
    if query:
        filtered = filtered[
            filtered["name"].str.lower().str.contains(query, na=False)
            | filtered["team"].str.lower().str.contains(query, na=False)
            | filtered["department"].str.lower().str.contains(query, na=False)
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


def _format_datetime_value(value: Any) -> str:
    if value is None or value == "":
        return "—"
    try:
        dt = pd.to_datetime(value)
        return dt.strftime("%b %d, %Y • %I:%M %p")
    except Exception:
        return str(value)


def _format_numeric_value(value: Any, decimals: int = 1) -> str:
    try:
        return f"{float(value):.{decimals}f}"
    except Exception:
        return "—"


def normalize_display_value(raw_value: Any) -> str:
    if raw_value is None:
        return "—"
    if isinstance(raw_value, str):
        return raw_value if raw_value != "" else "—"
    if isinstance(raw_value, Decimal):
        return format(raw_value, "f")
    if isinstance(raw_value, np.ndarray):
        if raw_value.size == 0:
            return "—"
        return np.array2string(raw_value, separator=", ")
    if isinstance(raw_value, (list, tuple)):
        if len(raw_value) == 0:
            return "—"
        try:
            return json.dumps(raw_value, ensure_ascii=False, default=str)
        except Exception:
            return str(raw_value)
    if isinstance(raw_value, dict):
        if len(raw_value) == 0:
            return "—"
        try:
            return json.dumps(raw_value, ensure_ascii=False, default=str, sort_keys=True)
        except Exception:
            return str(raw_value)
    try:
        if pd.isna(raw_value):
            return "—"
    except Exception:
        pass
    return str(raw_value)


def format_status_display(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "—"

    normalized = raw.lower()
    direct_labels = {
        "pending_review": "Pending review",
        "approved": "Approved",
        "rejected": "Rejected",
    }
    if normalized in direct_labels:
        return direct_labels[normalized]

    humanized = normalized.replace("_", " ").strip()
    if not humanized:
        return "—"
    return humanized[0].upper() + humanized[1:]


def _badge_style(value: Any, kind: str = "neutral") -> str:
    if kind == "score":
        try:
            score = float(value)
        except Exception:
            return "background:#f3f4f6;color:#374151;"
        if score >= 85:
            return "background:#ecfdf5;color:#065f46;"
        if score >= 70:
            return "background:#eff6ff;color:#1d4ed8;"
        if score >= 50:
            return "background:#fffbeb;color:#92400e;"
        return "background:#fef2f2;color:#991b1b;"
    if kind == "status":
        normalized = str(value).strip().lower()
        if normalized in {"active", "approved", "completed", "success", "passed"}:
            return "background:#ecfdf5;color:#065f46;"
        if normalized in {"pending", "pending_review", "pending review", "in progress", "warning", "not started"}:
            return "background:#fffbeb;color:#92400e;"
        if normalized in {"inactive", "failed", "error", "overdue", "fail", "rejected"}:
            return "background:#fef2f2;color:#991b1b;"
    return "background:#f3f4f6;color:#374151;"


def _normalize_css_size(value: int | str | None, default: str = "450px") -> str:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return f"{int(value)}px"
    cleaned = str(value).strip()
    return cleaned or default


def render_admin_table(
    df: pd.DataFrame,
    *,
    height: int = 500,
    empty_message: str = "No records match current filters.",
) -> None:
    if not st.session_state.get(_ADMIN_TABLE_STYLE_KEY):
        st.session_state[_ADMIN_TABLE_STYLE_KEY] = True
        st.markdown(
            """
            <style>
            .admin-table-card {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 16px;
                padding: 0;
                margin-top: 12px;
                margin-bottom: 12px;
                width: 100%;
                overflow: hidden;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

    if df is None or df.empty:
        st.info(empty_message)
        return

    st.markdown('<div class="admin-table-card">', unsafe_allow_html=True)
    st.dataframe(
        df,
        use_container_width=True,
        height=height,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _inject_admin_selection_table_styles() -> None:
    if st.session_state.get(_ADMIN_SELECTION_STYLE_KEY):
        return
    st.session_state[_ADMIN_SELECTION_STYLE_KEY] = True
    st.markdown(
        """
        <style>
        .app-table-host {
            width: 100%;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            overflow: hidden;
            background: #ffffff;
        }
        .app-table-host [data-testid="stDataFrame"] {
            width: 100%;
        }
        .app-table-host [data-testid="stDataFrame"] [role="columnheader"] {
            background: #f8fafc;
            border-bottom: 1px solid #e5e7eb;
        }
        .app-select-table {
            width: 100%;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            overflow: hidden;
            background: #ffffff;
        }
        .app-select-header {
            background: #f8fafc;
            border-bottom: 1px solid #e5e7eb;
            padding: 0.45rem 0.6rem;
            font-weight: 600;
            color: #111827;
            font-size: 0.84rem;
        }
        .app-select-row {
            border-bottom: 1px solid #f1f5f9;
            padding: 0.25rem 0.35rem;
        }
        .app-select-row:last-child {
            border-bottom: none;
        }
        .app-select-selected {
            background: #e0f2fe;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_admin_selection_table(
    df: pd.DataFrame,
    *,
    row_id_col: str,
    selection_state_key: str,
    table_key: str,
    selection_label: str = "Select",
    selection_help: str = "Select row(s).",
    single_select: bool = True,
    use_container_width: bool = True,
    hide_index: bool = True,
    height: int = 450,
    empty_message: str | None = None,
) -> tuple[pd.DataFrame, list]:
    _inject_admin_selection_table_styles()
    table_selection_key = f"{table_key}_selected_ids"

    if df is None or df.empty:
        if empty_message:
            st.info(empty_message)
        if table_selection_key not in st.session_state:
            st.session_state[table_selection_key] = []
        if selection_state_key not in st.session_state:
            st.session_state[selection_state_key] = None if single_select else []
        return pd.DataFrame(), []

    if row_id_col not in df.columns:
        raise ValueError(f"row_id_col '{row_id_col}' must exist in table data.")

    display_df = df.reset_index(drop=True).copy()
    selection_col = "__selected__"
    preserved_ids = st.session_state.get(table_selection_key, [])
    preserved_ids = [safe_int(v, -1) for v in preserved_ids]
    display_df.insert(0, selection_col, display_df[row_id_col].apply(lambda value: safe_int(value, -1) in preserved_ids))

    edited_df = st.data_editor(
        display_df,
        key=table_key,
        hide_index=hide_index,
        use_container_width=use_container_width,
        height=height or 420,
        column_config={
            selection_col: st.column_config.CheckboxColumn(
                selection_label,
                help=selection_help,
                default=False,
            ),
        },
        disabled=[column for column in display_df.columns if column != selection_col],
    )
    if edited_df is None or edited_df.empty:
        selected_ids = []
        selected_df = display_df.iloc[0:0]
    else:
        selected_rows_df = edited_df[edited_df[selection_col]].copy()
        selected_ids = [safe_int(v) for v in selected_rows_df[row_id_col].tolist()]
        selected_df = selected_rows_df.drop(columns=[selection_col], errors="ignore")

    if single_select and len(selected_ids) > 1:
        previously_selected = [safe_int(v, -1) for v in st.session_state.get(table_selection_key, [])]
        newly_selected = [row_id for row_id in selected_ids if row_id not in previously_selected]
        keep_id = newly_selected[-1] if newly_selected else selected_ids[-1]
        selected_ids = [keep_id]
        selected_df = selected_df[selected_df[row_id_col].apply(lambda value: safe_int(value, -1) == keep_id)]

    st.session_state["selected_row_ids"] = selected_ids
    if single_select:
        st.session_state[selection_state_key] = selected_ids[0] if selected_ids else None
    else:
        st.session_state[selection_state_key] = selected_ids
    st.session_state[table_selection_key] = selected_ids

    if selected_ids and single_select:
        st.caption(f"Selected: {selected_ids[0]}")
    elif selected_ids:
        st.caption(f"Selected: {len(selected_ids)}")
    else:
        st.caption("Selected: None")

    return selected_df, selected_ids


def _inject_app_table_styles() -> None:
    if st.session_state.get(_APP_TABLE_STYLE_KEY):
        return
    st.session_state[_APP_TABLE_STYLE_KEY] = True
    st.markdown(
        """
        <style>
        .app-table-headline {margin-bottom: 0.55rem;}
        .app-table-title {font-size: 1.03rem; font-weight: 650; color: #111827;}
        .app-table-subtitle {font-size: 0.86rem; color: #6b7280; margin-top: 2px;}
        .app-table-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 0;
            width: 100%;
            overflow: hidden;
        }
        .app-table-card.app-table-scroll {
            max-height: var(--app-table-max-height, 500px);
            overflow-x: auto;
            overflow-y: auto;
        }
        .app-table-inner {
            width: 100%;
        }
        .app-table {
            width: 100%;
            border-collapse: collapse;
            border-spacing: 0;
            table-layout: auto;
            font-size: 14px;
            min-width: 640px;
        }
        .app-table thead th {
            text-align: left;
            font-weight: 600;
            color: #6b7280;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            padding: 12px 16px;
            border-bottom: 1px solid #e5e7eb;
            white-space: nowrap;
            position: sticky;
            top: 0;
            z-index: 2;
            background: #ffffff;
            vertical-align: middle;
        }
        .app-table tbody td {
            padding: 12px 16px;
            border-bottom: 1px solid #f1f5f9;
            color: #111827;
            vertical-align: middle;
            line-height: 1.35;
            overflow-wrap: anywhere;
        }
        .app-table tbody tr:last-child td { border-bottom: none; }
        .app-table-primary { font-weight: 600; color: #111827; }
        .app-table-cell {
            display: block;
            max-width: 100%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .app-table-pill {
            display: inline-block;
            padding: 6px 10px;
            border-radius: 999px;
            font-weight: 600;
            font-size: 12px;
            line-height: 1;
            white-space: nowrap;
        }
        .app-table-empty {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 28px;
            color: #6b7280;
        }
        .app-table-select-wrap { margin-top: 0.55rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_app_table(
    df: pd.DataFrame,
    *,
    column_labels: Optional[dict] = None,
    datetime_columns: Optional[list] = None,
    numeric_formats: Optional[dict] = None,
    badge_columns: Optional[dict] = None,
    hidden_columns: Optional[list] = None,
    numeric_align: Optional[dict] = None,
    max_visible_rows: Optional[int] = None,
    sort_by: Optional[list[str]] = None,
    ascending: bool | list[bool] = True,
    table_title: str = "",
    table_subtitle: str = "",
    empty_title: str = "No data available",
    empty_message: str = "Records will appear here once data is available.",
    table_max_height: int | str | None = 500,
) -> pd.DataFrame:
    _inject_app_table_styles()
    column_labels = column_labels or {}
    datetime_columns = datetime_columns or []
    numeric_formats = numeric_formats or {}
    badge_columns = badge_columns or {}
    hidden_columns = hidden_columns or []
    numeric_align = numeric_align or {}

    if table_title or table_subtitle:
        st.markdown(
            f"""
            <div class="app-table-headline">
                <div class="app-table-title">{escape(table_title)}</div>
                <div class="app-table-subtitle">{escape(table_subtitle)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if df is None or df.empty:
        st.markdown(
            f"""
            <div class="app-table-empty">
                <div style="font-size:16px;font-weight:600;color:#111827;margin-bottom:6px;">{escape(empty_title)}</div>
                <div>{escape(empty_message)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return pd.DataFrame()

    render_df = df.copy()
    render_df = render_df.drop(columns=[c for c in hidden_columns if c in render_df.columns], errors="ignore")
    if sort_by:
        existing_sort = [col for col in sort_by if col in render_df.columns]
        if existing_sort:
            render_df = render_df.sort_values(existing_sort, ascending=ascending)
    if max_visible_rows is not None:
        render_df = render_df.head(max_visible_rows)

    for col in datetime_columns:
        if col in render_df.columns:
            render_df[col] = render_df[col].apply(_format_datetime_value)
    for col, decimals in numeric_formats.items():
        if col in render_df.columns and col not in badge_columns:
            render_df[col] = render_df[col].apply(lambda x: _format_numeric_value(x, decimals))

    table_height = _normalize_css_size(table_max_height)
    html = [f'<div class="app-table-card app-table-scroll" style="--app-table-max-height:{escape(table_height)};"><div class="app-table-inner"><table class="app-table"><thead><tr>']
    for col in render_df.columns:
        label = column_labels.get(col, col.replace("_", " ").title())
        alignment = "right" if numeric_align.get(col) == "right" else "left"
        html.append(f'<th style="text-align:{alignment};">{escape(str(label))}</th>')
    html.append("</tr></thead><tbody>")

    for _, row in render_df.iterrows():
        html.append("<tr>")
        for idx, col in enumerate(render_df.columns):
            raw_value = row[col]
            display_value = normalize_display_value(raw_value)
            alignment = "right" if numeric_align.get(col) == "right" else "left"
            if col in badge_columns:
                badge_kind = badge_columns[col]
                if col in numeric_formats:
                    display_value = _format_numeric_value(raw_value, numeric_formats[col])
                style = _badge_style(raw_value, badge_kind)
                html.append(
                    f'<td style="text-align:{alignment};"><span class="app-table-pill" style="{style}">{escape(str(display_value))}</span></td>'
                )
            else:
                cell_class = "app-table-primary" if idx == 0 else ""
                html.append(
                    f'<td style="text-align:{alignment};"><span class="app-table-cell {cell_class}">{escape(str(display_value))}</span></td>'
                )
        html.append("</tr>")
    html.append("</tbody></table></div></div>")
    st.markdown("".join(html), unsafe_allow_html=True)
    return render_df
