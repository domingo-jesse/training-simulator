from __future__ import annotations

import json
from html import escape
from contextlib import contextmanager
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

_APP_TABLE_STYLE_KEY = "_app_table_styles_injected"
_ADMIN_TABLE_STYLE_KEY = "_admin_table_styles_injected"
_PAGE_CONTAINER_VARIANTS = {"wide", "medium", "narrow"}


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
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 2rem;
            padding-left: 0;
            padding-right: 0;
            max-width: 100%;
        }
        .app-page-shell {
            width: 100%;
        }
        .app-page-container {
            margin-left: auto;
            margin-right: auto;
            padding: 0 28px 32px 28px;
            width: 100%;
            box-sizing: border-box;
        }
        .app-page-container-wide {
            max-width: 1280px;
        }
        .app-page-container-medium {
            max-width: 1080px;
        }
        .app-page-container-narrow {
            max-width: 820px;
            padding-left: 24px;
            padding-right: 24px;
        }
        .stApp,
        [data-testid="stAppViewContainer"] {
            background: var(--bg);
            color: var(--text);
        }
        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
            width: 250px !important;
            min-width: 250px !important;
            max-width: 250px !important;
            flex: 0 0 250px !important;
        }
        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
            background: var(--surface);
            border-right: 1px solid var(--line);
            width: 250px !important;
            min-width: 250px !important;
            max-width: 250px !important;
            padding-top: 0.8rem;
            overflow: visible !important;
        }
        [data-testid="stSidebar"] .block-container {
            padding-top: 0.35rem;
            padding-bottom: 1rem;
            padding-left: 0.8rem;
            padding-right: 0.8rem;
            max-width: 100% !important;
            overflow: visible !important;
        }
        .sidebar-header {
            margin: 0.1rem 0 1rem 0;
            padding: 0.2rem 0.3rem 0.8rem 0.3rem;
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
        [data-testid="stSidebar"] .stButton > button {
            width: 100% !important;
            max-width: 100% !important;
            border-radius: 12px;
            display: flex !important;
            align-items: center;
            justify-content: flex-start;
            text-align: left;
            white-space: nowrap;
            overflow: visible !important;
            text-overflow: clip !important;
            min-height: 2.1rem;
            padding: 0.4rem 0.72rem;
            font-weight: 500;
            font-size: 0.9rem;
            letter-spacing: 0.005em;
            box-shadow: none;
            border: none !important;
            background: transparent !important;
            transition: background-color 0.14s ease, color 0.14s ease;
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
            line-height: 1.16 !important;
            display: block !important;
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
        [data-testid="stVerticalBlockBorderWrapper"],
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
        .kpi-label { color: var(--muted); font-size: 0.82rem; font-weight: 600; text-transform: uppercase; letter-spacing: .02em; }
        .kpi-value { color: var(--text); font-size: 1.7rem; font-weight: 700; line-height: 1.1; margin-top: 6px; }
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
        .app-shell-header {
            padding: 0.1rem 0 0.35rem 0;
        }
        .app-shell-header-title {
            font-size: 1.32rem;
            font-weight: 700;
            color: var(--text);
            line-height: 1.2;
        }
        .app-shell-header-subtitle {
            margin-top: 0.2rem;
            color: var(--muted);
            font-size: 0.9rem;
        }
        .shell-divider {
            border-bottom: 1px solid var(--line);
            margin: 0.15rem 0 1rem 0;
        }
        .profile-menu-anchor {
            display: flex;
            justify-content: flex-end;
            align-items: center;
            width: 100%;
            padding-top: 0.1rem;
        }
        .profile-menu-anchor .stPopover {
            margin-left: auto;
        }
        .profile-menu-anchor .stPopover > button {
            height: 2.3rem;
            border-radius: 12px;
            padding: 0 0.8rem;
            font-size: 0.84rem;
            font-weight: 600;
            white-space: nowrap;
            max-width: 100%;
            overflow: hidden;
            text-overflow: ellipsis;
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


def render_kpi_card(label: str, value: Any, subtext: str = "") -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          <div class="kpi-sub">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
        if normalized in {"active", "completed", "success", "passed"}:
            return "background:#ecfdf5;color:#065f46;"
        if normalized in {"pending", "in progress", "warning", "not started"}:
            return "background:#fffbeb;color:#92400e;"
        if normalized in {"inactive", "failed", "error", "overdue", "fail"}:
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
                padding: 12px;
                margin-top: 12px;
                margin-bottom: 12px;
                width: 100%;
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
            padding: 10px 14px;
            width: 100%;
            max-height: var(--app-table-max-height, 450px);
            overflow: hidden;
        }
        .app-table-scroll {
            border-radius: 12px;
            max-height: 100%;
            overflow-y: auto;
            overflow-x: auto;
        }
        .app-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
            min-width: 100%;
        }
        .app-table thead th {
            text-align: left;
            font-weight: 600;
            color: #6b7280;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            padding: 12px 14px;
            border-bottom: 1px solid #e5e7eb;
            white-space: nowrap;
            position: sticky;
            top: 0;
            z-index: 2;
            background: #ffffff;
        }
        .app-table tbody td {
            padding: 14px;
            border-bottom: 1px solid #f1f5f9;
            color: #111827;
            vertical-align: middle;
        }
        .app-table tbody tr:last-child td { border-bottom: none; }
        .app-table-primary { font-weight: 600; color: #111827; }
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
    table_max_height: int | str | None = 450,
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
    html = [f'<div class="app-table-card" style="--app-table-max-height:{escape(table_height)};"><div class="app-table-scroll"><table class="app-table"><thead><tr>']
    for col in render_df.columns:
        label = column_labels.get(col, col.replace("_", " ").title())
        alignment = "right" if numeric_align.get(col) == "right" else "left"
        html.append(f'<th style="text-align:{alignment};">{escape(str(label))}</th>')
    html.append("</tr></thead><tbody>")

    for _, row in render_df.iterrows():
        html.append("<tr>")
        for idx, col in enumerate(render_df.columns):
            raw_value = row[col]
            display_value = raw_value if pd.notna(raw_value) and raw_value != "" else "—"
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
                    f'<td style="text-align:{alignment};"><span class="{cell_class}">{escape(str(display_value))}</span></td>'
                )
        html.append("</tr>")
    html.append("</tbody></table></div></div>")
    st.markdown("".join(html), unsafe_allow_html=True)
    return render_df
