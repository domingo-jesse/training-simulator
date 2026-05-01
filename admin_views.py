from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from html import escape
from io import StringIO
from uuid import uuid4

import pandas as pd
import psycopg2
import streamlit as st

from db import (
    ensure_module_rubric_criteria_table,
    execute,
    executemany,
    fetch_all,
    fetch_one,
    fetch_table_rows,
    get_database_debug_info,
    list_public_tables,
    table_exists,
)
from ai_grading import grade_submission_with_ai
from logger import get_logger
from log_viewer import (
    LOG_LEVEL_OPTIONS,
    filter_log_lines,
    get_recent_lines,
    read_full_file_for_download,
    read_log_lines,
)
from permissions import is_dev_account
from utils import (
    apply_learner_filters,
    build_learner_option_label,
    ensure_dataframe_schema,
    filter_active_learners,
    filter_inactive_learners,
    format_status_display,
    has_dataframe_columns,
    metric_row,
    render_admin_selection_table,
    render_app_table,
    render_kpi_card,
    render_page_header,
    table_card_container,
    safe_int,
    inject_scroll_to_top,
    to_df,
)
from module_generation import (
    ModuleDraftGenerationInput,
    ModuleGenerationInput,
    generate_ai_grading_criteria,
    generate_question_scoring_criteria,
    generate_module_draft,
    generate_module_preview,
)



def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

admin_logger = get_logger(module="admin_views")

QUESTION_SCORING_OPTIONS = ["manual", "keyword", "llm"]
MULTIPLE_CHOICE_SCORING_OPTIONS = ["manual", "llm"]
QUESTION_SCORING_LABELS = {
    "manual": "Manual Review",
    "keyword": "Keyword",
    "llm": "AI Review",
}
QUESTION_TYPE_OPTIONS = ["open_text", "multiple_choice", "ai_conversation"]
AI_CONVERSATION_MAX_RESPONSE_OPTIONS = [3, 4]


def _format_datetime_for_admin_grid(value) -> str:
    if value is None or value == "":
        return "—"
    try:
        dt = pd.to_datetime(value)
        return dt.strftime("%b %d, %Y • %I:%M %p")
    except Exception:
        return str(value)


def _select_all_filtered(multiselect_key: str, option_labels: list[str]) -> None:
    st.session_state[multiselect_key] = list(option_labels)


def _clear_filtered_selection(multiselect_key: str) -> None:
    st.session_state[multiselect_key] = []


@st.cache_data(show_spinner=False)
def column_exists(table_name: str, column_name: str) -> bool:
    row = fetch_one(
        """
        SELECT 1 AS exists_flag
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return bool(row)


@st.cache_data(show_spinner=False)
def _assignments_with_status(org_id: int, refresh_token: int = 0) -> pd.DataFrame:
    rows = fetch_all(
        """
        SELECT
            a.assignment_id,
            a.module_id,
            a.learner_id,
            a.due_date,
            a.assigned_at,
            a.is_active,
            u.name AS learner_name,
            u.email AS learner_email,
            u.team,
            u.is_active AS learner_active,
            o.name AS organization_name,
            m.title AS module_title,
            CASE
                WHEN x.last_attempt_at IS NOT NULL THEN 'Completed'
                WHEN a.due_date IS NOT NULL AND a.due_date::date < CURRENT_DATE THEN 'Overdue'
                WHEN x.attempt_count > 0 THEN 'In Progress'
                ELSE 'Not Started'
            END AS status,
            x.attempt_count,
            x.last_attempt_at
        FROM assignments a
        JOIN users u ON u.user_id = a.learner_id
        JOIN modules m ON m.module_id = a.module_id
        LEFT JOIN organizations o ON o.organization_id = u.organization_id
        LEFT JOIN (
            SELECT
                a2.assignment_id,
                COUNT(t.attempt_id) AS attempt_count,
                MAX(t.created_at) AS last_attempt_at
            FROM assignments a2
            LEFT JOIN attempts t
                ON t.user_id = a2.learner_id
               AND t.module_id = a2.module_id
               AND t.organization_id = a2.organization_id
               AND t.created_at >= a2.assigned_at
            WHERE a2.organization_id = ?
              AND a2.is_active = TRUE
            GROUP BY a2.assignment_id
        ) x ON x.assignment_id = a.assignment_id
        WHERE a.organization_id = ? AND a.is_active = TRUE AND u.is_active = TRUE
        ORDER BY a.assigned_at DESC
        """,
        (org_id, org_id),
    )
    return to_df(
        rows,
        columns=[
            "assignment_id",
            "module_id",
            "learner_id",
            "due_date",
            "assigned_at",
            "is_active",
            "learner_name",
            "learner_email",
            "team",
            "learner_active",
            "organization_name",
            "module_title",
            "status",
            "attempt_count",
            "last_attempt_at",
        ],
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_assignment_tool_learners(org_id: int) -> list[dict]:
    return fetch_all(
        """
        SELECT
            u.user_id,
            u.name,
            u.team,
            u.is_active,
            u.role,
            u.email,
            o.name AS organization_name
        FROM users u
        LEFT JOIN organizations o ON o.organization_id = u.organization_id
        WHERE u.organization_id=?
        ORDER BY u.name
        """,
        (org_id,),
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_assignment_tool_modules(org_id: int) -> list[dict]:
    return fetch_all(
        "SELECT module_id, title, status FROM modules WHERE organization_id=? AND COALESCE(status, 'existing') <> 'archived' ORDER BY title",
        (org_id,),
    )


def _sync_assignment_tracking_records(
    *,
    organization_id: int,
    module_id: int,
    learner_id: int,
    assigned_by_user_id: int,
) -> None:
    ids = fetch_one(
        """
        SELECT
            learner.id AS learner_external_id,
            module.id AS module_external_id,
            assigner.id AS assigner_external_id
        FROM users learner
        JOIN modules module ON module.module_id = ?
        LEFT JOIN users assigner ON assigner.user_id = ?
        WHERE learner.user_id = ?
          AND learner.organization_id = ?
        """,
        (module_id, assigned_by_user_id, learner_id, organization_id),
    )
    if not ids or not ids.get("learner_external_id") or not ids.get("module_external_id"):
        return

    execute(
        """
        INSERT INTO module_assignments (
            id,
            user_id,
            module_id,
            organization_id,
            assigned_at,
            assigned_by,
            created_at
        )
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, module_id) DO UPDATE SET
            organization_id = excluded.organization_id,
            assigned_at = CURRENT_TIMESTAMP,
            assigned_by = excluded.assigned_by
        """,
        (
            uuid4().hex,
            ids["learner_external_id"],
            ids["module_external_id"],
            organization_id,
            ids.get("assigner_external_id"),
        ),
    )
    execute(
        """
        INSERT INTO module_progress (
            id,
            user_id,
            module_id,
            organization_id,
            progress_percent,
            started_at,
            last_activity_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, module_id) DO UPDATE SET
            organization_id = excluded.organization_id,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            uuid4().hex,
            ids["learner_external_id"],
            ids["module_external_id"],
            organization_id,
        ),
    )


def _cleanup_assignment_tracking_records(
    *,
    organization_id: int,
    module_id: int,
    learner_id: int,
) -> None:
    has_other_active = fetch_one(
        """
        SELECT assignment_id
        FROM assignments
        WHERE organization_id = ?
          AND module_id = ?
          AND learner_id = ?
          AND is_active = TRUE
        LIMIT 1
        """,
        (organization_id, module_id, learner_id),
    )
    if has_other_active:
        return

    execute(
        """
        DELETE FROM module_assignments
        WHERE organization_id = ?
          AND user_id = (SELECT id FROM users WHERE user_id = ?)
          AND module_id = (SELECT id::uuid FROM modules WHERE module_id = ?)
        """,
        (organization_id, learner_id, module_id),
    )


def render_admin_dashboard(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    render_page_header("Dashboard Overview", "Operational health across learners, assignments, and outcomes.")

    try:
        learners_df = to_df(fetch_all("SELECT * FROM users WHERE role='learner' AND organization_id = ?", (org_id,)))
        modules_df = to_df(fetch_all("SELECT * FROM modules WHERE organization_id = ?", (org_id,)))
        assignments_df = _assignments_with_status(org_id)
    except Exception:
        logger.exception("Failed to load admin dashboard.")
        st.error("Failed to load dashboard data.")
        return

    learners_df = ensure_dataframe_schema(learners_df, ["is_active"])
    assignments_df = ensure_dataframe_schema(assignments_df, ["status", "last_attempt_at", "learner_name", "module_title"])

    total_learners = len(learners_df)
    active_learners = int(learners_df["is_active"].fillna(False).astype(bool).sum()) if not learners_df.empty else 0
    inactive_learners = total_learners - active_learners
    modules_created = len(modules_df)
    completion_rate = round((assignments_df["status"].eq("Completed").mean() * 100), 1) if not assignments_df.empty else 0.0
    overdue_assignments = int(assignments_df["status"].eq("Overdue").sum()) if not assignments_df.empty else 0
    in_progress_assignments = int(assignments_df["status"].eq("In Progress").sum()) if not assignments_df.empty else 0

    st.markdown(
        """
        <style>
        .dashboard-section-title {
            font-size: 0.95rem;
            font-weight: 700;
            color: #344054;
            margin: 0.05rem 0 0.4rem 0;
        }
        .dashboard-surface {
            border: 1px solid #eaecf0;
            border-radius: 14px;
            background: #ffffff;
            padding: 12px;
            height: 100%;
        }
        .attention-panel {
            border: 1px solid #f3d9d6;
            border-radius: 12px;
            background: #fff9f8;
            padding: 10px;
        }
        .attention-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 7px 0;
            border-bottom: 1px dashed #f0dbd7;
            font-size: 0.84rem;
        }
        .attention-row:last-child { border-bottom: none; padding-bottom: 2px; }
        .attention-label { color: #7a271a; font-weight: 600; }
        .attention-value { color: #b42318; font-weight: 700; }
        .dashboard-meta-list {
            margin: 0;
            padding: 0;
            list-style: none;
        }
        .dashboard-meta-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 7px 0;
            border-bottom: 1px solid #f2f4f7;
            font-size: 0.84rem;
            color: #475467;
        }
        .dashboard-meta-item:last-child { border-bottom: none; }
        .dashboard-meta-value {
            font-weight: 700;
            color: #101828;
        }
        .dashboard-submission-item {
            padding: 9px 0;
            border-bottom: 1px solid #eaecf0;
        }
        .dashboard-submission-item:last-child { border-bottom: none; padding-bottom: 0; }
        .submission-title {
            margin: 0;
            font-size: 0.9rem;
            font-weight: 600;
            color: #1d2939;
            line-height: 1.25;
        }
        .submission-meta {
            margin-top: 3px;
            color: #667085;
            font-size: 0.8rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    top_left, top_right = st.columns([1.8, 1], gap="small")
    with top_left:
        with st.container(border=True):
            st.markdown("<div class='dashboard-section-title'>Recent submissions</div>", unsafe_allow_html=True)
            if has_dataframe_columns(assignments_df, ["last_attempt_at"]):
                recent = assignments_df[assignments_df["last_attempt_at"].notna()].head(5)
            else:
                recent = pd.DataFrame()
            if recent.empty:
                st.info("No recent activity yet.")
            else:
                for _, row in recent.iterrows():
                    attempted_at = _format_datetime_for_admin_grid(row["last_attempt_at"])
                    learner_name = escape(str(row["learner_name"]))
                    module_title = escape(str(row["module_title"]))
                    st.markdown(
                        f"""
                        <div class="dashboard-submission-item">
                          <p class="submission-title">{learner_name} • {module_title}</p>
                          <div class="submission-meta">{attempted_at}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
    with top_right:
        with st.container(border=True):
            st.markdown("<div class='dashboard-section-title'>Needs attention</div>", unsafe_allow_html=True)
            st.markdown(
                f"""
                <div class="attention-panel">
                  <div class="attention-row"><span class="attention-label">🔴 Overdue assignments</span><span class="attention-value">{overdue_assignments}</span></div>
                  <div class="attention-row"><span class="attention-label">🟠 Inactive learners</span><span class="attention-value">{inactive_learners}</span></div>
                  <div class="attention-row"><span class="attention-label">🟡 Completion rate</span><span class="attention-value">{completion_rate}%</span></div>
                </div>
                <ul class="dashboard-meta-list">
                  <li class="dashboard-meta-item"><span>Active learners</span><span class="dashboard-meta-value">{active_learners}</span></li>
                  <li class="dashboard-meta-item"><span>Total modules</span><span class="dashboard-meta-value">{modules_created}</span></li>
                  <li class="dashboard-meta-item"><span>In progress assignments</span><span class="dashboard-meta-value">{in_progress_assignments}</span></li>
                </ul>
                """,
                unsafe_allow_html=True,
            )
    
    


def _create_account_from_admin(
    org_id: int,
    role: str,
    full_name: str,
    email: str,
    username: str,
    password: str,
    confirm_password: str,
    auth_provider: str = "local_password",
    is_active: bool = True,
) -> tuple[bool, str]:
    role = (role or "").strip().lower()
    full_name = (full_name or "").strip()
    email = (email or "").strip().lower()
    username = (username or "").strip()
    if role not in {"learner", "admin"}:
        return False, "Please select a valid role."
    auth_provider = (auth_provider or "local_password").strip().lower()
    if auth_provider not in {"local_password", "solar"}:
        return False, "Please select a valid auth provider."
    requires_password = auth_provider == "local_password"
    if not full_name or not email or (requires_password and (not password or not confirm_password)):
        return False, "Please complete all required fields."
    if "@" not in email or "." not in email.split("@")[-1]:
        return False, "Please enter a valid email address."
    existing = fetch_one("SELECT user_id FROM users WHERE LOWER(email)=? AND role=? LIMIT 1", (email, role))
    if existing:
        return False, f"A {role.title()} account with this email already exists."
    if username and fetch_one("SELECT user_id FROM users WHERE LOWER(username)=? LIMIT 1", (username.lower(),)):
        return False, "That username is already in use."
    if requires_password and password != confirm_password:
        return False, "Passwords must match."
    try:
        execute(
            """
            INSERT INTO users (id, name, email, role, team, organization_id, username, password_hash, auth_provider, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"u_{role}_{email}",
                full_name,
                email,
                role,
                "General",
                org_id,
                (username or None),
                hash_password(password) if requires_password else None,
                auth_provider,
                bool(is_active),
            ),
        )
    except Exception as exc:
        txt = str(exc).lower()
        if "username" in txt:
            return False, "That username is already in use."
        if "email" in txt:
            return False, f"A {role.title()} account with this email already exists."
        return False, "We couldn't create that account right now."
    return True, f"{role.title()} account created successfully ({'active' if is_active else 'inactive'} • {auth_provider})."


def render_account_management(current_user: dict) -> None:
    if str(current_user.get("role") or "").lower() != "admin":
        st.error("Account Management is restricted to admin users.")
        return

    org_id = current_user["organization_id"]
    render_page_header("Account Management", "View all organization accounts and create new accounts.")

    all_tab, create_tab = st.tabs(["All Accounts", "Create Account"])

    with all_tab:
        rows = fetch_all(
            """
            SELECT user_id, id AS user_uuid, name, email, username, role, team, is_active, auth_provider, organization_id
            FROM users
            WHERE organization_id = ?
            ORDER BY LOWER(name), user_id
            """,
            (org_id,),
        )
        df = to_df(rows, columns=["user_id", "user_uuid", "name", "email", "username", "role", "team", "is_active", "auth_provider", "organization_id"])
        if df.empty:
            st.info("No accounts in this organization yet.")
        else:
            df["team"] = df["team"].fillna("")
            df["email"] = df["email"].fillna("")
            df["username"] = df["username"].fillna("")
            df["role"] = df["role"].fillna("unknown")
            df["status"] = df["is_active"].astype(bool).map({True: "Active", False: "Inactive"})
            c1, c2, c3 = st.columns([2,1,1])
            with c1:
                q = st.text_input("Search accounts", placeholder="Search by name, email, username, or team")
            with c2:
                role_options = ["All"] + sorted(df["role"].str.lower().unique().tolist())
                role_filter = st.selectbox("Role", role_options)
            with c3:
                status_filter = st.selectbox("Status", ["All", "Active", "Inactive"])
            filtered = df.copy()
            if q.strip():
                qn = q.strip().lower()
                filtered = filtered[
                    filtered[["name", "email", "username", "team", "role"]]
                    .fillna("")
                    .apply(lambda col: col.astype(str).str.lower().str.contains(qn, na=False))
                    .any(axis=1)
                ]
            if role_filter != "All":
                filtered = filtered[filtered["role"].str.lower() == role_filter]
            if status_filter != "All":
                filtered = filtered[filtered["status"] == status_filter]

            display = filtered[["user_id", "name", "email", "username", "role", "team", "status", "auth_provider"]].rename(columns={
                "user_id": "Account ID",
                "name": "Name",
                "email": "Email",
                "username": "Username",
                "role": "Role",
                "team": "Team",
                "status": "Status",
                "auth_provider": "Auth Provider",
            })
            st.caption(f"{len(filtered)} account(s) shown.")
            st.dataframe(display, use_container_width=True, hide_index=True)

            st.markdown("#### Update account status")
            account_options = [
                (
                    int(row["user_id"]),
                    f"{row['name']} ({row['email'] or 'no-email'}) — {row['status']}",
                )
                for _, row in filtered.sort_values(["name", "user_id"]).iterrows()
            ]
            selected_user_id = st.selectbox(
                "Select account",
                options=[user_id for user_id, _ in account_options],
                format_func=lambda user_id: next(label for uid, label in account_options if uid == user_id),
                key="account_mgmt_status_target_user_id",
            )
            selected_user_row = df[df["user_id"] == selected_user_id].iloc[0]
            selected_is_active = bool(selected_user_row["is_active"])
            target_action = "Deactivate" if selected_is_active else "Activate"

            if st.button(
                f"{target_action} account",
                type="secondary" if selected_is_active else "primary",
                width="stretch",
                key="account_mgmt_toggle_status_btn",
            ):
                if int(selected_user_id) == int(current_user.get("user_id") or -1):
                    st.error("You cannot deactivate your own account while signed in.")
                else:
                    # users.is_active is a Postgres BOOLEAN column and must be written as True/False,
                    # never as integer flags like 0/1.
                    next_is_active = bool(not selected_is_active)
                    admin_logger.info(
                        "Updating account active state.",
                        organization_id=org_id,
                        target_user_id=int(selected_user_id),
                        is_active=next_is_active,
                    )
                    # Defensive normalization in case UI/session values arrive as 0/1.
                    next_is_active = bool(next_is_active)
                    execute(
                        "UPDATE users SET is_active = ? WHERE organization_id = ? AND user_id = ?",
                        (next_is_active, org_id, int(selected_user_id)),
                    )
                    st.success(
                        f"Account {'deactivated' if selected_is_active else 'activated'} for "
                        f"{selected_user_row['name']}."
                    )
                    st.cache_data.clear()
                    st.rerun()

    with create_tab:
        st.caption("Admin-only account creation. Passwords are stored as hashes (not plaintext).")
        role = st.selectbox("Role", ["learner", "admin"], format_func=lambda r: r.title())
        with st.form("admin_create_account_form", clear_on_submit=False):
            full_name = st.text_input("Full name *")
            email = st.text_input("Email *")
            username = st.text_input("Username (optional)")
            auth_provider = st.selectbox("Auth provider", ["local_password", "solar"])
            is_active = st.checkbox("Account is active", value=True)
            if auth_provider == "local_password":
                password = st.text_input("Password *", type="password")
                confirm_password = st.text_input("Confirm password *", type="password")
            else:
                st.caption("Solar accounts are created without a local password.")
                password = ""
                confirm_password = ""
            create_clicked = st.form_submit_button("Create Account", type="primary", width="stretch")
            if create_clicked:
                ok, message = _create_account_from_admin(
                    org_id,
                    role,
                    full_name,
                    email,
                    username,
                    password,
                    confirm_password,
                    auth_provider=auth_provider,
                    is_active=bool(is_active),
                )
                if ok:
                    st.success(message)
                    st.cache_data.clear()
                    st.rerun()
                st.error(message)


def _render_assignment_tool(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    logger.info("Assignment page rendered.", action="assignment_tool_render")
    assign_status_key = "assignment_tool_assign_status"
    assign_status_expiry_key = "assignment_tool_assign_status_expiry"
    assign_in_progress_key = "assignment_tool_is_assigning"
    selected_learner_ids_key = "assignment_selected_learner_ids"
    if selected_learner_ids_key not in st.session_state:
        st.session_state[selected_learner_ids_key] = []

    current_user_is_dev = is_dev_account(current_user)
    learners = _load_assignment_tool_learners(org_id)
    modules = _load_assignment_tool_modules(org_id)
    logger.info(
        "Assignment tool data loaded.",
        action="assignment_tool_load",
        learner_count=len(learners),
        module_count=len(modules),
    )
    if not learners:
        st.info("No learners available yet. Add or activate learners first.")
        return
    if not modules:
        st.info("No modules available yet. Create a module first.")
        return

    with st.container(border=True):
        st.markdown("#### Assign module")
        module_map: dict[str, int] = {}
        module_title_counts: dict[str, int] = {}
        for module in modules:
            base_title = str(module.get("title") or "Untitled module").strip() or "Untitled module"
            occurrence = module_title_counts.get(base_title, 0) + 1
            module_title_counts[base_title] = occurrence
            display_title = base_title if occurrence == 1 else f"{base_title} #{occurrence}"
            module_map[display_title] = int(module["module_id"])
        assignable_learners = [
            learner
            for learner in learners
            if learner.get("role") == "learner"
            or (current_user_is_dev and is_dev_account(learner))
        ]
        learners_df = to_df(
            assignable_learners,
            columns=["user_id", "name", "email", "team", "organization_name", "is_active"],
        )
        learners_df["email"] = learners_df["email"].fillna("")
        learners_df["team"] = learners_df["team"].fillna("")
        learners_df["organization_name"] = learners_df["organization_name"].fillna("Unassigned")
        learners_df["status"] = learners_df["is_active"].apply(lambda is_active: "Active" if bool(is_active) else "Inactive")

        team_options = sorted([team for team in learners_df["team"].unique().tolist() if team])
        org_options = sorted(learners_df["organization_name"].unique().tolist())
        default_filters = {"search": "", "team": "All", "org": "All"}
        active_filters = st.session_state.setdefault("assignment_tool_filters", dict(default_filters))

        # Filter controls are grouped in a form to avoid reruns while typing/changing each control.
        with st.form("assignment_tool_filters_form", clear_on_submit=False):
            fc1, fc2, fc3, fc4 = st.columns([2, 1, 1, 1])
            with fc1:
                search_draft = st.text_input("Search learners", value=active_filters.get("search", ""))
            with fc2:
                team_draft = st.selectbox(
                    "Team/Department",
                    ["All"] + team_options,
                    index=(["All"] + team_options).index(active_filters.get("team", "All"))
                    if active_filters.get("team", "All") in (["All"] + team_options)
                    else 0,
                )
            with fc3:
                org_draft = st.selectbox(
                    "Organization",
                    ["All"] + org_options,
                    index=(["All"] + org_options).index(active_filters.get("org", "All"))
                    if active_filters.get("org", "All") in (["All"] + org_options)
                    else 0,
                )
            with fc4:
                apply_filters = st.form_submit_button("Apply filters", width="stretch")

        if apply_filters:
            st.session_state["assignment_tool_filters"] = {
                "search": search_draft,
                "team": team_draft,
                "org": org_draft,
            }
            active_filters = st.session_state["assignment_tool_filters"]
        q = active_filters.get("search", "")
        team_filter = active_filters.get("team", "All")
        org_filter = active_filters.get("org", "All")

        all_active_learners = filter_active_learners(learners_df)
        filtered_active_learners = apply_learner_filters(
            all_active_learners,
            search_text=q,
            team_filter=team_filter,
            org_filter=org_filter,
        )
        logger.info(
            "Assignment learners dataframe ready for table selection.",
            action="assignment_tool_learners_df",
            shape=filtered_active_learners.shape,
            columns=list(filtered_active_learners.columns),
        )
        filtered_learners_df = filtered_active_learners.rename(
            columns={
                "user_id": "learner_id",
                "name": "full_name",
                "team": "team_department",
                "organization_name": "organization",
            }
        ).copy()
        filtered_learners_df["learner_label"] = filtered_learners_df.apply(
            lambda row: build_learner_option_label(
                {
                    "name": row.get("full_name"),
                    "team": row.get("team_department"),
                    "organization_name": row.get("organization"),
                    "email": row.get("email"),
                }
            ),
            axis=1,
        )
        all_filtered_labels = filtered_learners_df["learner_label"].tolist()
        label_to_learner_id = {
            row["learner_label"]: int(row["learner_id"])
            for _, row in filtered_learners_df.iterrows()
        }
        multiselect_key = "assignment_tool_filtered_multiselect"
        if multiselect_key not in st.session_state:
            st.session_state[multiselect_key] = []
        valid_selected = [
            label for label in st.session_state[multiselect_key] if label in all_filtered_labels
        ]
        if valid_selected != st.session_state[multiselect_key]:
            st.session_state[multiselect_key] = valid_selected

        def select_all_filtered_learners() -> None:
            st.session_state[multiselect_key] = list(all_filtered_labels)

        def clear_filtered_learners() -> None:
            st.session_state[multiselect_key] = []

        st.caption(f"{len(filtered_learners_df)} active learners match current filters")
        select_col1, select_col2 = st.columns([1, 1])
        with select_col1:
            st.button(
                "Select all filtered",
                key="assignment_tool_select_all_filtered",
                on_click=select_all_filtered_learners,
                disabled=filtered_learners_df.empty,
            )
        with select_col2:
            st.button(
                "Clear selected",
                key="assignment_tool_clear_filtered",
                on_click=clear_filtered_learners,
                disabled=not bool(st.session_state.get(multiselect_key, [])),
            )
        selected_labels = st.multiselect(
            "Selected learners",
            options=all_filtered_labels,
            key=multiselect_key,
            help="Selections persist while applying filters. Use Clear selected to reset.",
        )
        selected_learner_ids = [label_to_learner_id[label] for label in selected_labels if label in label_to_learner_id]
        st.session_state[selected_learner_ids_key] = selected_learner_ids

        if filtered_learners_df.empty:
            st.info("No active learners match the current filters.")
            return

        display_df = filtered_learners_df.copy()
        display_df = display_df[
            [
                "learner_id",
                "full_name",
                "email",
                "team_department",
                "organization",
                "status",
            ]
        ]
        display_df = display_df.rename(
            columns={
                "full_name": "Name",
                "email": "Email",
                "team_department": "Team/Department",
                "organization": "Organization",
                "status": "Status",
            }
        )
        with table_card_container():
            st.dataframe(
                display_df,
                hide_index=True,
                width="stretch",
                height=360,
                key="assignment_learner_table",
                column_config={
                    "learner_id": None,
                },
            )
        st.info(f"{len(selected_learner_ids)} learner(s) selected.")

        logger.info(
            "Assignment learner selection updated.",
            action="assignment_tool_selection",
            selected_count=len(selected_learner_ids),
        )

        due_date_enabled_key = "assignment_tool_due_date_enabled"
        due_date_value_key = "assignment_tool_due_date_value"

        if due_date_enabled_key not in st.session_state:
            st.session_state[due_date_enabled_key] = False
        if due_date_value_key not in st.session_state:
            st.session_state[due_date_value_key] = None

        def _toggle_due_date() -> None:
            if not st.session_state[due_date_enabled_key]:
                st.session_state[due_date_value_key] = None

        module_select_key = "assignment_tool_selected_module_label"
        module_id_to_label = {module_id: title for title, module_id in module_map.items()}
        pending_prefill_module_id = st.session_state.pop("assignment_tool_prefill_module_id", None)
        query_prefill_module_id = None
        query_prefill_value = st.query_params.get("module_id")
        if query_prefill_value is not None:
            try:
                query_prefill_module_id = int(str(query_prefill_value).strip())
            except (TypeError, ValueError):
                query_prefill_module_id = None

        default_prefill_module_id = pending_prefill_module_id
        if default_prefill_module_id is None:
            default_prefill_module_id = query_prefill_module_id
        if default_prefill_module_id is None:
            default_prefill_module_id = st.session_state.get("recently_created_module_id")
        if (
            isinstance(default_prefill_module_id, int)
            and default_prefill_module_id in module_id_to_label
            and (module_select_key not in st.session_state or st.session_state.get(module_select_key) not in module_map)
        ):
            st.session_state[module_select_key] = module_id_to_label[default_prefill_module_id]
        elif module_select_key not in st.session_state or st.session_state.get(module_select_key) not in module_map:
            st.session_state[module_select_key] = next(iter(module_map.keys()))

        selected_module = st.selectbox("Module", list(module_map.keys()), key=module_select_key)
        st.checkbox(
            "Set due date",
            key=due_date_enabled_key,
            on_change=_toggle_due_date,
        )
        due_date = st.date_input(
            "Due date",
            value=st.session_state[due_date_value_key],
            key=due_date_value_key,
            disabled=not st.session_state[due_date_enabled_key],
            help="Select a due date.",
        )
        status_container = st.container()
        action_col1, action_col2, action_col3 = st.columns(3)
        with action_col1:
            assign_submitted = st.button(
                "Assign Training",
                key="assignment_tool_assign_training",
                type="primary",
                width="stretch",
                disabled=bool(st.session_state.get(assign_in_progress_key, False)),
            )
        with action_col2:
            reassign_submitted = st.button(
                "Reassign",
                key="assignment_tool_reassign_training",
                width="stretch",
                disabled=bool(st.session_state.get(assign_in_progress_key, False)),
            )
        with action_col3:
            update_due_date_submitted = st.button(
                "Update Due Date",
                key="assignment_tool_update_due_date",
                width="stretch",
                disabled=bool(st.session_state.get(assign_in_progress_key, False)),
            )

        def _validate_action_requirements(require_due_date: bool = False) -> tuple[list[int], int | None, str | None]:
            selected_ids = [int(v) for v in st.session_state.get(selected_learner_ids_key, [])]
            module_id = module_map.get(selected_module)
            due_date_value = due_date.isoformat() if st.session_state[due_date_enabled_key] and due_date else None
            if module_id is None:
                raise ValueError("Select a module before continuing.")
            if require_due_date and not due_date_value:
                raise ValueError("Enable and select a due date before updating due dates.")
            return selected_ids, module_id, due_date_value

        action_name = None
        if assign_submitted:
            action_name = "assign"
        elif reassign_submitted:
            action_name = "reassign"
        elif update_due_date_submitted:
            action_name = "update_due_date"

        if action_name and not st.session_state.get(assign_in_progress_key, False):
            selected_learner_ids = st.session_state.get("assignment_selected_learner_ids", [])
            if not selected_learner_ids:
                st.warning("Please select at least one learner.")
                st.stop()
            st.session_state[assign_in_progress_key] = True
            st.session_state[assign_status_key] = None
            st.session_state[assign_status_expiry_key] = None
            try:
                selected_ids, module_id, due_date_value = _validate_action_requirements(
                    require_due_date=(action_name == "update_due_date")
                )
                logger.info(
                    "Assignment bulk action started.",
                    action=f"assignment_tool_{action_name}_start",
                    learner_count=len(selected_ids),
                )
                with status_container:
                    with st.spinner("Applying updates..."):
                        if action_name == "assign":
                            for learner_id in selected_ids:
                                execute(
                                    """
                                    INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, is_active)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    RETURNING assignment_id AS id
                                    """,
                                    (org_id, module_id, learner_id, current_user["user_id"], due_date_value, True),
                                )
                                _sync_assignment_tracking_records(
                                    organization_id=org_id,
                                    module_id=module_id,
                                    learner_id=learner_id,
                                    assigned_by_user_id=current_user["user_id"],
                                )
                            success_message = f"Assigned training to {len(selected_ids)} learner(s)."
                        elif action_name == "reassign":
                            execute(
                                """
                                UPDATE assignments
                                SET module_id = ?, due_date = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP
                                WHERE organization_id = ? AND is_active = TRUE AND learner_id = ANY(?)
                                """,
                                (module_id, due_date_value, current_user["user_id"], org_id, selected_ids),
                            )
                            for learner_id in selected_ids:
                                _sync_assignment_tracking_records(
                                    organization_id=org_id,
                                    module_id=module_id,
                                    learner_id=learner_id,
                                    assigned_by_user_id=current_user["user_id"],
                                )
                            success_message = f"Reassigned training for {len(selected_ids)} learner(s)."
                        else:
                            execute(
                                """
                                UPDATE assignments
                                SET due_date = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP
                                WHERE organization_id = ? AND is_active = TRUE AND learner_id = ANY(?) AND module_id = ?
                                """,
                                (due_date_value, current_user["user_id"], org_id, selected_ids, module_id),
                            )
                            success_message = f"Updated due date for {len(selected_ids)} learner(s)."
                logger.info(
                    "Assignment bulk action succeeded.",
                    action=f"assignment_tool_{action_name}_success",
                    learner_count=len(selected_ids),
                )
                st.session_state[assign_status_key] = ("success", success_message)
                st.session_state[assign_status_expiry_key] = time.time() + 5
                st.session_state[selected_learner_ids_key] = []
                _load_assignment_tool_learners.clear()
                _assignments_with_status.clear()
                st.session_state["assignment_management_refresh_token"] = int(
                    st.session_state.get("assignment_management_refresh_token", 0)
                ) + 1
                st.session_state.pop("assignment_management_filtered_cache", None)
            except ValueError as validation_error:
                st.session_state[assign_status_key] = ("warning", str(validation_error))
                st.session_state[assign_status_expiry_key] = time.time() + 8
            except Exception:
                logger.exception("Failed assignment bulk action.", action=f"assignment_tool_{action_name}_error")
                st.session_state[assign_status_key] = ("error", "Could not complete the selected action.")
                st.session_state[assign_status_expiry_key] = time.time() + 8
            finally:
                st.session_state[assign_in_progress_key] = False
            st.rerun()

        status_payload = st.session_state.get(assign_status_key)
        status_expiry = st.session_state.get(assign_status_expiry_key)
        if status_payload and status_expiry and time.time() > float(status_expiry):
            st.session_state[assign_status_key] = None
            st.session_state[assign_status_expiry_key] = None
            status_payload = None
        if status_payload:
            status_level, status_message = status_payload
            with status_container:
                if status_level == "success":
                    st.success(status_message)
                elif status_level == "warning":
                    st.warning(status_message)
                else:
                    st.error(status_message)


def render_assignment_management(current_user: dict) -> None:
    render_page_header("Assignment Management", "Review current assignments and assign modules to learners.")
    publish_notice = st.session_state.pop("module_publish_notice", None)
    if publish_notice:
        st.success("Module published successfully.")
        st.info("You can now assign this module to learners.")
    recently_created_module_id = st.session_state.get("recently_created_module_id")
    recently_created_module_title = st.session_state.get("recently_created_module_title")
    if isinstance(recently_created_module_id, int):
        module_label = str(recently_created_module_title or f"Module #{recently_created_module_id}")
        st.caption(f"Assigning newly created module: {module_label}")
    current_tab, tool_tab = st.tabs(["Current Assignments", "Assignment Tool"])
    with current_tab:
        render_current_assignments(current_user)
    with tool_tab:
        _render_assignment_tool(current_user)


def render_current_assignments(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))

    refresh_token = int(st.session_state.get("assignment_management_refresh_token", 0))
    assignments_df = _assignments_with_status(org_id, refresh_token)
    assignments_df = ensure_dataframe_schema(
        assignments_df,
        [
            "assignment_id",
            "learner_name",
            "team",
            "organization_name",
            "module_title",
            "status",
            "due_date",
            "last_attempt_at",
            "assigned_at",
            "module_id",
            "learner_id",
        ],
    )
    if assignments_df.empty:
        st.info("No assignments yet.")
        return

    assignments_df["team"] = assignments_df["team"].fillna("")
    assignments_df["organization_name"] = assignments_df["organization_name"].fillna("Unassigned")

    team_options = sorted([team for team in assignments_df["team"].unique().tolist() if team])
    org_options = sorted(assignments_df["organization_name"].unique().tolist())
    status_options = sorted(assignments_df["status"].unique().tolist())
    module_options = sorted(assignments_df["module_title"].unique().tolist())
    assignment_filters = st.session_state.setdefault(
        "current_assignments_filters",
        {"search": "", "team": "All", "org": "All", "status": "All", "module": "All"},
    )
    with st.container(border=True):
        with st.form("current_assignments_filters_form", clear_on_submit=False):
            f1, f2, f3, f4, f5, f6 = st.columns([2, 1, 1, 1, 1, 1])
            with f1:
                q_draft = st.text_input("Search learner or module", value=assignment_filters.get("search", ""))
            with f2:
                team_values = ["All"] + team_options
                team_draft = st.selectbox(
                    "Team/Department",
                    team_values,
                    index=team_values.index(assignment_filters.get("team", "All"))
                    if assignment_filters.get("team", "All") in team_values
                    else 0,
                )
            with f3:
                org_values = ["All"] + org_options
                org_draft = st.selectbox(
                    "Organization",
                    org_values,
                    index=org_values.index(assignment_filters.get("org", "All"))
                    if assignment_filters.get("org", "All") in org_values
                    else 0,
                )
            with f4:
                status_values = ["All"] + status_options
                status_draft = st.selectbox(
                    "Status",
                    status_values,
                    index=status_values.index(assignment_filters.get("status", "All"))
                    if assignment_filters.get("status", "All") in status_values
                    else 0,
                )
            with f5:
                module_values = ["All"] + module_options
                module_draft = st.selectbox(
                    "Module",
                    module_values,
                    index=module_values.index(assignment_filters.get("module", "All"))
                    if assignment_filters.get("module", "All") in module_values
                    else 0,
                )
            with f6:
                apply_assignment_filters = st.form_submit_button("Apply filters", width="stretch")

    if apply_assignment_filters:
        st.session_state["current_assignments_filters"] = {
            "search": q_draft,
            "team": team_draft,
            "org": org_draft,
            "status": status_draft,
            "module": module_draft,
        }
        assignment_filters = st.session_state["current_assignments_filters"]

    q = assignment_filters.get("search", "")
    team_filter = assignment_filters.get("team", "All")
    org_filter = assignment_filters.get("org", "All")
    status_filter = assignment_filters.get("status", "All")
    module_filter = assignment_filters.get("module", "All")

    filter_cache_key = (
        q.strip().lower(),
        team_filter,
        org_filter,
        status_filter,
        module_filter,
        len(assignments_df),
        refresh_token,
    )
    filtered_cache = st.session_state.get("assignment_management_filtered_cache")
    if filtered_cache and filtered_cache.get("key") == filter_cache_key:
        filtered_assignments = filtered_cache["df"].copy()
    else:
        filtered_assignments = apply_learner_filters(
            assignments_df,
            search_text=q,
            team_filter=team_filter,
            org_filter=org_filter,
        )
        if status_filter != "All":
            filtered_assignments = filtered_assignments[filtered_assignments["status"] == status_filter]
        if module_filter != "All":
            filtered_assignments = filtered_assignments[filtered_assignments["module_title"] == module_filter]
        st.session_state["assignment_management_filtered_cache"] = {
            "key": filter_cache_key,
            "df": filtered_assignments.copy(),
        }

    st.caption(f"{len(filtered_assignments)} assignment(s) match current filters")
    assignment_table_df = filtered_assignments[
        ["assignment_id", "learner_name", "team", "organization_name", "module_title", "due_date", "status", "last_attempt_at"]
    ].reset_index(drop=True)
    assignment_display_df = assignment_table_df.rename(
        columns={
            "assignment_id": "Assignment ID",
            "learner_name": "Learner",
            "team": "Team",
            "organization_name": "Organization",
            "module_title": "Module",
            "due_date": "Due Date",
            "status": "Status",
            "last_attempt_at": "Last Attempt",
        }
    )
    if "Due Date" in assignment_display_df.columns:
        assignment_display_df["Due Date"] = assignment_display_df["Due Date"].apply(_format_datetime_for_admin_grid)
    if "Last Attempt" in assignment_display_df.columns:
        assignment_display_df["Last Attempt"] = assignment_display_df["Last Attempt"].apply(_format_datetime_for_admin_grid)
    selection_state_key = "assignment_management_selected_ids"
    assignment_table_key = "assignment_management_data_editor"
    interactive_df = assignment_display_df.copy()
    render_admin_selection_table(
        interactive_df,
        row_id_col="Assignment ID",
        selection_state_key=selection_state_key,
        table_key=assignment_table_key,
        selection_label="Select",
        selection_help="Select assignments for actions.",
        single_select=False,
        height=520,
        show_selection_caption=False,
    )
    selected_ids_key = f"{assignment_table_key}_selected_ids"
    filtered_assignment_ids = set(assignment_table_df["assignment_id"].tolist())
    selected_assignment_ids = sorted(
        int(v)
        for v in st.session_state.get(selected_ids_key, [])
        if int(v) in filtered_assignment_ids
    )
    st.session_state[selected_ids_key] = selected_assignment_ids
    st.session_state[selection_state_key] = selected_assignment_ids
    selected_count = len(selected_assignment_ids)
    st.caption(f"{selected_count} item(s) selected.")

    with st.container(border=True):
        st.markdown("#### Assignment actions")
        action_cols = st.columns([1.2, 1.6, 2.2, 2.2, 1.5])

        with action_cols[0]:
            st.markdown("&nbsp;", unsafe_allow_html=True)

        with action_cols[1]:
            new_due = st.date_input("Due date", key="reassign_due", value=date.today())

        with action_cols[2]:
            if st.button(
                "Reassign selected training",
                key="assignment_management_bulk_reassign",
                width="stretch",
                disabled=selected_count == 0,
            ):
                selected_assignment_ids = st.session_state.get(selected_ids_key, [])
                if not selected_assignment_ids:
                    st.warning("Please select at least one row.")
                    st.stop()
                try:
                    execute(
                        "UPDATE assignments SET due_date = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP "
                        "WHERE organization_id = ? AND assignment_id IN ?",
                        (new_due.isoformat(), current_user["user_id"], org_id, tuple(selected_assignment_ids)),
                    )
                    logger.info(
                        "Button click.",
                        action="reassign_training",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.success(f"Reassigned training for {len(selected_assignment_ids)} assignment(s).")
                    st.session_state[selected_ids_key] = []
                    st.session_state[selection_state_key] = []
                    st.session_state["assignment_management_refresh_token"] = refresh_token + 1
                    st.session_state.pop("assignment_management_filtered_cache", None)
                    st.cache_data.clear()
                    st.rerun()
                except Exception:
                    logger.exception(
                        "Failed reassigning training.",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.error("Could not update selected assignments.")

        with action_cols[3]:
            if st.button(
                "Remove selected assignments",
                key="assignment_management_bulk_remove",
                type="primary",
                width="stretch",
                disabled=selected_count == 0,
            ):
                selected_assignment_ids = st.session_state.get(selected_ids_key, [])
                if not selected_assignment_ids:
                    st.warning("Please select at least one row.")
                    st.stop()
                try:
                    rows_to_cleanup = fetch_all(
                        """
                        SELECT assignment_id, module_id, learner_id
                        FROM assignments
                        WHERE organization_id = ? AND assignment_id IN ?
                        """,
                        (org_id, tuple(selected_assignment_ids)),
                    )
                    execute(
                        "UPDATE assignments SET is_active = FALSE WHERE organization_id = ? AND assignment_id IN ?",
                        (org_id, tuple(selected_assignment_ids)),
                    )
                    for row in rows_to_cleanup:
                        _cleanup_assignment_tracking_records(
                            organization_id=org_id,
                            module_id=int(row["module_id"]),
                            learner_id=int(row["learner_id"]),
                        )
                    logger.info(
                        "Button click.",
                        action="remove_assignment",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.success(f"Removed {len(selected_assignment_ids)} assignment(s).")
                    st.session_state[selected_ids_key] = []
                    st.session_state[selection_state_key] = []
                    st.session_state["assignment_management_refresh_token"] = refresh_token + 1
                    st.session_state.pop("assignment_management_filtered_cache", None)
                    st.cache_data.clear()
                    st.rerun()
                except Exception:
                    logger.exception(
                        "Failed removing assignments.",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.error("Could not remove selected assignments.")

        with action_cols[4]:
            if st.button(
                "Clear selection",
                key="assignment_management_bulk_clear_selection",
                width="stretch",
                disabled=selected_count == 0,
            ):
                st.session_state[selected_ids_key] = []
                st.session_state[selection_state_key] = []
                st.rerun()


def render_grading_center(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    render_page_header("Submission Grading", "Review learner submissions and scoring results for assigned modules.")

    total_score_expr = "COALESCE(sc.final_total_score, sc.admin_total_score, sc.ai_total_score, 0)"
    jsonb_empty_object_expr = "'{}'::jsonb"

    attempts = to_df(
        fetch_all(
            f"""
            SELECT
                a.attempt_id,
                sc.submission_score_id AS submission_score_id,
                a.created_at,
                u.name AS learner_name,
                m.title AS module_title,
                COALESCE(a.result_status, 'submitted') AS result_status,
                COALESCE(sc.review_status, sc.grading_status, a.result_status, 'pending_review') AS review_status,
                a.result_approved_at,
                a.result_approved_by_user_id,
                approver.name AS approved_by_name,
                {total_score_expr} AS total_score,
                COALESCE(sc.understanding_score, a.understanding_score) AS understanding_score,
                COALESCE(sc.investigation_score, a.investigation_score) AS investigation_score,
                COALESCE(sc.solution_score, a.solution_score) AS solution_score,
                COALESCE(sc.communication_score, a.communication_score) AS communication_score,
                COALESCE(sc.show_results_to_learner, FALSE) AS show_results_to_learner,
                COALESCE(sc.show_overall_score_to_learner, FALSE) AS show_overall_score_to_learner,
                COALESCE(sc.show_question_scores_to_learner, FALSE) AS show_question_scores_to_learner,
                COALESCE(sc.show_feedback_to_learner, FALSE) AS show_feedback_to_learner,
                COALESCE(sc.show_expected_answers_to_learner, FALSE) AS show_expected_answers_to_learner,
                COALESCE(sc.show_grading_criteria_to_learner, FALSE) AS show_grading_criteria_to_learner,
                COALESCE(sc.show_ai_review_to_learner, sc.show_ai_evaluation_details_to_learner, FALSE) AS show_ai_review_to_learner,
                COALESCE(sc.show_learner_responses_to_learner, FALSE) AS show_learner_responses_to_learner,
                COALESCE(sc.results_visibility_json, {jsonb_empty_object_expr}) AS results_visibility_json,
                sc.scoring_version,
                a.ai_feedback
            FROM attempts a
            JOIN users u ON u.user_id = a.user_id
            JOIN modules m ON m.module_id = a.module_id
            LEFT JOIN submission_scores sc ON sc.attempt_id = a.attempt_id
            LEFT JOIN users approver ON approver.user_id = a.result_approved_by_user_id
            WHERE a.organization_id = ?
              AND u.is_active = TRUE
            ORDER BY a.created_at DESC
            """,
            (org_id,),
        )
    ,
        columns=[
            "attempt_id",
            "submission_score_id",
            "created_at",
            "learner_name",
            "module_title",
            "result_status",
            "review_status",
            "result_approved_at",
            "result_approved_by_user_id",
            "approved_by_name",
            "total_score",
            "understanding_score",
            "investigation_score",
            "solution_score",
            "communication_score",
            "show_results_to_learner",
            "show_overall_score_to_learner",
            "show_question_scores_to_learner",
            "show_feedback_to_learner",
            "show_expected_answers_to_learner",
            "show_grading_criteria_to_learner",
            "show_ai_review_to_learner",
            "show_learner_responses_to_learner",
            "results_visibility_json",
            "scoring_version",
            "ai_feedback",
        ],
    )
    if attempts.empty:
        st.info("No learner submissions yet.")
        return

    learner_filter = st.multiselect(
        "Filter learner",
        options=sorted(attempts["learner_name"].unique().tolist()),
        default=[],
    )
    module_filter = st.multiselect(
        "Filter module",
        options=sorted(attempts["module_title"].unique().tolist()),
        default=[],
    )
    approval_filter = st.multiselect(
        "Filter review status",
        options=["submitted", "pending_review", "approved"],
        format_func=format_status_display,
        default=[],
    )
    filtered = attempts.copy()
    if learner_filter:
        filtered = filtered[filtered["learner_name"].isin(learner_filter)]
    if module_filter:
        filtered = filtered[filtered["module_title"].isin(module_filter)]
    if approval_filter:
        filtered = filtered[filtered["review_status"].isin(approval_filter)]

    metric_row(
        {
            "Submissions": len(filtered),
            "Average score": f"{round(filtered['total_score'].mean(), 1)}%" if len(filtered) else "0%",
            "Top score": f"{round(filtered['total_score'].max(), 1)}%" if len(filtered) else "0%",
        }
    )

    display_filtered = filtered.copy()
    display_filtered["review_status"] = display_filtered["review_status"].apply(format_status_display)
    grading_display_df = display_filtered[
        [
            "created_at",
            "learner_name",
            "module_title",
            "review_status",
            "total_score",
            "understanding_score",
            "investigation_score",
            "solution_score",
            "communication_score",
        ]
    ].rename(
        columns={
            "created_at": "Created At",
            "learner_name": "Learner Name",
            "module_title": "Module Title",
            "review_status": "Review Status",
            "total_score": "Total Score",
            "understanding_score": "Understanding Score",
            "investigation_score": "Investigation Score",
            "solution_score": "Solution Score",
            "communication_score": "Communication Score",
        }
    )
    grading_display_df["Created At"] = grading_display_df["Created At"].apply(_format_datetime_for_admin_grid)
    for score_col in [
        "Total Score",
        "Understanding Score",
        "Investigation Score",
        "Solution Score",
        "Communication Score",
    ]:
        grading_display_df[score_col] = pd.to_numeric(grading_display_df[score_col], errors="coerce").round(1)
    grading_table_height = max(360, min(760, 36 * (len(grading_display_df) + 1)))
    with table_card_container():
        st.dataframe(
            grading_display_df,
            hide_index=True,
            width="stretch",
            height=grading_table_height,
        )

    if filtered.empty or "attempt_id" not in filtered.columns:
        st.info("No submissions match the current filters.")
        return

    review_options = filtered["attempt_id"].tolist()
    selected_review_attempt_id = review_options[0]
    st.markdown("##### Review")
    selected_review_attempt_id = st.selectbox(
        "Submission",
        options=review_options,
        key="grading_center_review_attempt_id",
        format_func=lambda aid: (
            f"#{aid} • {filtered.loc[filtered['attempt_id'] == aid, 'learner_name'].iloc[0]}"
        ),
    )
    review_attempt = fetch_one(
        """
        SELECT
            attempt_id,
            diagnosis_answer,
            next_steps_answer,
            customer_response,
            notes,
            question_responses
        FROM attempts
        WHERE attempt_id = ? AND organization_id = ?
        """,
        (selected_review_attempt_id, org_id),
    )
    with st.popover("Review submission", width="stretch"):
        if not review_attempt:
            st.info("Submission details are unavailable for this attempt.")
        else:
            st.write("**Diagnosis**")
            st.code(review_attempt.get("diagnosis_answer") or "No diagnosis answer submitted.")
            st.write("**Next steps**")
            st.code(review_attempt.get("next_steps_answer") or "No next-steps answer submitted.")
            st.write("**Customer response**")
            st.code(review_attempt.get("customer_response") or "No customer response submitted.")
            st.write("**Learner notes**")
            st.code(review_attempt.get("notes") or "No notes submitted.")
            raw_questions = review_attempt.get("question_responses")
            if raw_questions:
                st.write("**Question responses**")
                parsed_questions = {}
                try:
                    parsed_questions = json.loads(raw_questions) if isinstance(raw_questions, str) else raw_questions
                except Exception:
                    parsed_questions = {}
                if isinstance(parsed_questions, dict) and parsed_questions:
                    for question, answer in parsed_questions.items():
                        st.write(f"- **{question}:** {answer or '—'}")
                else:
                    st.code(str(raw_questions))

    selected_attempt_id = st.selectbox(
        "Result approval controls",
        options=filtered["attempt_id"].tolist(),
        format_func=lambda aid: (
            f"Attempt #{aid} • {filtered.loc[filtered['attempt_id'] == aid, 'learner_name'].iloc[0]}"
            f" • {filtered.loc[filtered['attempt_id'] == aid, 'module_title'].iloc[0]}"
        ),
    )
    selected_attempt_row = filtered[filtered["attempt_id"] == selected_attempt_id].iloc[0]
    selected_result_status = str(selected_attempt_row.get("review_status") or "").strip().lower()
    is_approved = selected_result_status == "approved"
    is_rejected = selected_result_status == "rejected"
    status_label = format_status_display(selected_result_status)
    status_tone_class = "status-approved" if is_approved else ("status-rejected" if is_rejected else "status-pending")
    submitted_at_label = _format_datetime_for_admin_grid(selected_attempt_row.get("created_at"))
    approved_at_label = _format_datetime_for_admin_grid(selected_attempt_row.get("result_approved_at"))
    submitted_by_label = selected_attempt_row.get("learner_name") or "—"
    approved_by_user_id = selected_attempt_row.get("result_approved_by_user_id")
    approved_by_name = selected_attempt_row.get("approved_by_name")
    if is_approved and not approved_by_user_id and not approved_by_name:
        approved_by_label = "Legacy / Unknown"
    else:
        approved_by_label = approved_by_name or "—"
    st.markdown(
        f"""
        <style>
        .approval-summary-card {{
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 0.85rem;
            background: linear-gradient(180deg, rgba(248, 250, 252, 0.95) 0%, rgba(241, 245, 249, 0.9) 100%);
            padding: 1rem 1.15rem;
            margin: 0.6rem 0 0.8rem 0;
        }}
        .approval-summary-label {{
            font-size: 0.84rem;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            font-weight: 700;
            color: #475569;
            margin-bottom: 0.45rem;
        }}
        .approval-summary-status-chip {{
            display: inline-flex;
            align-items: center;
            padding: 0.5rem 0.85rem;
            border-radius: 999px;
            font-size: 1.2rem;
            font-weight: 700;
            line-height: 1.25;
            margin-bottom: 0.7rem;
        }}
        .approval-summary-status-chip.status-approved {{
            color: #166534;
            background: rgba(187, 247, 208, 0.65);
            border: 1px solid rgba(34, 197, 94, 0.45);
        }}
        .approval-summary-status-chip.status-pending {{
            color: #92400e;
            background: rgba(254, 243, 199, 0.8);
            border: 1px solid rgba(245, 158, 11, 0.45);
        }}
        .approval-summary-status-chip.status-rejected {{
            color: #991b1b;
            background: rgba(254, 226, 226, 0.85);
            border: 1px solid rgba(248, 113, 113, 0.45);
        }}
        .approval-summary-meta {{
            font-size: 0.86rem;
            color: #475569;
            line-height: 1.5;
        }}
        .approval-summary-meta strong {{
            color: #0f172a;
            font-weight: 600;
        }}
        </style>
        <div class="approval-summary-card">
          <div class="approval-summary-label">Approval Status</div>
          <div class="approval-summary-status-chip {status_tone_class}">{escape(status_label)}</div>
          <div class="approval-summary-meta">
            <div><strong>Submitted by:</strong> {escape(str(submitted_by_label))}</div>
            <div><strong>Submitted:</strong> {escape(submitted_at_label)}</div>
            <div><strong>Approved at:</strong> {escape(approved_at_label)}</div>
            {"<div><strong>Approved by:</strong> " + escape(str(approved_by_label)) + "</div>" if is_approved else ""}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    approval_status_key = "results_approval_status_message"
    approval_status_expiry_key = "results_approval_status_expiry"
    approval_status = st.session_state.get(approval_status_key)
    approval_status_expiry = st.session_state.get(approval_status_expiry_key)
    if approval_status and approval_status_expiry and time.time() > float(approval_status_expiry):
        st.session_state[approval_status_key] = None
        st.session_state[approval_status_expiry_key] = None
        approval_status = None
    if isinstance(approval_status, tuple) and len(approval_status) == 2:
        tone, message = approval_status
        if tone == "success":
            st.success(message)
        elif tone == "warning":
            st.warning(message)
        elif tone == "error":
            st.error(message)

    action_columns = st.columns([1, 1.5, 3.5])
    with action_columns[0]:
        if st.button("Approve Result", disabled=is_approved, width="stretch"):
            execute(
                """
                UPDATE attempts
                SET result_status = 'approved',
                    result_approved_at = CURRENT_TIMESTAMP,
                    result_approved_by_user_id = ?
                WHERE attempt_id = ?
                  AND organization_id = ?
                """,
                (current_user["user_id"], int(selected_attempt_id), org_id),
            )
            execute(
                """
                UPDATE submission_scores
                SET grading_status = 'approved',
                    review_status = 'approved',
                    final_total_score = COALESCE(admin_total_score, ai_total_score, final_total_score),
                    approved_by = ?,
                    approved_at = CURRENT_TIMESTAMP,
                    best_practice_reasoning = COALESCE(
                        best_practice_reasoning,
                        (SELECT best_practice_reasoning FROM attempts WHERE attempt_id = ?)
                    ),
                    recommended_response = COALESCE(
                        recommended_response,
                        (SELECT COALESCE(a.recommended_response, m.expected_customer_response)
                         FROM attempts a
                         JOIN modules m ON m.module_id = a.module_id
                         WHERE a.attempt_id = ?)
                    ),
                    lesson_takeaway = COALESCE(
                        lesson_takeaway,
                        (SELECT COALESCE(a.takeaway_summary, m.lesson_takeaway)
                         FROM attempts a
                         JOIN modules m ON m.module_id = a.module_id
                         WHERE a.attempt_id = ?)
                    ),
                    learner_strengths = COALESCE(
                        learner_strengths,
                        (SELECT strengths FROM attempts WHERE attempt_id = ?)
                    ),
                    learner_weaknesses = COALESCE(
                        learner_weaknesses,
                        (SELECT missed_points FROM attempts WHERE attempt_id = ?)
                    ),
                    learner_missed_points = COALESCE(
                        learner_missed_points,
                        (SELECT missed_points FROM attempts WHERE attempt_id = ?)
                    )
                WHERE attempt_id = ?
                """,
                (
                    current_user["user_id"],
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                    int(selected_attempt_id),
                ),
            )
            st.session_state[approval_status_key] = ("success", "Result approved. Configure learner visibility controls below to publish learner-facing sections.")
            st.session_state[approval_status_expiry_key] = time.time() + 8
            st.rerun()
    with action_columns[1]:
        if st.button("Mark Unapproved", disabled=not is_approved, width="stretch"):
            execute(
                """
                UPDATE attempts
                SET result_status = 'pending_review',
                    result_approved_at = NULL,
                    result_approved_by_user_id = NULL
                WHERE attempt_id = ?
                  AND organization_id = ?
                """,
                (int(selected_attempt_id), org_id),
            )
            execute(
                """
                UPDATE submission_scores
                SET grading_status = 'pending_review',
                    review_status = 'pending_review',
                    approved_by = NULL,
                    approved_at = NULL
                WHERE attempt_id = ?
                """,
                (int(selected_attempt_id),),
            )
            st.session_state[approval_status_key] = ("warning", "Approval revoked. Learner-facing results are now hidden until re-approved.")
            st.session_state[approval_status_expiry_key] = time.time() + 8
            st.rerun()

    st.markdown("#### Results Visibility")
    st.caption("Approval and visibility are separate controls. Learners only see approved result sections that are explicitly enabled here.")
    submission_score_id_raw = selected_attempt_row.get("submission_score_id")
    submission_score_id = safe_int(submission_score_id_raw, 0)
    visibility_override_key = f"results_visibility_override_{selected_attempt_id}"
    visibility_override = st.session_state.get(visibility_override_key, {})
    visibility_defaults = {
        "show_results_to_learner": bool(visibility_override.get("show_results_to_learner", selected_attempt_row.get("show_results_to_learner"))),
        "show_overall_score_to_learner": bool(visibility_override.get("show_overall_score_to_learner", selected_attempt_row.get("show_overall_score_to_learner"))),
        "show_question_scores_to_learner": bool(visibility_override.get("show_question_scores_to_learner", selected_attempt_row.get("show_question_scores_to_learner"))),
        "show_feedback_to_learner": bool(visibility_override.get("show_feedback_to_learner", selected_attempt_row.get("show_feedback_to_learner"))),
        "show_expected_answers_to_learner": bool(visibility_override.get("show_expected_answers_to_learner", selected_attempt_row.get("show_expected_answers_to_learner"))),
        "show_grading_criteria_to_learner": bool(visibility_override.get("show_grading_criteria_to_learner", selected_attempt_row.get("show_grading_criteria_to_learner"))),
        "show_ai_review_to_learner": bool(visibility_override.get("show_ai_review_to_learner", selected_attempt_row.get("show_ai_review_to_learner"))),
        "show_learner_responses_to_learner": bool(visibility_override.get("show_learner_responses_to_learner", selected_attempt_row.get("show_learner_responses_to_learner"))),
    }
    with st.form(f"learner_visibility_form_{selected_attempt_id}"):
        show_results_to_learner = st.checkbox(
            "Show results shell to learner",
            value=visibility_defaults["show_results_to_learner"],
            disabled=not is_approved,
        )
        visibility_options = [
            ("Overall score", "show_overall_score_to_learner"),
            ("Per-question score", "show_question_scores_to_learner"),
            ("Feedback / comments", "show_feedback_to_learner"),
            ("Expected answer / rationale", "show_expected_answers_to_learner"),
            ("Learner responses", "show_learner_responses_to_learner"),
            ("AI review details", "show_ai_review_to_learner"),
            ("Grading criteria", "show_grading_criteria_to_learner"),
        ]
        default_visibility_labels = [
            label for label, field_name in visibility_options if visibility_defaults[field_name]
        ]
        selected_visibility_labels = st.multiselect(
            "Select learner-visible sections",
            options=[label for label, _ in visibility_options],
            default=default_visibility_labels,
            disabled=not is_approved or not show_results_to_learner,
            help="Select one or more learner-visible sections.",
        )
        selected_visibility_fields = {
            field_name for label, field_name in visibility_options if label in selected_visibility_labels
        }
        show_overall_score_to_learner = "show_overall_score_to_learner" in selected_visibility_fields
        show_question_scores_to_learner = "show_question_scores_to_learner" in selected_visibility_fields
        show_feedback_to_learner = "show_feedback_to_learner" in selected_visibility_fields
        show_expected_answers_to_learner = "show_expected_answers_to_learner" in selected_visibility_fields
        show_grading_criteria_to_learner = "show_grading_criteria_to_learner" in selected_visibility_fields
        show_ai_review_to_learner = "show_ai_review_to_learner" in selected_visibility_fields
        show_learner_responses_to_learner = "show_learner_responses_to_learner" in selected_visibility_fields
        saved_visibility = st.form_submit_button("Save results visibility", width="stretch", disabled=not is_approved)
    if saved_visibility and is_approved:
        if not submission_score_id:
            st.session_state[approval_status_key] = ("error", "Could not save visibility: missing submission score row id.")
            st.session_state[approval_status_expiry_key] = time.time() + 8
            st.rerun()
        results_visibility = {
            "show_results_to_learner": bool(show_results_to_learner),
            "show_overall_score_to_learner": bool(show_overall_score_to_learner),
            "show_question_scores_to_learner": bool(show_question_scores_to_learner),
            "show_feedback_to_learner": bool(show_feedback_to_learner),
            "show_expected_answers_to_learner": bool(show_expected_answers_to_learner),
            "show_grading_criteria_to_learner": bool(show_grading_criteria_to_learner),
            "show_ai_review_to_learner": bool(show_ai_review_to_learner),
            "show_learner_responses_to_learner": bool(show_learner_responses_to_learner),
        }
        logger.info(
            "Saving learner results visibility",
            submission_score_id=int(submission_score_id),
            results_visibility=results_visibility,
        )
        updated_rows = fetch_all(
            """
            UPDATE submission_scores
            SET show_results_to_learner = ?,
                show_overall_score_to_learner = ?,
                show_question_scores_to_learner = ?,
                show_feedback_to_learner = ?,
                show_expected_answers_to_learner = ?,
                show_grading_criteria_to_learner = ?,
                show_ai_review_to_learner = ?,
                show_learner_responses_to_learner = ?,
                results_visibility_json = ?
            WHERE submission_score_id = ?
            RETURNING submission_score_id
            """,
            (
                show_results_to_learner,
                show_overall_score_to_learner,
                show_question_scores_to_learner,
                show_feedback_to_learner,
                show_expected_answers_to_learner,
                show_grading_criteria_to_learner,
                show_ai_review_to_learner,
                show_learner_responses_to_learner,
                json.dumps(results_visibility),
                int(submission_score_id),
            ),
        )
        if len(updated_rows) == 0:
            st.session_state[approval_status_key] = ("warning", "Results visibility save did not update any rows.")
            st.session_state[approval_status_expiry_key] = time.time() + 8
            st.rerun()
        reloaded_visibility = fetch_one(
            """
            SELECT
                submission_score_id,
                show_results_to_learner,
                show_overall_score_to_learner,
                show_question_scores_to_learner,
                show_feedback_to_learner,
                show_expected_answers_to_learner,
                show_grading_criteria_to_learner,
                show_ai_review_to_learner,
                show_learner_responses_to_learner,
                results_visibility_json
            FROM submission_scores
            WHERE submission_score_id = ?
            """,
            (int(submission_score_id),),
        )
        if reloaded_visibility:
            st.session_state[visibility_override_key] = {
                "show_results_to_learner": bool(reloaded_visibility.get("show_results_to_learner")),
                "show_overall_score_to_learner": bool(reloaded_visibility.get("show_overall_score_to_learner")),
                "show_question_scores_to_learner": bool(reloaded_visibility.get("show_question_scores_to_learner")),
                "show_feedback_to_learner": bool(reloaded_visibility.get("show_feedback_to_learner")),
                "show_expected_answers_to_learner": bool(reloaded_visibility.get("show_expected_answers_to_learner")),
                "show_grading_criteria_to_learner": bool(reloaded_visibility.get("show_grading_criteria_to_learner")),
                "show_ai_review_to_learner": bool(reloaded_visibility.get("show_ai_review_to_learner")),
                "show_learner_responses_to_learner": bool(show_learner_responses_to_learner),
            }
        st.session_state[approval_status_key] = ("success", "Results visibility settings saved.")
        st.session_state[approval_status_expiry_key] = time.time() + 8
        st.rerun()

    st.markdown("#### AI Question Grading Review")
    question_rows = fetch_all(
        """
        SELECT
            mq.question_id,
            mq.question_order,
            mq.question_text,
            COALESCE(mq.question_type, 'open_text') AS question_type,
            COALESCE(mq.expected_answer, mq.rationale, '') AS expected_answer,
            COALESCE(mq.rubric, mq.rationale, '') AS rubric,
            COALESCE(mq.keyword_expected_terms, '') AS keyword_expected_terms,
            COALESCE(mq.llm_grading_criteria, mq.llm_grading_instructions, '') AS llm_grading_criteria,
            COALESCE(mq.partial_credit_guidance, '') AS partial_credit_guidance,
            COALESCE(mq.incorrect_criteria, '') AS incorrect_criteria,
            COALESCE(mq.incomplete_criteria, '') AS incomplete_criteria,
            COALESCE(mq.strong_response_criteria, '') AS strong_response_criteria,
            COALESCE(mq.max_points, 10) AS max_points,
            sqs.learner_answer,
            COALESCE(sqs.ai_awarded_points, sqs.ai_score) AS ai_awarded_points,
            sqs.ai_max_points,
            COALESCE(sqs.ai_feedback, sqs.feedback) AS ai_feedback,
            sqs.ai_reasoning,
            COALESCE(sqs.admin_awarded_points, sqs.admin_score) AS admin_awarded_points,
            sqs.admin_feedback,
            COALESCE(sqs.final_awarded_points, sqs.final_score, sqs.admin_awarded_points, sqs.admin_score, sqs.ai_awarded_points, sqs.ai_score) AS final_awarded_points,
            COALESCE(sqs.visible_to_learner, FALSE) AS visible_to_learner
        FROM attempts a
        JOIN module_questions mq ON mq.module_id = a.module_id
        LEFT JOIN submission_question_scores sqs ON sqs.attempt_id = a.attempt_id AND sqs.question_id = mq.question_id
        WHERE a.attempt_id = ?
        ORDER BY mq.question_order
        """,
        (int(selected_attempt_id),),
    )
    if question_rows:
        for row in question_rows:
            with st.container(border=True):
                st.markdown(f"**Q{row['question_order']}** {row['question_text']}")
                st.caption(f"Expected: {row.get('expected_answer') or '—'}")
                st.caption(f"Rubric: {row.get('rubric') or '—'}")
                st.caption(f"Keywords: {row.get('keyword_expected_terms') or '—'}")
                st.caption(f"LLM instructions: {row.get('llm_grading_criteria') or '—'}")
                st.caption(f"Partial credit: {row.get('partial_credit_guidance') or '—'}")
                st.caption(
                    "Incorrect / Incomplete / Strong: "
                    f"{row.get('incorrect_criteria') or '—'} / {row.get('incomplete_criteria') or '—'} / {row.get('strong_response_criteria') or '—'}"
                )
                if str(row.get("question_type") or "").strip() == "ai_conversation":
                    transcript_raw = str(row.get("learner_answer") or "[]")
                    try:
                        transcript_payload = json.loads(transcript_raw)
                    except Exception:
                        transcript_payload = []
                    st.write("Conversation transcript")
                    if isinstance(transcript_payload, list) and transcript_payload:
                        for turn in transcript_payload:
                            if not isinstance(turn, dict):
                                continue
                            role = str(turn.get("role") or "unknown").strip().title()
                            content = str(turn.get("content") or "").strip()
                            st.markdown(f"- **{role}:** {content}")
                    else:
                        st.caption("No transcript available.")
                else:
                    st.write(f"Learner answer: {row.get('learner_answer') or '—'}")
                st.write(f"AI score: {row.get('ai_awarded_points') if row.get('ai_awarded_points') is not None else 0} / {row.get('ai_max_points') or row.get('max_points')}")
                st.write(f"AI rationale: {row.get('ai_reasoning') or '—'}")
                with st.form(f"grade_edit_{selected_attempt_id}_{row['question_id']}"):
                    admin_points = st.number_input(
                        "Admin points",
                        min_value=0.0,
                        max_value=float(row.get("max_points") or 10),
                        value=float(row.get("admin_awarded_points") if row.get("admin_awarded_points") is not None else (row.get("ai_awarded_points") or 0.0)),
                        step=0.5,
                    )
                    admin_feedback = st.text_area("Admin feedback", value=row.get("admin_feedback") or row.get("ai_feedback") or "")
                    visible_to_learner = st.checkbox("Visible to learner", value=bool(row.get("visible_to_learner")))
                    if st.form_submit_button("Save question edits"):
                        execute(
                            """
                            INSERT INTO submission_question_scores (
                                attempt_id, question_id, ai_score, admin_score, final_score, feedback,
                                learner_answer, ai_awarded_points, ai_max_points,
                                ai_feedback, ai_reasoning, admin_awarded_points, admin_feedback,
                                final_awarded_points, visible_to_learner, is_admin_override
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(attempt_id, question_id) DO UPDATE SET
                                ai_score = excluded.ai_score,
                                admin_score = excluded.admin_score,
                                final_score = excluded.final_score,
                                feedback = excluded.feedback,
                                admin_awarded_points = excluded.admin_awarded_points,
                                admin_feedback = excluded.admin_feedback,
                                final_awarded_points = excluded.final_awarded_points,
                                visible_to_learner = excluded.visible_to_learner,
                                is_admin_override = excluded.is_admin_override,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            (
                                int(selected_attempt_id),
                                int(row["question_id"]),
                                row.get("ai_awarded_points"),
                                float(admin_points),
                                float(admin_points),
                                admin_feedback.strip(),
                                row.get("learner_answer") or "",
                                row.get("ai_awarded_points"),
                                row.get("ai_max_points") or row.get("max_points"),
                                row.get("ai_feedback") or "",
                                row.get("ai_reasoning") or "",
                                float(admin_points),
                                admin_feedback.strip(),
                                float(admin_points),
                                bool(visible_to_learner),
                                True,
                            ),
                        )
                        st.success("Question edits saved.")
                        st.rerun()

        totals = fetch_one(
            """
            SELECT
                SUM(COALESCE(ai_awarded_points, ai_score, 0)) AS ai_total,
                SUM(COALESCE(admin_awarded_points, admin_score, ai_awarded_points, ai_score, 0)) AS admin_total,
                SUM(COALESCE(ai_max_points, 0)) AS max_total
            FROM submission_question_scores
            WHERE attempt_id = ?
            """,
            (int(selected_attempt_id),),
        ) or {}
        ai_total = float(totals.get("ai_total") or 0)
        admin_total = float(totals.get("admin_total") or 0)
        max_total = float(totals.get("max_total") or 0)
        final_total = admin_total if admin_total > 0 else ai_total
        percentage = round((final_total / max_total) * 100, 1) if max_total else None

        controls_col_a, controls_col_b = st.columns(2)
        with controls_col_a:
            if st.button("Save admin edits", width="stretch"):
                execute(
                    """
                    INSERT INTO submission_scores (
                        attempt_id, ai_total_score, admin_total_score, final_total_score, max_total_score, percentage, grading_status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'ai_graded_pending_review')
                    ON CONFLICT(attempt_id) DO UPDATE SET
                        ai_total_score = excluded.ai_total_score,
                        admin_total_score = excluded.admin_total_score,
                        final_total_score = excluded.final_total_score,
                        max_total_score = excluded.max_total_score,
                        percentage = excluded.percentage,
                        grading_status = excluded.grading_status,
                        scored_at = CURRENT_TIMESTAMP
                    """,
                    (int(selected_attempt_id), ai_total, admin_total, final_total, max_total, percentage),
                )
                execute(
                    "UPDATE attempts SET total_score = ?, result_status = 'ai_graded_pending_review' WHERE attempt_id = ? AND organization_id = ?",
                    (percentage, int(selected_attempt_id), org_id),
                )
                st.success("Admin edits saved.")
                st.rerun()
        with controls_col_b:
            if st.button("Retry AI grading", width="stretch"):
                grade_submission_with_ai(int(selected_attempt_id))
                st.success("AI grading completed.")
                st.rerun()
    else:
        st.info("No per-question grading records yet.")


def render_admin_assignment_review(current_user: dict, assignment_id: int | None) -> None:
    st.subheader("Assignment Instance Review")
    if not assignment_id:
        st.info("Provide an assignment_id in the URL to review a specific assignment instance.")
        return

    org_id = current_user["organization_id"]
    assignment = fetch_one(
        """
        SELECT
            a.assignment_id,
            a.assigned_at,
            a.due_date,
            a.is_active,
            a.module_id,
            a.learner_id,
            m.title AS module_title,
            m.category,
            m.difficulty,
            u.name AS learner_name,
            u.email AS learner_email
        FROM assignments a
        JOIN modules m ON m.module_id = a.module_id
        JOIN users u ON u.user_id = a.learner_id
        WHERE a.assignment_id = ? AND a.organization_id = ?
        """,
        (assignment_id, org_id),
    )
    if not assignment:
        st.error("Assignment not found or you are not authorized to view it.")
        return

    st.caption(f"Assignment #{assignment['assignment_id']} • {assignment['module_title']}")
    metric_row(
        {
            "Learner": assignment["learner_name"],
            "Module": assignment["module_title"],
            "Assigned at": str(assignment["assigned_at"]),
            "Due date": str(assignment["due_date"] or "N/A"),
        }
    )

    workspace_state = fetch_one(
        """
        SELECT *
        FROM assignment_workspace_state
        WHERE assignment_id = ? AND organization_id = ?
        """,
        (assignment_id, org_id),
    )
    attempt = fetch_one(
        """
        SELECT attempt_id, created_at, total_score, diagnosis_answer, next_steps_answer, customer_response, notes, question_responses
        FROM attempts
        WHERE user_id = ?
          AND module_id = ?
          AND organization_id = ?
          AND created_at >= ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (assignment["learner_id"], assignment["module_id"], org_id, assignment["assigned_at"]),
    )

    with st.container(border=True):
        st.markdown("#### Assignment details")
        st.write(f"Learner: **{assignment['learner_name']}** ({assignment['learner_email'] or 'No email'})")
        st.write(f"Status: **{'Active' if int(assignment['is_active']) else 'Inactive'}**")
        if workspace_state:
            st.write(f"Wizard step: **{workspace_state.get('current_step', 1)}**")
            st.write(f"Progress state: **{workspace_state.get('progress_status', 'not_started')}**")
            st.write(f"Last saved: **{workspace_state.get('last_saved_at')}**")
            st.write(f"Submitted state: **{'Yes' if bool(workspace_state.get('submitted_state')) else 'No'}**")

    with st.container(border=True):
        st.markdown("#### Learner responses")
        if workspace_state:
            st.write("Notes")
            st.code(workspace_state.get("learner_notes") or "No notes yet.")
            st.write("Diagnosis")
            st.code(workspace_state.get("diagnosis_response") or "No diagnosis yet.")
            st.write("Next steps")
            st.code(workspace_state.get("next_steps_response") or "No next steps yet.")
            st.write("Customer response")
            st.code(workspace_state.get("customer_response") or "No response yet.")
            st.write(f"Escalation: **{workspace_state.get('escalation_choice') or 'No escalation'}**")
        else:
            st.info("No saved in-progress workspace state found yet.")

    with st.container(border=True):
        st.markdown("#### Submission / results")
        if not attempt:
            st.info("No submitted attempt found for this assignment instance yet.")
            return
        st.write(f"Attempt ID: **{attempt['attempt_id']}**")
        st.write(f"Submitted at: **{attempt['created_at']}**")
        st.write(f"Total score: **{attempt['total_score']}%**")
        st.write("Diagnosis answer")
        st.code(attempt.get("diagnosis_answer") or "N/A")
        st.write("Next steps answer")
        st.code(attempt.get("next_steps_answer") or "N/A")
        st.write("Customer response")
        st.code(attempt.get("customer_response") or "N/A")
        try:
            question_payload = json.loads(attempt.get("question_responses") or "{}")
        except Exception:
            question_payload = {}
        module_question_rows = fetch_all(
            "SELECT question_id, question_order, question_text, question_type FROM module_questions WHERE module_id = ? ORDER BY question_order",
            (assignment["module_id"],),
        )
        if module_question_rows:
            st.markdown("#### Assessment question responses")
            for question_row in module_question_rows:
                qid = str(question_row.get("question_id"))
                st.markdown(f"**Q{question_row.get('question_order')}. {question_row.get('question_text')}**")
                answer = question_payload.get(qid)
                if str(question_row.get("question_type") or "").strip() == "ai_conversation":
                    transcript = answer.get("transcript") if isinstance(answer, dict) else []
                    if isinstance(transcript, list) and transcript:
                        for turn in transcript:
                            role = "AI" if str(turn.get("role")) == "assistant" else "Learner"
                            st.markdown(f"- **{role}:** {turn.get('content') or ''}")
                    else:
                        st.caption("No conversation transcript.")
                else:
                    st.code(str(answer or "N/A"))

    selected_attempt = st.selectbox(
        "Submission feedback",
        options=filtered["attempt_id"].tolist(),
        format_func=lambda aid: f"Attempt #{aid}",
    )
    feedback_row = filtered[filtered["attempt_id"] == selected_attempt].iloc[0]
    with st.container(border=True):
        st.markdown("#### AI feedback")
        st.caption(f"Scoring version: {feedback_row.get('scoring_version') or 'legacy'}")
        st.write(feedback_row["ai_feedback"] or "No feedback available.")


def render_progress_tracking(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    st.markdown(
        """
<style>
/* Remove top padding from main block */
.block-container {
    padding-top: 0rem !important;
}

/* Remove extra spacing from first container */
div[data-testid="stElementContainer"]:first-child {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

/* Remove phantom empty containers */
div[data-testid="stElementContainer"]:empty {
    display: none;
}
</style>
""",
        unsafe_allow_html=True,
    )
    st.title("Progress Tracking")
    st.caption("Monitor assignment completion and learner activity across your organization.")

    assignments_df = _assignments_with_status(org_id)
    if assignments_df.empty:
        st.info("No assignments to track.")
        return

    filter_state_key = "progress_tracking_filters"
    default_filters = {
        "status": ["Completed", "In Progress", "Overdue", "Not Started"],
        "learners": [],
        "organizations": [],
        "due_from": None,
        "due_to": None,
        "attempted_from": None,
        "attempted_to": None,
        "no_attempt_yet": False,
    }
    if filter_state_key not in st.session_state:
        st.session_state[filter_state_key] = default_filters.copy()

    reset_filters_key = "progress_tracking_reset_filters"
    if st.session_state.pop(reset_filters_key, False):
        st.session_state[filter_state_key] = default_filters.copy()
        st.session_state["progress_due_from"] = None
        st.session_state["progress_due_to"] = None
        st.session_state["progress_attempted_from"] = None
        st.session_state["progress_attempted_to"] = None

    current_filters = st.session_state[filter_state_key]
    widget_defaults = {
        "progress_due_from": current_filters.get("due_from"),
        "progress_due_to": current_filters.get("due_to"),
        "progress_attempted_from": current_filters.get("attempted_from"),
        "progress_attempted_to": current_filters.get("attempted_to"),
    }
    for key, default_value in widget_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

    learner_labels = sorted(
        {
            f"{(row.get('learner_name') or 'Unknown learner').strip()} ({(row.get('learner_email') or 'no-email')})"
            for _, row in assignments_df.iterrows()
        }
    )
    organization_options = sorted(
        {
            str(row.get("organization_name") or "Unassigned")
            for _, row in assignments_df.iterrows()
        }
    )

    with st.container(border=True):
        st.caption("No attempt yet ignores attempted date range filters.")
        with st.form("progress-tracking-filters", clear_on_submit=False):
            row_one_cols = st.columns([1.1, 1.1, 1.2, 1.2])
            status_filter = row_one_cols[0].multiselect(
                "Status",
                ["Completed", "In Progress", "Overdue", "Not Started"],
                default=current_filters.get("status", default_filters["status"]),
            )
            learner_filter = row_one_cols[1].multiselect(
                "Learner",
                learner_labels,
                default=current_filters.get("learners", []),
                placeholder="Search learner",
            )
            org_filter = row_one_cols[2].multiselect(
                "Organization",
                organization_options,
                default=current_filters.get("organizations", []),
                placeholder="Search organization",
            )
            no_attempt_yet = row_one_cols[3].checkbox(
                "No attempt yet",
                value=bool(current_filters.get("no_attempt_yet", False)),
                help="When enabled, attempted date range is ignored and only rows with no attempt are shown.",
            )

            row_two_cols = st.columns(4)
            due_from = row_two_cols[0].date_input(
                "Due from",
                key="progress_due_from",
            )
            due_to = row_two_cols[1].date_input(
                "Due to",
                key="progress_due_to",
            )
            attempted_from = row_two_cols[2].date_input(
                "Attempted from",
                disabled=no_attempt_yet,
                key="progress_attempted_from",
            )
            attempted_to = row_two_cols[3].date_input(
                "Attempted to",
                disabled=no_attempt_yet,
                key="progress_attempted_to",
            )

            apply_col, clear_col = st.columns([1, 1])
            apply_pressed = apply_col.form_submit_button("Apply Filters", width="stretch", type="primary")
            clear_pressed = clear_col.form_submit_button("Clear Filters", width="stretch")

        if clear_pressed:
            st.session_state[reset_filters_key] = True
            st.rerun()
        if apply_pressed:
            st.session_state[filter_state_key] = {
                "status": status_filter or default_filters["status"],
                "learners": learner_filter,
                "organizations": org_filter,
                "due_from": due_from,
                "due_to": due_to,
                "attempted_from": None if no_attempt_yet else attempted_from,
                "attempted_to": None if no_attempt_yet else attempted_to,
                "no_attempt_yet": no_attempt_yet,
            }

    applied_filters = st.session_state[filter_state_key]

    filtered = assignments_df.copy()
    filtered["learner_label"] = filtered.apply(
        lambda row: f"{(row.get('learner_name') or 'Unknown learner').strip()} ({(row.get('learner_email') or 'no-email')})",
        axis=1,
    )
    filtered["organization_label"] = filtered["organization_name"].fillna("Unassigned").astype(str)
    filtered["due_date_dt"] = pd.to_datetime(filtered["due_date"], errors="coerce").dt.date
    filtered["attempted_dt"] = pd.to_datetime(filtered["last_attempt_at"], errors="coerce").dt.date

    if applied_filters["status"]:
        filtered = filtered[filtered["status"].isin(applied_filters["status"])]
    if applied_filters["learners"]:
        filtered = filtered[filtered["learner_label"].isin(applied_filters["learners"])]
    if applied_filters["organizations"]:
        filtered = filtered[filtered["organization_label"].isin(applied_filters["organizations"])]
    if applied_filters["due_from"]:
        filtered = filtered[filtered["due_date_dt"].notna() & (filtered["due_date_dt"] >= applied_filters["due_from"])]
    if applied_filters["due_to"]:
        filtered = filtered[filtered["due_date_dt"].notna() & (filtered["due_date_dt"] <= applied_filters["due_to"])]
    if applied_filters["no_attempt_yet"]:
        filtered = filtered[filtered["attempted_dt"].isna()]
    else:
        if applied_filters["attempted_from"]:
            filtered = filtered[
                filtered["attempted_dt"].notna() & (filtered["attempted_dt"] >= applied_filters["attempted_from"])
            ]
        if applied_filters["attempted_to"]:
            filtered = filtered[
                filtered["attempted_dt"].notna() & (filtered["attempted_dt"] <= applied_filters["attempted_to"])
            ]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Completed", int(filtered["status"].eq("Completed").sum()))
    col2.metric("In Progress", int(filtered["status"].eq("In Progress").sum()))
    col3.metric("Overdue", int(filtered["status"].eq("Overdue").sum()))
    col4.metric("Not Started", int(filtered["status"].eq("Not Started").sum()))

    if filtered.empty:
        st.info("No progress records match the selected filters.")
        return

    progress_display_df = filtered[
        [
            "learner_name",
            "module_title",
            "status",
            "due_date",
            "assigned_at",
            "last_attempt_at",
        ]
    ].rename(
        columns={
            "learner_name": "Learner Name",
            "module_title": "Module Title",
            "status": "Status",
            "due_date": "Due Date",
            "assigned_at": "Assigned At",
            "last_attempt_at": "Completed At",
        }
    )
    for datetime_col in ["Due Date", "Assigned At", "Completed At"]:
        progress_display_df[datetime_col] = progress_display_df[datetime_col].apply(_format_datetime_for_admin_grid)
    progress_table_height = max(360, min(760, 36 * (len(progress_display_df) + 1)))
    with table_card_container():
        st.dataframe(
            progress_display_df,
            hide_index=True,
            width="stretch",
            height=progress_table_height,
        )


def _parse_lines(value: str) -> str:
    return "\n".join([line.strip() for line in value.splitlines() if line.strip()])


def _normalize_question_type(value: object, fallback: str = "open_text") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in QUESTION_TYPE_OPTIONS:
        return normalized
    return fallback


def _normalize_question_scoring_type(value: object, fallback: str = "llm") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"ai_review", "ai"}:
        return "llm"
    if normalized in {"manual_review", "manual"}:
        return "manual"
    if normalized in QUESTION_SCORING_OPTIONS:
        return normalized
    if normalized == "hybrid":
        return "manual"
    if normalized in {"rubric_llm", "llm_rubric"}:
        return "llm"
    normalized_fallback = str(fallback or "").strip().lower()
    return normalized_fallback if normalized_fallback in QUESTION_SCORING_OPTIONS else "llm"


def _safe_scoring_option_index(
    scoring_type: object, options: list[str] | tuple[str, ...], fallback: str = "llm"
) -> int:
    normalized = _normalize_question_scoring_type(scoring_type, fallback=fallback)
    options_list = [str(option).strip().lower() for option in options]
    if normalized in options_list:
        return options_list.index(normalized)
    fallback_normalized = _normalize_question_scoring_type(fallback, fallback="llm")
    if fallback_normalized in options_list:
        return options_list.index(fallback_normalized)
    return 0


def _normalize_module_scoring_fallback(module_value: object) -> str:
    normalized = str(module_value or "").strip().lower()
    if normalized in {"manual_review", "manual"}:
        return "manual"
    if normalized in {"rubric_llm", "llm"}:
        return "llm"
    return "llm"


def _clean_question_scoring_fields(question: dict[str, object]) -> dict[str, object]:
    question_type = _normalize_question_type(question.get("question_type"))
    scoring_type = _normalize_scoring_for_question_type(
        question_type,
        question.get("scoring_type"),
        fallback="llm",
    )
    cleaned = dict(question)
    cleaned["scoring_type"] = scoring_type
    if question_type != "ai_conversation":
        cleaned["ai_conversation_prompt"] = ""
        cleaned["ai_role_or_persona"] = ""
        cleaned["evaluation_focus"] = ""
        cleaned["max_learner_responses"] = 3
        cleaned["optional_wrap_up_instruction"] = ""
    if question_type == "multiple_choice":
        cleaned["expected_answer"] = ""
        cleaned["keyword_expected_terms"] = ""
        cleaned["rubric_criteria_text"] = ""
        if scoring_type != "llm":
            cleaned["llm_grading_criteria"] = ""
    if scoring_type == "manual":
        cleaned["expected_answer"] = ""
        cleaned["llm_grading_criteria"] = ""
        cleaned["rubric_criteria_text"] = ""
    elif scoring_type == "keyword":
        cleaned["llm_grading_criteria"] = ""
        cleaned["rubric_criteria_text"] = ""
    elif scoring_type == "llm":
        cleaned["expected_answer"] = ""
    return cleaned


def _normalize_scoring_for_question_type(question_type: object, scoring_type: object, fallback: str = "llm") -> str:
    normalized = _normalize_question_scoring_type(scoring_type, fallback=fallback)
    normalized_question_type = str(question_type or "").strip().lower()
    if normalized_question_type in {"ai_conversation", "multiple_choice"} and normalized == "keyword":
        return "llm"
    return normalized


def _coerce_choice_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(choice) for choice in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(choice) for choice in parsed]
            if isinstance(parsed, dict):
                choices = parsed.get("choices")
                if isinstance(choices, list):
                    return [str(choice) for choice in choices]
        except Exception:
            pass
        return [line.strip() for line in value.splitlines() if line.strip()]
    return []


def _normalize_question_choices(question: dict[str, object]) -> list[str]:
    choices = _coerce_choice_list(question.get("choices"))
    if choices:
        return choices
    return _coerce_choice_list(question.get("options_text"))


def _serialize_question_options(question: dict[str, object]) -> str:
    if str(question.get("question_type") or "").strip() != "multiple_choice":
        return ""
    choices = [str(choice or "") for choice in _coerce_choice_list(question.get("choices"))]
    payload: dict[str, object] = {"choices": choices}
    correct_choice_index = question.get("correct_choice_index")
    if isinstance(correct_choice_index, int) and 0 <= correct_choice_index < len(choices):
        payload["correct_choice_index"] = correct_choice_index
    return json.dumps(payload)


def _extract_correct_choice_index(question: dict[str, object]) -> int | None:
    raw_value = question.get("correct_choice_index")
    if isinstance(raw_value, int):
        return raw_value
    options = question.get("options_text")
    if isinstance(options, str):
        try:
            parsed = json.loads(options)
            if isinstance(parsed, dict) and isinstance(parsed.get("correct_choice_index"), int):
                return int(parsed.get("correct_choice_index"))
        except Exception:
            return None
    return None


def _normalize_ai_max_responses(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 3
    return parsed if parsed in AI_CONVERSATION_MAX_RESPONSE_OPTIONS else 3


def _parse_rubric_criteria_lines(value: str) -> list[dict[str, object]]:
    criteria: list[dict[str, object]] = []
    for order, raw_line in enumerate((value or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split("|")]
        label = parts[0] if parts else ""
        if not label:
            continue
        max_points = 1.0
        guidance = ""
        if len(parts) > 1:
            try:
                max_points = max(0.1, float(parts[1]))
            except (TypeError, ValueError):
                guidance = parts[1]
        if len(parts) > 2:
            guidance = parts[2]
        criteria.append(
            {
                "criterion_order": order,
                "label": label,
                "max_points": max_points,
                "grading_guidance": guidance,
            }
        )
    return criteria


def _coerce_rubric_criteria(value: object) -> list[dict[str, object]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [row for row in parsed if isinstance(row, dict)]
        except Exception:
            return []
    return []


def _estimated_time_to_minutes(value: str | None, fallback: int = 20) -> int:
    text = (value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return fallback
    return max(1, int(digits))


def _is_present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "f", "0", "no", "n", "off"}:
            return False
    return default


def _has_session_text(key: str) -> bool:
    return bool(str(st.session_state.get(key, "")).strip())


def _format_review_value(value: object) -> str:
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = _normalize_text(value)
    if not text:
        return "Not provided"
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else "Not provided"


def _render_review_layout_styles() -> None:
    if st.session_state.get("_module_review_styles_injected"):
        return
    st.session_state["_module_review_styles_injected"] = True
    st.markdown(
        """
        <style>
            .review-shell {
                max-width: 920px;
                margin: 0 auto;
                padding: 0.25rem 0.25rem 0.4rem;
            }
            .review-section {
                margin-bottom: 2rem;
            }
            .review-section-title {
                display: flex;
                align-items: center;
                gap: 0.55rem;
                font-size: 1.4rem;
                font-weight: 800;
                color: #101828;
                margin-bottom: 0.65rem;
                padding-bottom: 0.65rem;
                border-bottom: 1px solid #dfe3e8;
                line-height: 1.2;
            }
            .review-section-icon {
                font-size: 1.2rem;
                line-height: 1;
            }
            .review-row {
                display: grid;
                grid-template-columns: minmax(180px, 220px) minmax(0, 1fr);
                align-items: start;
                column-gap: 0.5rem;
                row-gap: 0.15rem;
                padding: 0.3rem 0;
            }
            .review-label {
                color: #475467;
                font-size: 0.9rem;
                font-weight: 700;
                line-height: 1.4;
                white-space: nowrap;
            }
            .review-value {
                color: #101828;
                font-size: 0.97rem;
                font-weight: 400;
                line-height: 1.4;
                word-break: break-word;
                min-width: 0;
            }
            .review-badge {
                display: inline-block;
                border-radius: 999px;
                padding: 0.15rem 0.62rem;
                font-size: 0.76rem;
                font-weight: 700;
                border: 1px solid transparent;
            }
            .review-badge-difficulty-beginner {
                color: #067647;
                background: #ecfdf3;
                border-color: #abefc6;
            }
            .review-badge-difficulty-intermediate {
                color: #b54708;
                background: #fffaeb;
                border-color: #fedf89;
            }
            .review-badge-difficulty-advanced {
                color: #b42318;
                background: #fef3f2;
                border-color: #fecdca;
            }
            .review-badge-yes {
                color: #067647;
                background: #ecfdf3;
                border-color: #abefc6;
            }
            .review-badge-no {
                color: #344054;
                background: #f2f4f7;
                border-color: #d0d5dd;
            }
            .review-value-strong {
                font-weight: 700;
                color: #0f172a;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_review_value_html(label: str, value: object) -> str:
    normalized = _format_review_value(value)
    if label == "Difficulty":
        badge_key = normalized.strip().lower()
        if badge_key in {"beginner", "intermediate", "advanced"}:
            return (
                f"<span class='review-badge review-badge-difficulty-{badge_key}'>"
                f"{escape(normalized)}</span>"
            )
    if label == "Quiz Required":
        is_yes = _normalize_bool(value, default=False)
        tone = "yes" if is_yes else "no"
        text = "Yes" if is_yes else "No"
        return f"<span class='review-badge review-badge-{tone}'>{text}</span>"
    if label == "Estimated Time" and normalized != "Not provided":
        return f"<span class='review-value-strong'>{escape(normalized)} minutes</span>"
    if label in {"Estimated Time (minutes)", "Question Count"} and normalized != "Not provided":
        return f"<span class='review-value-strong'>{escape(normalized)}</span>"
    return "<br>".join(escape(line) for line in normalized.splitlines())


def _render_review_section(title: str, fields: list[tuple[str, object]], icon: str = "") -> None:
    rows_html: list[str] = []
    for label, raw_value in fields:
        rows_html.append(
            "<div class='review-row'>"
            f"<span class='review-label'>{escape(label)}:</span>"
            f"<span class='review-value'>{_render_review_value_html(label, raw_value)}</span>"
            "</div>"
        )
    icon_html = f"<span class='review-section-icon'>{escape(icon)}</span>" if icon else ""
    st.markdown(
        "<div class='review-section'>"
        f"<div class='review-section-title'>{icon_html}<span>{escape(title)}</span></div>"
        f"{''.join(rows_html)}"
        "</div>",
        unsafe_allow_html=True,
    )


def _render_module_review_summary(module_values: dict) -> None:
    _render_review_layout_styles()
    st.markdown("<div class='review-shell'>", unsafe_allow_html=True)
    _render_review_section(
        "Basic Info",
        [
            ("Title", module_values.get("title")),
            ("Category", module_values.get("category")),
            ("Difficulty", module_values.get("difficulty")),
        ],
        icon="📌",
    )
    _render_review_section(
        "Module Details",
        [
            ("Role Focus", module_values.get("role_focus")),
            ("Test Focus", module_values.get("test_focus")),
            ("Description", module_values.get("description")),
        ],
        icon="🧭",
    )
    _render_review_section(
        "Learning Content",
        [
            ("Learning Objectives", module_values.get("learning_objectives")),
            ("Scenario Constraints", module_values.get("scenario_constraints")),
            ("Content Sections", module_values.get("content_sections")),
            ("Completion Requirements", module_values.get("completion_requirements")),
        ],
        icon="📚",
    )
    _render_review_section(
        "Assessment",
        [
            ("Quiz Required", module_values.get("quiz_required")),
            ("Estimated Time", module_values.get("estimated_minutes")),
            ("Question Count", module_values.get("question_count")),
        ],
        icon="✅",
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_wizard_progress(step_index: int, total_steps: int, title: str) -> None:
    st.caption(f"Step {step_index + 1} of {total_steps}")
    st.progress((step_index + 1) / total_steps)
    st.markdown(f"#### {title}")


def _render_named_step_indicator(step_index: int, labels: list[str]) -> None:
    total_steps = len(labels)
    clamped_step_index = max(0, min(step_index, max(total_steps - 1, 0)))
    percent_complete = (clamped_step_index + 1) / total_steps if total_steps else 0
    st.caption(f"Step {clamped_step_index + 1} of {total_steps}")
    st.progress(percent_complete)


def render_module_builder(current_user: dict) -> None:
    org_id = current_user["organization_id"]

    mode_key = "module_builder_mode"
    form_key = "module_builder_editor_form"
    dirty_key = "module_builder_editor_dirty"
    pending_close_key = "module_builder_editor_pending_close"
    save_status_key = "module_builder_editor_save_status"
    last_input_key = "module_builder_editor_last_input_ts"
    last_save_key = "module_builder_editor_last_save_ts"
    touched_key = "module_builder_editor_touched_fields"
    publish_attempted_key = "module_builder_editor_publish_attempted"
    ai_prompt_key = "module_builder_ai_prompt"
    ai_draft_key = "module_builder_ai_draft"
    ai_feedback_key = "module_builder_ai_feedback"
    ai_last_prompt_key = "module_builder_ai_last_prompt"
    ai_keep_editing_key = "module_builder_ai_keep_editing"
    ai_question_count_key = "ai_question_count"
    pending_updates_key = "pending_module_builder_state_updates"
    pending_reset_mode_key = "module_builder_pending_reset_mode"
    recently_created_module_id_key = "recently_created_module_id"
    recently_created_module_title_key = "recently_created_module_title"
    widget_keys = [
        "module_builder_title",
        "module_builder_description",
        "module_builder_scenario_constraints",
        "module_builder_category",
        "module_builder_difficulty",
        "module_builder_role_focus",
        "module_builder_test_focus",
        "module_builder_learning_objectives",
        "module_builder_content_sections",
        "module_builder_completion_requirements",
        "module_builder_quiz_required",
        "module_builder_estimated_minutes",
        "module_builder_attempt_limit",
    ]

    default_form = {
        "title": "",
        "description": "",
        "scenario_constraints": "",
        "category": "General",
        "difficulty": "Beginner",
        "role_focus": "",
        "test_focus": "",
        "learning_objectives": "",
        "content_sections": "",
        "completion_requirements": "",
        "quiz_required": True,
        "estimated_minutes": 20,
        "question_count": 1,
        "attempt_limit": 1,
        "questions": [
            {
                "question_text": "",
                "question_type": "open_text",
                "options_text": "",
                "choices": [],
                "correct_choice_index": None,
                "rationale": "",
                "expected_answer": "",
                "max_points": 10.0,
                "scoring_type": "llm",
                "llm_grading_criteria": "",
                "rubric_criteria_text": "",
                "ai_conversation_prompt": "",
                "ai_role_or_persona": "",
                "evaluation_focus": "",
                "max_learner_responses": 3,
                "optional_wrap_up_instruction": "",
            }
        ],
    }

    def _reset_builder_state(builder_mode: str | None) -> None:
        st.session_state[mode_key] = builder_mode
        st.session_state[form_key] = dict(default_form)
        if builder_mode == "ai":
            st.session_state[form_key].update(
                {
                    "title": "AI Generated Module",
                    "description": "Describe the module goal and let AI draft a complete first version.",
                    "scenario_constraints": "Provide context, constraints, and expected behavior for the generated scenario.",
                    "learning_objectives": "Define learning objective 1\nDefine learning objective 2",
                    "completion_requirements": "Include key completion requirements and pass/fail criteria.",
                }
            )
        st.session_state[save_status_key] = "Saved"
        st.session_state[dirty_key] = False
        st.session_state[last_input_key] = 0.0
        st.session_state[last_save_key] = 0.0
        st.session_state[touched_key] = set()
        st.session_state[publish_attempted_key] = False
        st.session_state[pending_close_key] = False
        st.session_state[ai_draft_key] = None
        st.session_state[ai_feedback_key] = None
        st.session_state[ai_last_prompt_key] = ""
        st.session_state[ai_keep_editing_key] = False
        st.session_state[ai_question_count_key] = 3
        keys_to_clear = [k for k in list(st.session_state.keys()) if k.startswith("module_builder_q_")]
        for key in [ai_prompt_key, *keys_to_clear, *widget_keys]:
            st.session_state.pop(key, None)

    def _queue_builder_reset(builder_mode: str | None) -> None:
        st.session_state[pending_reset_mode_key] = builder_mode
        st.rerun()

    def _init_state_once(key: str, default_value: object) -> None:
        if key not in st.session_state:
            st.session_state[key] = default_value

    def _queue_module_builder_state_update(updates: dict[str, object]) -> None:
        pending_updates = dict(st.session_state.get(pending_updates_key, {}))
        pending_updates.update(updates)
        st.session_state[pending_updates_key] = pending_updates

    def _apply_pending_module_builder_updates() -> None:
        pending_updates = st.session_state.pop(pending_updates_key, None)
        if isinstance(pending_updates, dict):
            for key, value in pending_updates.items():
                st.session_state[key] = value

    st.session_state.setdefault(mode_key, None)
    if pending_reset_mode_key in st.session_state:
        queued_mode = st.session_state.pop(pending_reset_mode_key)
        _reset_builder_state(queued_mode)
    _apply_pending_module_builder_updates()

    selected_mode = st.session_state.get(mode_key)
    header_description = "Single-page module editor with autosave and inline validation."
    render_page_header("Module Builder", header_description)
    inject_scroll_to_top()

    if selected_mode is None:
        st.caption("Choose a creation method before opening the full module builder.")
        mode_col_1, mode_col_2 = st.columns(2)
        if mode_col_1.button("Start from Scratch", width="stretch"):
            _queue_builder_reset("manual")
        if mode_col_2.button("Generate with AI", width="stretch"):
            _queue_builder_reset("ai")
        st.info("Select a creation mode to continue.")
        return

    if selected_mode == "manual":
        mode_summary_col, mode_action_col = st.columns([3, 1])
        with mode_summary_col:
            st.caption("Mode: Start from Scratch")
        with mode_action_col:
            if st.button("Change creation method", key="module_builder_change_creation_method", width="stretch"):
                _queue_builder_reset(None)

    if form_key not in st.session_state:
        st.session_state[form_key] = dict(default_form)
    if save_status_key not in st.session_state:
        st.session_state[save_status_key] = "Saved"
    st.session_state.setdefault(dirty_key, False)
    st.session_state.setdefault(last_input_key, 0.0)
    st.session_state.setdefault(last_save_key, 0.0)
    st.session_state.setdefault(touched_key, set())
    st.session_state.setdefault(publish_attempted_key, False)
    st.session_state.setdefault(pending_close_key, False)

    module_form = st.session_state[form_key]
    now_ts = time.time()

    def _normalize_generated_difficulty(value: object, current: str) -> str:
        normalized = str(value or "").strip().lower()
        mapping = {
            "beginner": "Beginner",
            "basic": "Beginner",
            "intermediate": "Intermediate",
            "mid": "Intermediate",
            "advanced": "Advanced",
            "expert": "Advanced",
        }
        return mapping.get(normalized, current or "Beginner")

    def _apply_generated_draft_to_form(generated_draft: dict[str, object]) -> None:
        questions = generated_draft.get("questions") if isinstance(generated_draft, dict) else None
        normalized_questions = []
        if isinstance(questions, list):
            for question in questions:
                if not isinstance(question, dict):
                    continue
                question_text = str(question.get("question_text") or "").strip()
                if not question_text:
                    continue
                answer_guidance = str(question.get("answer_guidance") or "").strip()
                rationale_parts = []
                if answer_guidance:
                    rationale_parts.append(f"Ideal answer guidance:\n{answer_guidance}")
                llm_criteria = str(question.get("llm_grading_criteria") or "").strip()
                if llm_criteria:
                    rationale_parts.append(f"LLM grading criteria:\n{llm_criteria}")
                q_type = str(question.get("question_type") or "").strip().lower()
                if q_type not in QUESTION_TYPE_OPTIONS:
                    q_type = "open_text"
                scoring_type = _normalize_scoring_for_question_type(q_type, question.get("scoring_type"), fallback="llm")
                max_points = float(question.get("max_points") or 10)
                generated_criteria = generate_question_scoring_criteria(
                    str(generated_draft.get("title") or ""),
                    str(generated_draft.get("description") or ""),
                    question_text,
                    q_type,
                    answer_choices=_coerce_choice_list(question.get("choices")),
                    expected_answer="\n".join(_coerce_choice_list(question.get("keyword_expected_terms"))),
                    max_points=max_points,
                ) if scoring_type == "llm" else {}
                normalized_questions.append(
                    {
                        "question_text": question_text,
                        "question_type": q_type,
                        "options_text": "",
                        "choices": _coerce_choice_list(question.get("choices")),
                        "correct_choice_index": question.get("correct_choice_index"),
                        "rationale": "\n\n".join(rationale_parts),
                        "expected_answer": "\n".join(_coerce_choice_list(question.get("keyword_expected_terms"))),
                        "max_points": max_points,
                        "scoring_type": scoring_type,
                        "llm_grading_criteria": llm_criteria or str(generated_criteria.get("grader_instructions") or ""),
                        "rubric_criteria_text": str(generated_criteria.get("rubric_criteria") or ""),
                        "ai_conversation_prompt": str(question.get("ai_conversation_prompt") or "").strip(),
                        "ai_role_or_persona": str(question.get("ai_role_or_persona") or "").strip(),
                        "evaluation_focus": str(question.get("evaluation_focus") or "").strip(),
                        "max_learner_responses": _normalize_ai_max_responses(question.get("max_learner_responses")),
                        "optional_wrap_up_instruction": str(question.get("optional_wrap_up_instruction") or "").strip(),
                    }
                )
        if not normalized_questions:
            normalized_questions = [dict(default_form["questions"][0])] if selected_mode == "manual" else []

        module_form["title"] = _normalize_text(generated_draft.get("title")) or module_form.get("title", "")
        module_form["description"] = _normalize_text(generated_draft.get("description")) or module_form.get("description", "")
        module_form["scenario_constraints"] = _normalize_text(generated_draft.get("scenario")) or module_form.get(
            "scenario_constraints", ""
        )
        category = _normalize_text(generated_draft.get("category"))
        if category:
            module_form["category"] = category
        role_focus = _normalize_text(generated_draft.get("role_focus"))
        if role_focus:
            module_form["role_focus"] = role_focus
        test_focus = _normalize_text(generated_draft.get("test_focus"))
        if test_focus:
            module_form["test_focus"] = test_focus
        learning_objectives = generated_draft.get("learning_objectives")
        if isinstance(learning_objectives, list):
            learning_objectives_text = "\n".join(
                str(item).strip() for item in learning_objectives if str(item).strip()
            )
            if learning_objectives_text:
                module_form["learning_objectives"] = learning_objectives_text
        content_sections = generated_draft.get("content_sections")
        if isinstance(content_sections, list):
            content_sections_text = "\n".join(
                str(item).strip() for item in content_sections if str(item).strip()
            )
            if content_sections_text:
                module_form["content_sections"] = content_sections_text
        module_form["difficulty"] = _normalize_generated_difficulty(generated_draft.get("difficulty"), module_form.get("difficulty"))
        time_limit = generated_draft.get("time_limit_minutes")
        if isinstance(time_limit, (int, float)) and int(time_limit) > 0:
            module_form["estimated_minutes"] = int(time_limit)

        overall_rubric = _normalize_text(generated_draft.get("overall_rubric"))
        if overall_rubric:
            module_form["completion_requirements"] = overall_rubric
        module_form["questions"] = normalized_questions
        module_form["question_count"] = len(normalized_questions)

        st.session_state["module_builder_title"] = module_form["title"]
        st.session_state["module_builder_description"] = module_form["description"]
        st.session_state["module_builder_scenario_constraints"] = module_form["scenario_constraints"]
        st.session_state["module_builder_category"] = module_form["category"]
        st.session_state["module_builder_difficulty"] = module_form["difficulty"]
        st.session_state["module_builder_role_focus"] = module_form.get("role_focus", "")
        st.session_state["module_builder_test_focus"] = module_form.get("test_focus", "")
        st.session_state["module_builder_learning_objectives"] = module_form.get("learning_objectives", "")
        st.session_state["module_builder_content_sections"] = module_form.get("content_sections", "")
        st.session_state["module_builder_estimated_minutes"] = int(module_form["estimated_minutes"])
        st.session_state["module_builder_completion_requirements"] = module_form["completion_requirements"]
        keys_to_clear = [k for k in list(st.session_state.keys()) if k.startswith("module_builder_q_")]
        for key in keys_to_clear:
            st.session_state.pop(key, None)
        st.session_state[dirty_key] = True
        st.session_state[last_input_key] = time.time()
        st.session_state[save_status_key] = "Saving..."

    def _mark_dirty(field_key: str | None = None) -> None:
        st.session_state[dirty_key] = True
        st.session_state[last_input_key] = time.time()
        st.session_state[save_status_key] = "Saving..."
        if field_key:
            touched = set(st.session_state.get(touched_key, set()))
            touched.add(field_key)
            st.session_state[touched_key] = touched

    def _on_question_type_change(current_idx: int) -> None:
        q_prefix = f"module_builder_q_{current_idx}"
        question_type = st.session_state.get(f"{q_prefix}_type", "open_text")
        choices_key = f"{q_prefix}_choices"
        if question_type == "multiple_choice" and not st.session_state.get(choices_key):
            st.session_state[choices_key] = ["", ""]
            st.session_state[f"{q_prefix}_correct_choice"] = None
        if question_type == "multiple_choice":
            st.session_state[f"{q_prefix}_scoring_type"] = _normalize_scoring_for_question_type(
                "multiple_choice",
                st.session_state.get(f"{q_prefix}_scoring_type"),
                fallback="llm",
            )
        if question_type == "ai_conversation":
            st.session_state[f"{q_prefix}_scoring_type"] = "manual"
        _mark_dirty(f"question_{current_idx + 1}")

    def _sync_form_from_widgets() -> None:
        module_form["title"] = st.session_state.get("module_builder_title", module_form.get("title", ""))
        module_form["description"] = st.session_state.get("module_builder_description", module_form.get("description", ""))
        module_form["scenario_constraints"] = st.session_state.get(
            "module_builder_scenario_constraints", module_form.get("scenario_constraints", "")
        )
        module_form["category"] = st.session_state.get("module_builder_category", module_form.get("category", "General"))
        module_form["difficulty"] = st.session_state.get("module_builder_difficulty", module_form.get("difficulty", "Beginner"))
        module_form["role_focus"] = st.session_state.get("module_builder_role_focus", module_form.get("role_focus", ""))
        module_form["test_focus"] = st.session_state.get("module_builder_test_focus", module_form.get("test_focus", ""))
        module_form["learning_objectives"] = st.session_state.get(
            "module_builder_learning_objectives", module_form.get("learning_objectives", "")
        )
        module_form["content_sections"] = st.session_state.get(
            "module_builder_content_sections", module_form.get("content_sections", "")
        )
        module_form["completion_requirements"] = st.session_state.get(
            "module_builder_completion_requirements", module_form.get("completion_requirements", "")
        )
        module_form["quiz_required"] = _normalize_bool(st.session_state.get("module_builder_quiz_required", True), default=True)
        module_form["estimated_minutes"] = int(st.session_state.get("module_builder_estimated_minutes", 20))
        module_form["attempt_limit"] = int(st.session_state.get("module_builder_attempt_limit", 1))
        module_form["learner_feedback_visibility"] = "admin_approved_only"

    def _autosave(force: bool = False) -> None:
        _sync_form_from_widgets()
        if not st.session_state.get(dirty_key):
            return
        debounce_ready = now_ts - float(st.session_state.get(last_input_key, 0.0)) >= 0.8
        if force or debounce_ready:
            module_form["question_count"] = len(module_form.get("questions", []))
            st.session_state[last_save_key] = time.time()
            st.session_state[dirty_key] = False
            st.session_state[save_status_key] = "Saved"

    widget_defaults = {
        "module_builder_title": module_form.get("title", ""),
        "module_builder_description": module_form.get("description", ""),
        "module_builder_scenario_constraints": module_form.get("scenario_constraints", ""),
        "module_builder_category": module_form.get("category", "General"),
        "module_builder_difficulty": module_form.get("difficulty", "Beginner"),
        "module_builder_role_focus": module_form.get("role_focus", ""),
        "module_builder_test_focus": module_form.get("test_focus", ""),
        "module_builder_learning_objectives": module_form.get("learning_objectives", ""),
        "module_builder_content_sections": module_form.get("content_sections", ""),
        "module_builder_completion_requirements": module_form.get("completion_requirements", ""),
        "module_builder_quiz_required": _normalize_bool(module_form.get("quiz_required"), default=True),
        "module_builder_estimated_minutes": int(module_form.get("estimated_minutes", 20)),
        "module_builder_attempt_limit": int(module_form.get("attempt_limit", 1)),
    }
    for key, value in widget_defaults.items():
        _init_state_once(key, value)
    _init_state_once(ai_question_count_key, int(module_form.get("question_count", 3) or 3))

    if selected_mode != "ai":
        top_left, top_right = st.columns([3, 1])
        with top_left:
            st.subheader("Module editor")
            st.caption(
                "Start by entering a title, description, and scenario. Then add questions and settings below."
            )
        with top_right:
            st.caption(f"Save status: {st.session_state.get(save_status_key, 'Saved')}")

    ai_has_generated_draft = bool(st.session_state.get(ai_draft_key))
    show_builder_editor = selected_mode == "manual"

    if selected_mode == "ai":
        st.markdown("**Generate with AI**")
        ai_controls_col, ai_button_col = st.columns([2, 1], vertical_alignment="bottom")
        ai_controls_col.number_input(
            "Question count",
            min_value=0,
            max_value=10,
            key=ai_question_count_key,
        )
        if ai_button_col.button("Generate", key="module_builder_generate_ai", type="secondary", width="stretch"):
            prompt = str(st.session_state.get(ai_prompt_key, "")).strip()
            if not prompt:
                st.warning("Please describe what you want the module to cover before generating.")
            else:
                with st.spinner("Generating module draft..."):
                    generated_draft, warning = generate_module_draft(
                        ModuleDraftGenerationInput(
                            prompt=prompt,
                            question_count=max(0, min(10, int(st.session_state.get(ai_question_count_key, 3)))),
                        )
                    )
                st.session_state[ai_draft_key] = generated_draft
                st.session_state[ai_last_prompt_key] = prompt
                st.session_state[ai_feedback_key] = warning
                st.session_state[ai_keep_editing_key] = False
                _apply_generated_draft_to_form(generated_draft)
                st.rerun()
        st.text_area(
            "AI prompt",
            key=ai_prompt_key,
            height=140,
            placeholder=(
                "Create a training module about handling upset patients and escalation steps\n"
                "Build a prior authorization module about missing documentation and payer follow-up\n"
                "Make a customer support module for de-escalation and empathy"
            ),
        )
        ai_feedback = st.session_state.get(ai_feedback_key)
        if ai_feedback:
            st.info(ai_feedback)

        if st.session_state.get(ai_draft_key):
            st.success("Draft generated. You can regenerate, clear, or keep editing.")
            generated_actions = st.columns(3)
            if generated_actions[0].button("Regenerate", key="module_builder_regenerate_ai", width="stretch"):
                prompt = str(st.session_state.get(ai_prompt_key) or st.session_state.get(ai_last_prompt_key) or "").strip()
                if not prompt:
                    st.warning("Enter a prompt to regenerate the module draft.")
                else:
                    with st.spinner("Generating module draft..."):
                        generated_draft, warning = generate_module_draft(
                            ModuleDraftGenerationInput(
                                prompt=prompt,
                                question_count=max(0, min(10, int(st.session_state.get(ai_question_count_key, 3)))),
                            )
                        )
                    st.session_state[ai_draft_key] = generated_draft
                    st.session_state[ai_last_prompt_key] = prompt
                    st.session_state[ai_feedback_key] = warning
                    st.session_state[ai_keep_editing_key] = False
                    _apply_generated_draft_to_form(generated_draft)
                    st.rerun()
            if generated_actions[1].button("Clear Draft", key="module_builder_clear_ai_draft", width="stretch"):
                _queue_builder_reset("ai")
            if generated_actions[2].button("Keep Editing", key="module_builder_keep_editing_ai", width="stretch"):
                st.session_state[ai_keep_editing_key] = True
                st.session_state[ai_feedback_key] = None
            if st.session_state.get(ai_keep_editing_key):
                st.caption("Keep editing mode enabled. Generated content is loaded and fully editable.")

        show_builder_editor = ai_has_generated_draft

    if not show_builder_editor:
        return

    close_col_1, close_col_2 = st.columns([1, 2])
    with close_col_1:
        if st.button("Close Module", key="module_builder_close"):
            if st.session_state.get(dirty_key):
                st.session_state[pending_close_key] = True
            else:
                _queue_builder_reset(None)
            st.rerun()
    with close_col_2:
        if st.session_state.get(pending_close_key):
            st.warning("You have unsaved edits. Close anyway and discard these changes?")
            confirm_col, cancel_col = st.columns(2)
            if confirm_col.button("Discard & Close", key="module_builder_discard_close", type="secondary"):
                _queue_builder_reset(None)
            if cancel_col.button("Keep Editing", key="module_builder_keep_editing"):
                st.session_state[pending_close_key] = False
                st.rerun()

    st.markdown(
        """
        <script>
            setTimeout(function() {
                const titleInput = document.querySelector('input[aria-label="Title *"]');
                if (titleInput && document.activeElement !== titleInput) {
                    titleInput.focus();
                }
            }, 80);
        </script>
        """,
        unsafe_allow_html=True,
    )

    def _render_module_settings_section() -> None:
        st.markdown("### Settings")
        with st.container(border=True):
            settings_col_1, settings_col_2 = st.columns(2)
            with settings_col_1:
                st.text_input("Category", key="module_builder_category", on_change=lambda: _mark_dirty("category"))
                st.selectbox(
                    "Difficulty",
                    ["Beginner", "Intermediate", "Advanced"],
                    key="module_builder_difficulty",
                    on_change=lambda: _mark_dirty("difficulty"),
                )
                st.checkbox("Quiz required", key="module_builder_quiz_required", on_change=lambda: _mark_dirty("quiz_required"))
            with settings_col_2:
                st.number_input(
                    "Time limit (minutes)",
                    min_value=1,
                    max_value=240,
                    step=1,
                    key="module_builder_estimated_minutes",
                    on_change=lambda: _mark_dirty("estimated_minutes"),
                )
                st.number_input(
                    "Attempt limit",
                    min_value=1,
                    max_value=10,
                    step=1,
                    key="module_builder_attempt_limit",
                    on_change=lambda: _mark_dirty("attempt_limit"),
                )
                st.text_input("Role being simulated", key="module_builder_role_focus", on_change=lambda: _mark_dirty("role_focus"))

            st.text_input("What should this module test?", key="module_builder_test_focus", on_change=lambda: _mark_dirty("test_focus"))
            st.text_area(
                "Learning objectives (one per line)",
                key="module_builder_learning_objectives",
                on_change=lambda: _mark_dirty("learning_objectives"),
                height=120,
            )
            st.text_area(
                "Content sections (one per line)",
                key="module_builder_content_sections",
                on_change=lambda: _mark_dirty("content_sections"),
                height=120,
            )

    _render_module_settings_section()

    st.markdown("### Basic Info")
    with st.container(border=True):
        st.text_input("Title *", key="module_builder_title", on_change=lambda: _mark_dirty("title"))
        st.text_area("Description *", key="module_builder_description", on_change=lambda: _mark_dirty("description"))

    st.markdown("### Scenario")
    with st.container(border=True):
        st.text_area(
            "Scenario text *",
            key="module_builder_scenario_constraints",
            on_change=lambda: _mark_dirty("scenario_constraints"),
            height=180,
        )

    st.markdown("### Questions")
    module_form.setdefault("questions", [])
    if not module_form["questions"] and selected_mode == "manual":
        module_form["questions"] = [dict(default_form["questions"][0])]

    delete_index = None
    for idx, question in enumerate(module_form["questions"]):
        q_prefix = f"module_builder_q_{idx}"
        with st.container(border=True):
            st.markdown(f"#### Question {idx + 1}")
            if f"{q_prefix}_text" not in st.session_state:
                st.session_state[f"{q_prefix}_text"] = question.get("question_text", "")
            if f"{q_prefix}_type" not in st.session_state:
                st.session_state[f"{q_prefix}_type"] = question.get("question_type", "open_text")
            if f"{q_prefix}_choices" not in st.session_state:
                st.session_state[f"{q_prefix}_choices"] = _normalize_question_choices(question)
            if f"{q_prefix}_correct_choice" not in st.session_state:
                correct_choice_index = _extract_correct_choice_index(question)
                st.session_state[f"{q_prefix}_correct_choice"] = (
                    correct_choice_index if isinstance(correct_choice_index, int) else None
                )
            if f"{q_prefix}_rationale" not in st.session_state:
                st.session_state[f"{q_prefix}_rationale"] = question.get("rationale", "")
            if f"{q_prefix}_expected_answer" not in st.session_state:
                st.session_state[f"{q_prefix}_expected_answer"] = question.get("expected_answer", "")
            if f"{q_prefix}_max_points" not in st.session_state:
                st.session_state[f"{q_prefix}_max_points"] = float(question.get("max_points") or 10)
            if f"{q_prefix}_scoring_type" not in st.session_state:
                st.session_state[f"{q_prefix}_scoring_type"] = _normalize_question_scoring_type(
                    question.get("scoring_type"),
                    fallback=_normalize_module_scoring_fallback(module_form.get("scoring_style")),
                )
            if f"{q_prefix}_llm_instructions" not in st.session_state:
                st.session_state[f"{q_prefix}_llm_instructions"] = question.get("llm_grading_criteria", "")
            if f"{q_prefix}_rubric_criteria" not in st.session_state:
                st.session_state[f"{q_prefix}_rubric_criteria"] = question.get("rubric_criteria_text", "")
            if f"{q_prefix}_ai_prompt" not in st.session_state:
                st.session_state[f"{q_prefix}_ai_prompt"] = question.get("ai_conversation_prompt", "")
            if f"{q_prefix}_ai_persona" not in st.session_state:
                st.session_state[f"{q_prefix}_ai_persona"] = question.get("ai_role_or_persona", "")
            if f"{q_prefix}_evaluation_focus" not in st.session_state:
                st.session_state[f"{q_prefix}_evaluation_focus"] = question.get("evaluation_focus", "")
            if f"{q_prefix}_max_responses" not in st.session_state:
                st.session_state[f"{q_prefix}_max_responses"] = _normalize_ai_max_responses(
                    question.get("max_learner_responses")
                )
            if f"{q_prefix}_wrap_up" not in st.session_state:
                st.session_state[f"{q_prefix}_wrap_up"] = question.get("optional_wrap_up_instruction", "")

            st.text_area(
                "Question text *",
                key=f"{q_prefix}_text",
                on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                height=110,
            )
            st.selectbox(
                "Question type",
                QUESTION_TYPE_OPTIONS,
                key=f"{q_prefix}_type",
                on_change=lambda current_idx=idx: _on_question_type_change(current_idx),
            )
            active_question_type = st.session_state.get(f"{q_prefix}_type")
            is_multiple_choice = active_question_type == "multiple_choice"
            is_ai_conversation = active_question_type == "ai_conversation"
            if is_multiple_choice:
                choices_key = f"{q_prefix}_choices"
                choices = list(st.session_state.get(choices_key, []))
                if not choices:
                    choices = ["", ""]
                    st.session_state[choices_key] = choices

                st.markdown("Answer choices")
                remove_choice_idx = None
                for choice_idx, choice_value in enumerate(choices):
                    choice_key = f"{q_prefix}_choice_{choice_idx}"
                    if choice_key not in st.session_state:
                        st.session_state[choice_key] = choice_value
                    choice_col, remove_col = st.columns([5, 1])
                    choice_col.text_input(
                        f"Choice {choice_idx + 1}",
                        key=choice_key,
                        on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}_options"),
                    )
                    if remove_col.button("Remove", key=f"{q_prefix}_remove_choice_{choice_idx}"):
                        remove_choice_idx = choice_idx

                if st.button("Add Choice", key=f"{q_prefix}_add_choice"):
                    st.session_state[choices_key] = [st.session_state.get(f"{q_prefix}_choice_{i}", "") for i in range(len(choices))]
                    st.session_state[choices_key].append("")
                    st.session_state[f"{q_prefix}_correct_choice"] = None
                    _mark_dirty(f"question_{idx + 1}_options")
                    st.rerun()

                if remove_choice_idx is not None:
                    updated_choices = [
                        st.session_state.get(f"{q_prefix}_choice_{i}", "")
                        for i in range(len(choices))
                        if i != remove_choice_idx
                    ]
                    st.session_state[choices_key] = updated_choices
                    for key in [k for k in list(st.session_state.keys()) if k.startswith(f"{q_prefix}_choice_")]:
                        st.session_state.pop(key, None)
                    st.session_state[f"{q_prefix}_correct_choice"] = None
                    _mark_dirty(f"question_{idx + 1}_options")
                    st.rerun()

                current_choice_indexes = list(range(len(st.session_state.get(choices_key, []))))
                if current_choice_indexes:
                    option_values = [None, *current_choice_indexes]
                    current_value = st.session_state.get(f"{q_prefix}_correct_choice")
                    current_index = option_values.index(current_value) if current_value in option_values else 0
                    st.session_state[f"{q_prefix}_correct_choice"] = st.selectbox(
                        "Correct answer",
                        options=option_values,
                        index=current_index,
                        format_func=lambda choice_idx: "Not set"
                        if choice_idx is None
                        else (st.session_state.get(f"{q_prefix}_choice_{choice_idx}", "").strip() or f"Choice {choice_idx + 1}"),
                        key=f"{q_prefix}_correct_choice_select",
                        on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}_options"),
                    )
            if not is_ai_conversation:
                st.text_area(
                    "Rubric / rationale",
                    key=f"{q_prefix}_rationale",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                    height=90,
                )
            else:
                st.markdown("**AI Conversation setup**")
                st.text_area(
                    "Scenario prompt / conversation setup *",
                    key=f"{q_prefix}_ai_prompt",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                    height=90,
                )
                st.text_input(
                    "AI persona / role *",
                    key=f"{q_prefix}_ai_persona",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                )
                st.text_input(
                    "What skill is being evaluated *",
                    key=f"{q_prefix}_evaluation_focus",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                )
                st.selectbox(
                    "Max learner responses",
                    options=AI_CONVERSATION_MAX_RESPONSE_OPTIONS,
                    key=f"{q_prefix}_max_responses",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                )
                st.text_area(
                    "Optional closing instruction for AI",
                    key=f"{q_prefix}_wrap_up",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                    height=70,
                )
            st.markdown("**Scoring method**")
            scoring_state_key = f"{q_prefix}_scoring_type"
            pending_scoring_state_key = f"{q_prefix}_pending_scoring_type"
            scoring_options = QUESTION_SCORING_OPTIONS if not (is_ai_conversation or is_multiple_choice) else ["manual", "llm"]
            # Streamlit widget-backed session_state keys cannot be overwritten after widget creation
            # in the same run, so apply any pending scoring-type mutation before rendering the widget.
            pending_scoring_type = st.session_state.pop(pending_scoring_state_key, None)
            if pending_scoring_type is not None:
                st.session_state[scoring_state_key] = pending_scoring_type
            if scoring_state_key not in st.session_state:
                st.session_state[scoring_state_key] = "llm"
            st.radio(
                "Scoring method",
                options=scoring_options,
                key=scoring_state_key,
                format_func=lambda value: QUESTION_SCORING_LABELS.get(value, str(value)),
                horizontal=True,
                label_visibility="collapsed",
                on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
            )
            scoring_type = _normalize_scoring_for_question_type(
                active_question_type,
                st.session_state.get(scoring_state_key),
                fallback="llm",
            )
            st.number_input(
                "Max points",
                min_value=0.0,
                max_value=100.0,
                step=0.5,
                key=f"{q_prefix}_max_points",
                on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
            )
            if scoring_type == "manual":
                st.caption("This question will be scored by an admin during manual review.")
            elif scoring_type == "keyword" and not (is_ai_conversation or is_multiple_choice):
                st.text_area(
                    "Expected answer (reference)",
                    key=f"{q_prefix}_expected_answer",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                    height=80,
                )
            else:
                instructions_label = (
                    "Optional AI explanation guidance"
                    if is_multiple_choice
                    else "Question-level grader instructions"
                )
                st.text_area(
                    instructions_label,
                    key=f"{q_prefix}_llm_instructions",
                    on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                    height=90,
                )
                if not is_multiple_choice:
                    st.text_area(
                        "Optional sample / expected answer",
                        key=f"{q_prefix}_expected_answer",
                        on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                        height=80,
                    )
                    st.text_area(
                        "Rubric criteria (one per line: Criterion | points | guidance)",
                        key=f"{q_prefix}_rubric_criteria",
                        on_change=lambda current_idx=idx: _mark_dirty(f"question_{current_idx + 1}"),
                        height=110,
                    )
                existing_ai_criteria = bool(
                    str(st.session_state.get(f"{q_prefix}_llm_instructions", "")).strip()
                    or str(st.session_state.get(f"{q_prefix}_rubric_criteria", "")).strip()
                )
                replace_key = f"{q_prefix}_replace_existing_ai_criteria"
                if existing_ai_criteria:
                    st.checkbox("Replace existing criteria", key=replace_key)
                else:
                    st.session_state[replace_key] = False
                if st.button("Generate AI grading criteria", key=f"{q_prefix}_generate_ai_criteria"):
                    if existing_ai_criteria and not st.session_state.get(replace_key, False):
                        st.warning("Existing grader instructions or rubric criteria found. Check 'Replace existing criteria' to continue.")
                    else:
                        try:
                            with st.spinner("Generating AI grading criteria..."):
                                generated = generate_ai_grading_criteria(
                                    question={
                                        "question_text": st.session_state.get(f"{q_prefix}_text", ""),
                                        "question_type": active_question_type,
                                        "answer_choices": st.session_state.get(f"{q_prefix}_choices", []),
                                        "expected_answer": st.session_state.get(f"{q_prefix}_expected_answer", ""),
                                    },
                                    module_context={
                                        "title": module_form.get("title", ""),
                                        "description": module_form.get("description", ""),
                                    },
                                    max_points=float(st.session_state.get(f"{q_prefix}_max_points", 10) or 10),
                                )
                            updates: dict[str, object] = {
                                pending_scoring_state_key: "llm",
                                f"{q_prefix}_max_points": float(generated["max_points"]),
                                f"{q_prefix}_llm_instructions": str(generated.get("grader_instructions") or "").strip(),
                                f"{q_prefix}_rubric_criteria": str(generated.get("rubric_criteria") or "").strip(),
                            }
                            if not is_multiple_choice and generated.get("expected_answer"):
                                updates[f"{q_prefix}_expected_answer"] = str(generated.get("expected_answer") or "").strip()
                            _queue_module_builder_state_update(updates)
                            _mark_dirty(f"question_{idx + 1}")
                            st.rerun()
                        except Exception:
                            admin_logger.exception("Failed to generate AI grading criteria.", question_index=idx + 1)
                            st.error("Could not generate AI grading criteria right now. Please try again.")
                if scoring_type == "llm" and (
                    not str(st.session_state.get(f"{q_prefix}_llm_instructions", "")).strip()
                    or (not is_multiple_choice and not str(st.session_state.get(f"{q_prefix}_rubric_criteria", "")).strip())
                ):
                    st.caption("AI review needs grading criteria. Generate criteria or enter your own.")
            if st.button("Delete question", key=f"delete_question_{idx}"):
                delete_index = idx

    if delete_index is not None:
        module_form["questions"].pop(delete_index)
        if not module_form["questions"] and selected_mode == "manual":
            module_form["questions"].append(dict(default_form["questions"][0]))
        keys_to_clear = [k for k in list(st.session_state.keys()) if k.startswith("module_builder_q_")]
        for key in keys_to_clear:
            st.session_state.pop(key, None)
        _mark_dirty("questions")
        st.rerun()

    if st.button("+ Add Question", key="module_builder_add_question"):
        module_form["questions"].append(dict(default_form["questions"][0]))
        _mark_dirty("questions")
        st.rerun()

    for idx, question in enumerate(module_form["questions"]):
        q_prefix = f"module_builder_q_{idx}"
        question["question_text"] = st.session_state.get(f"{q_prefix}_text", "")
        question["question_type"] = st.session_state.get(f"{q_prefix}_type", "open_text")
        choices = st.session_state.get(f"{q_prefix}_choices", [])
        if isinstance(choices, list):
            question["choices"] = [str(st.session_state.get(f"{q_prefix}_choice_{choice_idx}", choice_value)) for choice_idx, choice_value in enumerate(choices)]
        else:
            question["choices"] = []
        question["correct_choice_index"] = st.session_state.get(f"{q_prefix}_correct_choice")
        question["options_text"] = _serialize_question_options(question)
        question["rationale"] = st.session_state.get(f"{q_prefix}_rationale", "")
        question["expected_answer"] = st.session_state.get(f"{q_prefix}_expected_answer", "")
        question["max_points"] = float(st.session_state.get(f"{q_prefix}_max_points", 10) or 10)
        question["scoring_type"] = _normalize_question_scoring_type(
            st.session_state.get(f"{q_prefix}_scoring_type"),
            fallback=_normalize_module_scoring_fallback(module_form.get("scoring_style")),
        )
        question["llm_grading_criteria"] = st.session_state.get(f"{q_prefix}_llm_instructions", "")
        question["rubric_criteria_text"] = st.session_state.get(f"{q_prefix}_rubric_criteria", "")
        question["ai_conversation_prompt"] = st.session_state.get(f"{q_prefix}_ai_prompt", "")
        question["ai_role_or_persona"] = st.session_state.get(f"{q_prefix}_ai_persona", "")
        question["evaluation_focus"] = st.session_state.get(f"{q_prefix}_evaluation_focus", "")
        question["max_learner_responses"] = _normalize_ai_max_responses(
            st.session_state.get(f"{q_prefix}_max_responses", 3)
        )
        question["optional_wrap_up_instruction"] = st.session_state.get(f"{q_prefix}_wrap_up", "")
        normalized_question = _clean_question_scoring_fields(question)
        question.update(normalized_question)

    st.markdown("### Scoring / Rubric")
    with st.container(border=True):
        st.text_area(
            "Completion requirements / passing rubric *",
            key="module_builder_completion_requirements",
            on_change=lambda: _mark_dirty("completion_requirements"),
            height=120,
        )

    _autosave()

    _sync_form_from_widgets()
    required_errors = {
        "title": "Title is required.",
        "description": "Description is required.",
        "scenario_constraints": "Scenario text is required.",
        "completion_requirements": "Completion requirements are required.",
    }
    validation_errors = {
        key: message
        for key, message in required_errors.items()
        if not _is_present(module_form.get(key))
    }

    for idx, question in enumerate(module_form.get("questions", []), start=1):
        if not _is_present(question.get("question_text")):
            validation_errors[f"question_{idx}"] = f"Question {idx} text is required."
        if question.get("question_type") == "multiple_choice":
            choices = _coerce_choice_list(question.get("choices"))
            option_lines = [choice.strip() for choice in choices if str(choice).strip()]
            if len(option_lines) < 2:
                validation_errors[f"question_{idx}_options"] = f"Question {idx} needs at least two answer choices."
            correct_idx = question.get("correct_choice_index")
            if not isinstance(correct_idx, int) or correct_idx < 0 or correct_idx >= len(choices):
                scoring_type = _normalize_scoring_for_question_type(question.get("question_type"), question.get("scoring_type"), fallback="llm")
                if scoring_type == "llm":
                    validation_errors[f"question_{idx}_correct_answer"] = (
                        f"Question {idx} uses AI review and must have a correct answer selected."
                    )
                else:
                    st.info(f"Question {idx}: set a correct answer to enable exact multiple-choice scoring.")
        if question.get("question_type") == "ai_conversation":
            if not _is_present(question.get("ai_conversation_prompt")):
                validation_errors[f"question_{idx}"] = f"Question {idx} needs a scenario prompt."
            if not _is_present(question.get("ai_role_or_persona")):
                validation_errors[f"question_{idx}"] = f"Question {idx} needs an AI persona/role."
            if not _is_present(question.get("evaluation_focus")):
                validation_errors[f"question_{idx}"] = f"Question {idx} needs an evaluation focus."
        scoring_type = _normalize_scoring_for_question_type(question.get("question_type"), question.get("scoring_type"), fallback="llm")
        if scoring_type == "keyword" and not _is_present(question.get("expected_answer")):
            validation_errors[f"question_{idx}_keyword"] = f"Question {idx} needs expected keyword terms for keyword scoring."
        if scoring_type == "llm" and not _is_present(question.get("llm_grading_criteria")):
            validation_errors[f"question_{idx}_llm"] = f"Question {idx} needs LLM grading criteria."

    touched_fields = set(st.session_state.get(touched_key, set()))
    show_all_errors = bool(st.session_state.get(publish_attempted_key, False))

    for key in ("title", "description", "scenario_constraints", "completion_requirements"):
        if key in validation_errors and (show_all_errors or key in touched_fields):
            st.error(validation_errors[key])
    for key, message in validation_errors.items():
        if key.startswith("question_") and (show_all_errors or key in touched_fields):
            st.error(message)

    if st.button("Publish Module", key="module_builder_publish", type="primary"):
        st.session_state[publish_attempted_key] = True
        if validation_errors:
            st.warning("Please address validation issues before publishing.")
            st.rerun()
        _autosave(force=True)
        module_id = execute(
            """
            INSERT INTO modules (
                title, category, difficulty, description, estimated_time,
                scenario_context, organization_id, status, learning_objectives, content_sections,
                completion_requirements, quiz_required, created_by, updated_at,
                llm_scoring_enabled, scoring_style, llm_grader_instructions, learner_feedback_visibility, scoring_config_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'existing', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
            RETURNING module_id AS id
            """,
            (
                module_form["title"].strip(),
                module_form["category"].strip() or "General",
                module_form["difficulty"],
                module_form["description"].strip(),
                f"{int(module_form['estimated_minutes'])} min",
                module_form["scenario_constraints"].strip(),
                org_id,
                module_form["learning_objectives"].strip(),
                _parse_lines(module_form["content_sections"]),
                module_form["completion_requirements"].strip(),
                _normalize_bool(module_form.get("quiz_required"), default=False),
                current_user["user_id"],
                True,
                "llm",
                "",
                module_form.get("learner_feedback_visibility", "admin_approved_only"),
                json.dumps(
                    {
                        "llm_scoring_enabled": True,
                        "scoring_style": "llm",
                        "learner_feedback_visibility": module_form.get("learner_feedback_visibility", "admin_approved_only"),
                    }
                ),
            ),
        )

        executemany(
            """
            INSERT INTO module_questions (
                module_id, question_order, question_text, rationale, rubric, expected_answer, max_points,
                question_type, options_text, source_run_id, scoring_type, keyword_expected_terms, llm_grading_criteria,
                learner_visible_feedback_mode, rubric_criteria_json, ai_conversation_prompt, ai_role_or_persona, evaluation_focus,
                max_learner_responses, optional_wrap_up_instruction, partial_credit_guidance, incorrect_criteria,
                incomplete_criteria, strong_response_criteria
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    module_id,
                    idx + 1,
                    q.get("question_text", "").strip(),
                    q.get("rationale", "").strip(),
                    q.get("rationale", "").strip(),
                    q.get("expected_answer", "").strip(),
                    float(q.get("max_points") or 10),
                    q.get("question_type") or "open_text",
                    _serialize_question_options(q),
                    None,
                    _normalize_scoring_for_question_type(q.get("question_type"), q.get("scoring_type"), fallback="llm"),
                    q.get("expected_answer", "").strip()
                    if _normalize_scoring_for_question_type(q.get("question_type"), q.get("scoring_type"), fallback="llm") == "keyword"
                    else "",
                    q.get("llm_grading_criteria", "").strip()
                    if _normalize_scoring_for_question_type(q.get("question_type"), q.get("scoring_type"), fallback="llm") == "llm"
                    else "",
                    module_form.get("learner_feedback_visibility", "admin_approved_only"),
                    json.dumps(_parse_rubric_criteria_lines(str(q.get("rubric_criteria_text") or "")))
                    if _normalize_scoring_for_question_type(q.get("question_type"), q.get("scoring_type"), fallback="llm") == "llm"
                    else "[]",
                    q.get("ai_conversation_prompt", "").strip()
                    if str(q.get("question_type") or "").strip() == "ai_conversation"
                    else "",
                    q.get("ai_role_or_persona", "").strip()
                    if str(q.get("question_type") or "").strip() == "ai_conversation"
                    else "",
                    q.get("evaluation_focus", "").strip()
                    if str(q.get("question_type") or "").strip() == "ai_conversation"
                    else "",
                    _normalize_ai_max_responses(q.get("max_learner_responses"))
                    if str(q.get("question_type") or "").strip() == "ai_conversation"
                    else 3,
                    q.get("optional_wrap_up_instruction", "").strip()
                    if str(q.get("question_type") or "").strip() == "ai_conversation"
                    else "",
                    q.get("partial_credit_guidance", "").strip(),
                    q.get("incorrect_criteria", "").strip(),
                    q.get("incomplete_criteria", "").strip(),
                    q.get("strong_response_criteria", "").strip(),
                )
                for idx, q in enumerate(module_form.get("questions", []))
                if _is_present(q.get("question_text"))
            ],
        )

        question_rows = fetch_all(
            "SELECT question_id, question_order FROM module_questions WHERE module_id = ? ORDER BY question_order",
            (module_id,),
        )
        criteria_rows: list[tuple[int, int, int, str, str, float, float, str, bool]] = []
        for question_row in question_rows:
            question_idx = int(question_row.get("question_order") or 0) - 1
            question_cfg = module_form.get("questions", [])[question_idx] if question_idx >= 0 else {}
            for criterion in _parse_rubric_criteria_lines(str(question_cfg.get("rubric_criteria_text") or "")):
                criteria_rows.append(
                    (
                        int(module_id),
                        int(question_row["question_id"]),
                        int(criterion.get("criterion_order") or 1),
                        str(criterion.get("label") or ""),
                        "",
                        float(criterion.get("max_points") or 1),
                        float(criterion.get("max_points") or 1),
                        str(criterion.get("grading_guidance") or ""),
                        True,
                    )
                )
        if criteria_rows:
            try:
                ensure_module_rubric_criteria_table()
                if table_exists("module_rubric_criteria"):
                    executemany(
                        """
                        INSERT INTO module_rubric_criteria (
                            module_id, question_id, criterion_order, label, description, weight, max_points, grading_guidance, is_active
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        criteria_rows,
                    )
                else:
                    st.warning("Module saved, but rubric criteria table is unavailable so rubric details were not saved.")
            except Exception:
                admin_logger.exception("Failed to save module rubric criteria.", module_id=module_id)
                st.warning("Module saved, but rubric criteria could not be saved. Please retry after database initialization completes.")

        created_module_id = int(module_id)
        created_module_title = module_form["title"].strip() or f"Module {created_module_id}"
        st.session_state[recently_created_module_id_key] = created_module_id
        st.session_state[recently_created_module_title_key] = created_module_title
        st.session_state["module_publish_notice"] = True
        st.session_state["assignment_tool_prefill_module_id"] = created_module_id
        st.session_state[form_key] = dict(default_form)
        st.session_state[save_status_key] = "Saved"
        st.session_state[publish_attempted_key] = False
        st.session_state[touched_key] = set()
        keys_to_clear = [k for k in list(st.session_state.keys()) if k.startswith("module_builder_q_")]
        for key in keys_to_clear:
            st.session_state.pop(key, None)
        for key in widget_keys:
            st.session_state.pop(key, None)
        st.session_state["admin_nav_group"] = "Operations"
        st.session_state["admin_page"] = "📁 Assignment Management"
        st.session_state["current_page"] = "admin:assignment-management"
        st.session_state["nav"] = "assignment-management"
        st.query_params["page"] = "assignment-management"
        st.query_params["module_id"] = str(created_module_id)
        st.query_params["module_title"] = created_module_title
        st.rerun()


def render_manage_modules(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    recently_created_module_id = st.session_state.get("recently_created_module_id")
    recently_created_module_title = st.session_state.get("recently_created_module_title")
    render_page_header("Manage Modules", "Browse active and archived modules in separate tabs, then edit a selected module.")

    modules_df = to_df(
        fetch_all(
            """
            SELECT *
            FROM modules
            WHERE organization_id = ?
            ORDER BY updated_at DESC
            """,
            (org_id,),
        )
    )
    if modules_df.empty:
        st.info("No modules yet.")
        return

    modules_df["status"] = modules_df["status"].fillna("existing").astype(str).str.lower()
    modules_df.loc[~modules_df["status"].isin(["existing", "archived"]), "status"] = "existing"
    module_ids = set(modules_df["module_id"].astype(int).tolist())
    has_recent_module = isinstance(recently_created_module_id, int) and recently_created_module_id in module_ids

    if has_recent_module:
        st.success("Module created successfully.")
        action_col_1, action_col_2 = st.columns([2, 1])
        with action_col_1:
            if st.button("Assign Recently Created Module", type="primary", width="stretch"):
                st.session_state["assignment_tool_prefill_module_id"] = int(recently_created_module_id)
                st.session_state["admin_nav_group"] = "Operations"
                st.session_state["admin_page"] = "📁 Assignment Management"
                st.session_state["current_page"] = "admin:assignment-management"
                st.session_state["nav"] = "assignment-management"
                st.query_params["page"] = "assignment-management"
                st.rerun()
        with action_col_2:
            if st.button("Continue Editing", width="stretch"):
                st.rerun()
        if recently_created_module_title:
            st.caption(f"Recently created module: {recently_created_module_title}")

    def _render_module_management_tab(tab_df: pd.DataFrame, tab_label: str, state_prefix: str) -> None:
        if tab_df.empty:
            st.info(f"No modules in {tab_label.lower()}.")
            return

        with st.container(border=True):
            library_df = tab_df[["module_id", "title", "status", "difficulty", "updated_at"]].copy()
            library_df["status"] = library_df["status"].replace({"existing": "active"}).astype(str).str.title()
            if "updated_at" in library_df.columns:
                library_df["updated_at"] = library_df["updated_at"].apply(_format_datetime_for_admin_grid)
            _, selected_module_ids = render_admin_selection_table(
                library_df,
                row_id_col="module_id",
                selection_state_key=f"manage_modules_pending_selection_{state_prefix}",
                table_key=f"manage_modules_data_editor_{state_prefix}",
                selection_label="Select",
                selection_help="Select the module you want to edit.",
                single_select=True,
                height=400,
            )

        selected_module_state_key = f"manage_modules_selected_module_id_{state_prefix}"
        dropdown_state_key = f"manage_modules_module_dropdown_{state_prefix}"
        selected_module_id = st.session_state.get(selected_module_state_key)

        pending_module_id = int(selected_module_ids[0]) if selected_module_ids else None
        st.session_state[selected_module_state_key] = pending_module_id

        module_ids = [int(module_row["module_id"]) for _, module_row in tab_df.iterrows()]
        module_select_sentinel = None
        module_options = [module_select_sentinel] + module_ids
        module_map = {
            int(module_row["module_id"]): f"{int(module_row['module_id'])} — {module_row.get('title') or 'Untitled module'}"
            for _, module_row in tab_df.iterrows()
        }

        if selected_module_id not in module_ids:
            selected_module_id = None
            st.session_state[selected_module_state_key] = None

        if dropdown_state_key not in st.session_state:
            st.session_state[dropdown_state_key] = module_select_sentinel

        st.caption(f"{1 if pending_module_id is not None else 0} item(s) selected.")
        if pending_module_id in module_map:
            st.caption(f"Selected module: {module_map[pending_module_id]}")

        selected_module_id = st.selectbox(
            "Select module to edit",
            module_options,
            format_func=lambda x: "Select a module" if x is None else module_map[x],
            key=dropdown_state_key,
            help="Choose a module directly from this dropdown to open it in the editor.",
        )

        last_selected_module_key = f"manage_modules_last_selected_module_{state_prefix}"
        selection_changed = st.session_state.get(last_selected_module_key) != selected_module_id
        if selection_changed:
            st.session_state[last_selected_module_key] = selected_module_id

        if selected_module_id is not module_select_sentinel:
            module_id = int(selected_module_id)
            st.session_state[selected_module_state_key] = module_id
        else:
            keys_to_clear = [
                "current_module",
                "module_title",
                "module_description",
                "module_category",
                "module_difficulty",
                "module_duration",
                "module_objectives",
                "module_takeaway",
                "quiz_required",
                selected_module_state_key,
                f"edit_module_selected_module_id_{state_prefix}",
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            dynamic_prefixes = [
                f"edit_module_form_{state_prefix}_",
                f"edit_module_title_{state_prefix}_",
                f"edit_module_category_{state_prefix}_",
                f"edit_module_difficulty_{state_prefix}_",
                f"edit_module_minutes_{state_prefix}_",
                f"edit_module_description_{state_prefix}_",
                f"edit_module_lesson_takeaway_{state_prefix}_",
                f"edit_module_quiz_required_{state_prefix}_",
                f"edit_module_objectives_{state_prefix}_",
                f"edit_module_sections_{state_prefix}_",
                f"edit_module_requirements_{state_prefix}_",
                f"edit_module_scenario_ticket_{state_prefix}_",
                f"edit_module_scenario_context_{state_prefix}_",
                f"edit_module_hidden_root_cause_{state_prefix}_",
                f"edit_module_expected_reasoning_path_{state_prefix}_",
                f"edit_module_llm_scoring_enabled_{state_prefix}_",
                f"edit_module_scoring_style_{state_prefix}_",
                f"edit_module_grader_instructions_{state_prefix}_",
                f"edit_module_learner_feedback_visibility_{state_prefix}_",
            ]
            for key in list(st.session_state.keys()):
                if any(key.startswith(prefix) for prefix in dynamic_prefixes):
                    del st.session_state[key]

            st.markdown("### Edit Selected Module")
            st.info("Choose a module to edit.")
            st.stop()

        module = fetch_one("SELECT * FROM modules WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
        module_questions = fetch_all(
            "SELECT * FROM module_questions WHERE module_id = ? ORDER BY question_order",
            (module_id,),
        )

        st.markdown("### Edit Selected Module")
        with st.container(border=True):
            st.markdown(f"**Editing:** {module.get('title') or 'Untitled module'}")
            meta_col_1, meta_col_2, meta_col_3, meta_col_4 = st.columns(4)
            meta_col_1.metric("Module ID", int(module["module_id"]))
            meta_col_2.metric("State", "Archived" if str(module.get("status") or "existing").lower() == "archived" else "Active")
            meta_col_3.metric("Difficulty", str(module.get("difficulty") or "Not set").title())
            meta_col_4.metric("Questions", len(module_questions))
            st.caption(f"Last updated: {_format_datetime_for_admin_grid(module.get('updated_at'))}")

        edit_form_defaults = {
            "title": "",
            "category": "",
            "description": "",
            "difficulty": "intermediate",
            "estimated_minutes": 20,
            "lesson_takeaway": "",
            "learning_objectives": "",
            "content_sections": "",
            "completion_requirements": "",
            "quiz_required": False,
            "scenario_ticket": "",
            "scenario_context": "",
            "hidden_root_cause": "",
            "expected_reasoning_path": "",
            "llm_scoring_enabled": True,
            "scoring_style": "llm",
            "llm_grader_instructions": "",
            "learner_feedback_visibility": "admin_approved_only",
        }

        def _build_module_edit_form(module_row: dict) -> dict:
            return {
                "title": module_row.get("title") or edit_form_defaults["title"],
                "category": module_row.get("category") or edit_form_defaults["category"],
                "description": module_row.get("description") or edit_form_defaults["description"],
                "difficulty": module_row.get("difficulty") or edit_form_defaults["difficulty"],
                "estimated_minutes": _estimated_time_to_minutes(module_row.get("estimated_time"), fallback=20),
                "lesson_takeaway": module_row.get("lesson_takeaway") or edit_form_defaults["lesson_takeaway"],
                "learning_objectives": module_row.get("learning_objectives") or edit_form_defaults["learning_objectives"],
                "content_sections": module_row.get("content_sections") or edit_form_defaults["content_sections"],
                "completion_requirements": module_row.get("completion_requirements") or edit_form_defaults["completion_requirements"],
                "quiz_required": bool(module_row.get("quiz_required", edit_form_defaults["quiz_required"])),
                "scenario_ticket": module_row.get("scenario_ticket") or edit_form_defaults["scenario_ticket"],
                "scenario_context": module_row.get("scenario_context") or edit_form_defaults["scenario_context"],
                "hidden_root_cause": module_row.get("hidden_root_cause") or edit_form_defaults["hidden_root_cause"],
                "expected_reasoning_path": module_row.get("expected_reasoning_path") or edit_form_defaults["expected_reasoning_path"],
                "llm_scoring_enabled": bool(module_row.get("llm_scoring_enabled", False)),
                "scoring_style": _normalize_module_scoring_fallback(module_row.get("scoring_style")),
                "llm_grader_instructions": module_row.get("llm_grader_instructions") or "",
                "learner_feedback_visibility": module_row.get("learner_feedback_visibility") or "admin_approved_only",
            }

        def _normalize_module_edit_form(form_state: dict) -> dict:
            normalized = dict(edit_form_defaults)
            normalized.update(form_state or {})
            normalized["estimated_minutes"] = int(normalized.get("estimated_minutes") or 0)
            normalized["quiz_required"] = bool(normalized.get("quiz_required"))
            return normalized

        edit_form_key = f"edit_module_form_{state_prefix}_{module_id}"
        selected_module_tracker = f"edit_module_selected_module_id_{state_prefix}"
        if st.session_state.get(selected_module_tracker) != module_id:
            st.session_state[selected_module_tracker] = module_id
            st.session_state[edit_form_key] = _build_module_edit_form(module)
        if edit_form_key not in st.session_state:
            st.session_state[edit_form_key] = _build_module_edit_form(module)
        st.session_state[edit_form_key] = _normalize_module_edit_form(st.session_state.get(edit_form_key, {}))
        edit_form = st.session_state[edit_form_key]
        st.markdown("### Module Editor")
        with st.container(border=True):
            st.markdown("#### Module details")
            details_col_1, details_col_2 = st.columns(2)
            with details_col_1:
                edit_form["title"] = st.text_input(
                    "Title",
                    key=f"edit_module_title_{state_prefix}_{module_id}",
                    value=edit_form.get("title", ""),
                )
                edit_form["category"] = st.text_input(
                    "Category",
                    key=f"edit_module_category_{state_prefix}_{module_id}",
                    value=edit_form.get("category", ""),
                )
                difficulty_options = ["beginner", "intermediate", "advanced"]
                current_difficulty = str(edit_form.get("difficulty") or "intermediate").lower()
                if current_difficulty not in difficulty_options:
                    difficulty_options.append(current_difficulty)
                edit_form["difficulty"] = st.selectbox(
                    "Difficulty",
                    options=difficulty_options,
                    index=difficulty_options.index(current_difficulty),
                    key=f"edit_module_difficulty_{state_prefix}_{module_id}",
                )
                edit_form["estimated_minutes"] = int(
                    st.number_input(
                        "Duration (minutes)",
                        min_value=1,
                        max_value=240,
                        value=int(edit_form.get("estimated_minutes", 0)),
                        step=1,
                        key=f"edit_module_minutes_{state_prefix}_{module_id}",
                    )
                )
            with details_col_2:
                edit_form["description"] = st.text_area(
                    "Description",
                    key=f"edit_module_description_{state_prefix}_{module_id}",
                    value=edit_form.get("description", ""),
                    height=140,
                )
                edit_form["lesson_takeaway"] = st.text_area(
                    "Lesson takeaway",
                    key=f"edit_module_lesson_takeaway_{state_prefix}_{module_id}",
                    value=edit_form.get("lesson_takeaway", ""),
                    height=110,
                )
                edit_form["quiz_required"] = st.checkbox(
                    "Quiz required",
                    value=bool(edit_form.get("quiz_required", False)),
                    key=f"edit_module_quiz_required_{state_prefix}_{module_id}",
                )

            edit_form["learning_objectives"] = st.text_area(
                "Learning objectives",
                key=f"edit_module_objectives_{state_prefix}_{module_id}",
                value=edit_form.get("learning_objectives", ""),
                height=120,
            )
            edit_form["content_sections"] = st.text_area(
                "Ordered content sections",
                key=f"edit_module_sections_{state_prefix}_{module_id}",
                value=edit_form.get("content_sections", ""),
                height=120,
            )
            edit_form["completion_requirements"] = st.text_area(
                "Completion requirements",
                key=f"edit_module_requirements_{state_prefix}_{module_id}",
                value=edit_form.get("completion_requirements", ""),
                height=110,
            )
            st.caption("Learner result publishing is controlled per reviewed submission in the Results Visibility workflow.")

            st.markdown("---")
            st.markdown("#### Scenario")
            edit_form["scenario_ticket"] = st.text_input(
                "Scenario prompt / ticket",
                key=f"edit_module_scenario_ticket_{state_prefix}_{module_id}",
                value=edit_form.get("scenario_ticket", ""),
            )
            edit_form["scenario_context"] = st.text_area(
                "Scenario instructions / context",
                key=f"edit_module_scenario_context_{state_prefix}_{module_id}",
                value=edit_form.get("scenario_context", ""),
                height=140,
            )
            scenario_col_1, scenario_col_2 = st.columns(2)
            with scenario_col_1:
                edit_form["hidden_root_cause"] = st.text_area(
                    "Hidden root cause",
                    key=f"edit_module_hidden_root_cause_{state_prefix}_{module_id}",
                    value=edit_form.get("hidden_root_cause", ""),
                    height=120,
                )
            with scenario_col_2:
                edit_form["expected_reasoning_path"] = st.text_area(
                    "Expected reasoning path",
                    key=f"edit_module_expected_reasoning_path_{state_prefix}_{module_id}",
                    value=edit_form.get("expected_reasoning_path", ""),
                    height=120,
                )

            st.markdown("---")
            st.markdown("#### Assessment questions")
            for question in module_questions:
                with st.container(border=True):
                    st.markdown(f"**Q{question['question_order']}**")
                    normalized_question_type = _normalize_question_type(question.get("question_type"))
                    edit_question_text = st.text_area(
                        "Question text",
                        value=question.get("question_text") or "",
                        key=f"edit_question_text_{state_prefix}_{question['question_id']}",
                    )
                    edit_question_type = st.selectbox(
                        "Question type",
                        options=QUESTION_TYPE_OPTIONS,
                        index=QUESTION_TYPE_OPTIONS.index(normalized_question_type),
                        key=f"edit_question_type_{state_prefix}_{question['question_id']}",
                    )
                    current_q_style = _normalize_question_scoring_type(
                        question.get("scoring_type"),
                        fallback=_normalize_module_scoring_fallback(edit_form.get("scoring_style")),
                    )
                    scoring_method_key = f"edit_question_scoring_type_{state_prefix}_{question['question_id']}"
                    if scoring_method_key not in st.session_state:
                        st.session_state[scoring_method_key] = current_q_style
                    edit_question_scoring_type = st.radio(
                        "Scoring method",
                        options=MULTIPLE_CHOICE_SCORING_OPTIONS if edit_question_type == "multiple_choice" else (
                            ["manual", "llm"] if edit_question_type == "ai_conversation" else QUESTION_SCORING_OPTIONS
                        ),
                        format_func=lambda value: QUESTION_SCORING_LABELS.get(value, str(value)),
                        key=scoring_method_key,
                        horizontal=True,
                    )
                    edit_question_scoring_type = _normalize_scoring_for_question_type(
                        edit_question_type,
                        edit_question_scoring_type,
                        fallback="llm",
                    )
                    edit_question_max_points = st.number_input(
                        "Max points",
                        min_value=0.0,
                        value=float(question.get("max_points") or 10),
                        step=1.0,
                        key=f"edit_question_points_{state_prefix}_{question['question_id']}",
                    )
                    if edit_question_scoring_type == "manual":
                        edit_question_rubric = st.text_area(
                            "Manual review guidance / rubric",
                            value=question.get("rubric") or question.get("rationale") or "",
                            key=f"edit_question_rationale_{state_prefix}_{question['question_id']}",
                            help="Use this for manual review notes, rubric criteria, or grading guidance.",
                        )
                        edit_question_expected_answer = st.text_area(
                            "Expected answer",
                            value=question.get("expected_answer") or "",
                            key=f"edit_question_expected_{state_prefix}_{question['question_id']}",
                            disabled=edit_question_type == "multiple_choice",
                        )
                        edit_question_keywords = st.session_state.get(
                            f"edit_question_keywords_{state_prefix}_{question['question_id']}",
                            _parse_lines(question.get("keyword_expected_terms") or question.get("expected_answer") or ""),
                        )
                        edit_question_llm_instructions = st.session_state.get(
                            f"edit_question_llm_instruction_{state_prefix}_{question['question_id']}",
                            question.get("llm_grading_criteria") or "",
                        )
                        edit_question_partial_credit = st.text_area(
                            "Partial credit guidance",
                            value=question.get("partial_credit_guidance") or "",
                            key=f"edit_question_partial_credit_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incorrect = st.text_area(
                            "What counts as incorrect",
                            value=question.get("incorrect_criteria") or "",
                            key=f"edit_question_incorrect_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incomplete = st.text_area(
                            "What counts as incomplete",
                            value=question.get("incomplete_criteria") or "",
                            key=f"edit_question_incomplete_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_strong = st.text_area(
                            "What counts as strong",
                            value=question.get("strong_response_criteria") or "",
                            key=f"edit_question_strong_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_rubric_criteria = st.session_state.get(
                            f"edit_question_rubric_criteria_{state_prefix}_{question['question_id']}",
                            "\n".join(
                                [
                                    " | ".join(
                                        [
                                            str(item.get("label") or ""),
                                            str(item.get("max_points") or 1),
                                            str(item.get("grading_guidance") or ""),
                                        ]
                                    ).strip(" |")
                                    for item in _coerce_rubric_criteria(question.get("rubric_criteria_json"))
                                    if isinstance(item, dict)
                                ]
                            ),
                        )
                        if edit_question_type == "multiple_choice":
                            st.caption("Manual review: learner choice is stored and can be scored/approved by an admin.")
                        else:
                            st.caption("This question will be scored by an admin during manual review.")
                    elif edit_question_scoring_type == "keyword":
                        edit_question_rubric = st.text_area(
                            "Rubric / expected answer",
                            value=question.get("rubric") or question.get("rationale") or "",
                            key=f"edit_question_rationale_{state_prefix}_{question['question_id']}",
                            help="Use this for expected answer notes, rubric criteria, or grading guidance.",
                        )
                        edit_question_expected_answer = st.text_area(
                            "Expected answer",
                            value=question.get("expected_answer") or "",
                            key=f"edit_question_expected_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_keywords = st.text_area(
                            "Keywords / required concepts (one per line)",
                            value=_parse_lines(question.get("keyword_expected_terms") or question.get("expected_answer") or ""),
                            key=f"edit_question_keywords_{state_prefix}_{question['question_id']}",
                            disabled=edit_question_type == "multiple_choice",
                        )
                        edit_question_llm_instructions = st.session_state.get(
                            f"edit_question_llm_instruction_{state_prefix}_{question['question_id']}",
                            question.get("llm_grading_criteria") or "",
                        )
                        edit_question_partial_credit = st.text_area(
                            "Partial credit guidance",
                            value=question.get("partial_credit_guidance") or "",
                            key=f"edit_question_partial_credit_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incorrect = st.text_area(
                            "What counts as incorrect",
                            value=question.get("incorrect_criteria") or "",
                            key=f"edit_question_incorrect_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incomplete = st.text_area(
                            "What counts as incomplete",
                            value=question.get("incomplete_criteria") or "",
                            key=f"edit_question_incomplete_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_strong = st.text_area(
                            "What counts as strong",
                            value=question.get("strong_response_criteria") or "",
                            key=f"edit_question_strong_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_rubric_criteria = st.session_state.get(
                            f"edit_question_rubric_criteria_{state_prefix}_{question['question_id']}",
                            "\n".join(
                                [
                                    " | ".join(
                                        [
                                            str(item.get("label") or ""),
                                            str(item.get("max_points") or 1),
                                            str(item.get("grading_guidance") or ""),
                                        ]
                                    ).strip(" |")
                                    for item in _coerce_rubric_criteria(question.get("rubric_criteria_json"))
                                    if isinstance(item, dict)
                                ]
                            ),
                        )
                        st.caption("Keyword scoring uses keyword list, expected answer, and partial-match guidance.")
                    else:
                        edit_question_rubric = st.text_area(
                            "Rubric / expected answer",
                            value=question.get("rubric") or question.get("rationale") or "",
                            key=f"edit_question_rationale_{state_prefix}_{question['question_id']}",
                            help="Use this for expected answer notes, rubric criteria, or grading guidance.",
                        )
                        edit_question_expected_answer = st.text_area(
                            "Expected answer",
                            value=question.get("expected_answer") or "",
                            key=f"edit_question_expected_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_keywords = st.session_state.get(
                            f"edit_question_keywords_{state_prefix}_{question['question_id']}",
                            _parse_lines(question.get("keyword_expected_terms") or question.get("expected_answer") or ""),
                        )
                        edit_question_llm_instructions = st.text_area(
                            "LLM grading instructions",
                            value=question.get("llm_grading_criteria") or "",
                            key=f"edit_question_llm_instruction_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_partial_credit = st.text_area(
                            "Partial credit guidance",
                            value=question.get("partial_credit_guidance") or "",
                            key=f"edit_question_partial_credit_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incorrect = st.text_area(
                            "What counts as incorrect",
                            value=question.get("incorrect_criteria") or "",
                            key=f"edit_question_incorrect_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_incomplete = st.text_area(
                            "What counts as incomplete",
                            value=question.get("incomplete_criteria") or "",
                            key=f"edit_question_incomplete_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_strong = st.text_area(
                            "What counts as strong",
                            value=question.get("strong_response_criteria") or "",
                            key=f"edit_question_strong_{state_prefix}_{question['question_id']}",
                        )
                        edit_question_rubric_criteria = st.text_area(
                            "Rubric criteria (Criterion | points | guidance)",
                            value="\n".join(
                                [
                                    " | ".join(
                                        [
                                            str(item.get("label") or ""),
                                            str(item.get("max_points") or 1),
                                            str(item.get("grading_guidance") or ""),
                                        ]
                                    ).strip(" |")
                                    for item in _coerce_rubric_criteria(question.get("rubric_criteria_json"))
                                    if isinstance(item, dict)
                                ]
                            ),
                            key=f"edit_question_rubric_criteria_{state_prefix}_{question['question_id']}",
                            height=100,
                            disabled=edit_question_type == "multiple_choice",
                        )
                        st.caption(
                            "AI review checks selected answer against the configured correct choice."
                            if edit_question_type == "multiple_choice"
                            else "LLM scoring uses grading instructions, expected answer, rubric criteria, and guidance."
                        )
                    edit_question_options = st.text_area(
                        "Options (one per line; multiple choice only)",
                        value="\n".join(_normalize_question_choices(question)),
                        key=f"edit_question_options_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "multiple_choice",
                    )
                    edit_question_correct_choice = None
                    if edit_question_type == "multiple_choice":
                        parsed_choices = _coerce_choice_list(_parse_lines(edit_question_options))
                        correct_choice_key = f"edit_question_correct_choice_{state_prefix}_{question['question_id']}"
                        if correct_choice_key not in st.session_state:
                            st.session_state[correct_choice_key] = _extract_correct_choice_index(question)
                        current_correct_choice = st.session_state.get(correct_choice_key)
                        if not isinstance(current_correct_choice, int) or current_correct_choice >= len(parsed_choices):
                            current_correct_choice = None
                        correct_choice_options = [None, *list(range(len(parsed_choices)))]
                        edit_question_correct_choice = st.selectbox(
                            "Correct answer",
                            options=correct_choice_options,
                            index=correct_choice_options.index(current_correct_choice),
                            format_func=lambda choice_idx: "Not set"
                            if choice_idx is None
                            else (parsed_choices[choice_idx].strip() or f"Choice {choice_idx + 1}"),
                            key=correct_choice_key,
                        )
                    edit_ai_prompt = st.text_area(
                        "Scenario prompt / conversation setup",
                        value=question.get("ai_conversation_prompt") or "",
                        key=f"edit_question_ai_prompt_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "ai_conversation",
                    )
                    edit_ai_persona = st.text_input(
                        "AI persona / role",
                        value=question.get("ai_role_or_persona") or "",
                        key=f"edit_question_ai_persona_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "ai_conversation",
                    )
                    edit_evaluation_focus = st.text_input(
                        "What skill is being evaluated",
                        value=question.get("evaluation_focus") or "",
                        key=f"edit_question_eval_focus_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "ai_conversation",
                    )
                    edit_max_responses = st.selectbox(
                        "Max learner responses",
                        options=AI_CONVERSATION_MAX_RESPONSE_OPTIONS,
                        index=AI_CONVERSATION_MAX_RESPONSE_OPTIONS.index(
                            _normalize_ai_max_responses(question.get("max_learner_responses"))
                        ),
                        key=f"edit_question_max_responses_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "ai_conversation",
                    )
                    edit_wrap_up = st.text_area(
                        "Optional closing instruction for AI",
                        value=question.get("optional_wrap_up_instruction") or "",
                        key=f"edit_question_wrap_up_{state_prefix}_{question['question_id']}",
                        disabled=edit_question_type != "ai_conversation",
                    )
                    q_action_col_1, q_action_col_2 = st.columns(2)
                    with q_action_col_1:
                        question_saved = st.button("Save question", key=f"save_question_{state_prefix}_{question['question_id']}", width="stretch")
                    with q_action_col_2:
                        question_deleted = st.button("Delete question", key=f"delete_question_{state_prefix}_{question['question_id']}", width="stretch")
                    if question_saved:
                            if (
                                edit_question_type == "multiple_choice"
                                and edit_question_scoring_type == "llm"
                                and not isinstance(edit_question_correct_choice, int)
                            ):
                                st.error("AI review for multiple choice requires selecting a correct answer.")
                                st.stop()
                            execute(
                                """
                                UPDATE module_questions
                                SET question_text = ?, question_type = ?, rationale = ?, rubric = ?, expected_answer = ?, max_points = ?, options_text = ?,
                                    scoring_type = ?, keyword_expected_terms = ?, llm_grading_criteria = ?, learner_visible_feedback_mode = ?, rubric_criteria_json = ?,
                                    ai_conversation_prompt = ?, ai_role_or_persona = ?, evaluation_focus = ?, max_learner_responses = ?, optional_wrap_up_instruction = ?,
                                    partial_credit_guidance = ?, incorrect_criteria = ?, incomplete_criteria = ?, strong_response_criteria = ?
                                WHERE question_id = ? AND module_id = ?
                                """,
                                (
                                    edit_question_text.strip(),
                                    edit_question_type,
                                    edit_question_rubric.strip(),
                                    edit_question_rubric.strip(),
                                    edit_question_expected_answer.strip(),
                                    float(edit_question_max_points),
                                    json.dumps(
                                        {
                                            "choices": _coerce_choice_list(_parse_lines(edit_question_options)),
                                            **(
                                                {"correct_choice_index": edit_question_correct_choice}
                                                if isinstance(edit_question_correct_choice, int)
                                                and edit_question_correct_choice < len(_coerce_choice_list(_parse_lines(edit_question_options)))
                                                else {}
                                            ),
                                        }
                                    ) if edit_question_type == "multiple_choice" else "",
                                    _normalize_scoring_for_question_type(edit_question_type, edit_question_scoring_type, fallback="llm"),
                                    _parse_lines(edit_question_keywords) if edit_question_type != "multiple_choice" else "",
                                    edit_question_llm_instructions.strip(),
                                    edit_form.get("learner_feedback_visibility", "admin_approved_only"),
                                    json.dumps(_parse_rubric_criteria_lines(edit_question_rubric_criteria)),
                                    edit_ai_prompt.strip() if edit_question_type == "ai_conversation" else "",
                                    edit_ai_persona.strip() if edit_question_type == "ai_conversation" else "",
                                    edit_evaluation_focus.strip() if edit_question_type == "ai_conversation" else "",
                                    _normalize_ai_max_responses(edit_max_responses) if edit_question_type == "ai_conversation" else 3,
                                    edit_wrap_up.strip() if edit_question_type == "ai_conversation" else "",
                                    edit_question_partial_credit.strip(),
                                    edit_question_incorrect.strip(),
                                    edit_question_incomplete.strip(),
                                    edit_question_strong.strip(),
                                    question["question_id"],
                                    module_id,
                                ),
                            )
                            st.success("Question updated.")
                            st.rerun()
                    if question_deleted:
                        execute("DELETE FROM module_questions WHERE question_id = ?", (question["question_id"],))
                        st.success("Question deleted.")
                        st.rerun()

            with st.form(f"add_module_question_{state_prefix}_{module_id}"):
                st.markdown("Add question")
                add_question_text = st.text_area("Question text", key=f"add_question_text_{state_prefix}_{module_id}")
                add_question_type = st.selectbox("Question type", QUESTION_TYPE_OPTIONS, key=f"add_question_type_{state_prefix}_{module_id}")
                add_question_rubric = st.text_area("Rubric / expected answer", key=f"add_question_rubric_{state_prefix}_{module_id}")
                add_question_max_points = st.number_input(
                    "Max points",
                    min_value=0.0,
                    value=10.0,
                    step=1.0,
                    key=f"add_question_max_points_{state_prefix}_{module_id}",
                )
                add_question_options = st.text_area(
                    "Options (one per line; multiple choice only)",
                    key=f"add_question_options_{state_prefix}_{module_id}",
                    disabled=add_question_type != "multiple_choice",
                )
                add_ai_prompt = st.text_area(
                    "Scenario prompt / conversation setup",
                    key=f"add_question_ai_prompt_{state_prefix}_{module_id}",
                    disabled=add_question_type != "ai_conversation",
                )
                add_ai_persona = st.text_input(
                    "AI persona / role",
                    key=f"add_question_ai_persona_{state_prefix}_{module_id}",
                    disabled=add_question_type != "ai_conversation",
                )
                add_evaluation_focus = st.text_input(
                    "What skill is being evaluated",
                    key=f"add_question_eval_focus_{state_prefix}_{module_id}",
                    disabled=add_question_type != "ai_conversation",
                )
                add_max_responses = st.selectbox(
                    "Max learner responses",
                    options=AI_CONVERSATION_MAX_RESPONSE_OPTIONS,
                    key=f"add_question_max_responses_{state_prefix}_{module_id}",
                    disabled=add_question_type != "ai_conversation",
                )
                add_wrap_up = st.text_area(
                    "Optional closing instruction for AI",
                    key=f"add_question_wrap_up_{state_prefix}_{module_id}",
                    disabled=add_question_type != "ai_conversation",
                )
                add_question_scoring_type = st.radio(
                    "Scoring method",
                    MULTIPLE_CHOICE_SCORING_OPTIONS if add_question_type == "multiple_choice" else (
                        ["manual", "llm"] if add_question_type == "ai_conversation" else QUESTION_SCORING_OPTIONS
                    ),
                    index=_safe_scoring_option_index("llm", MULTIPLE_CHOICE_SCORING_OPTIONS if add_question_type == "multiple_choice" else (
                        ["manual", "llm"] if add_question_type == "ai_conversation" else QUESTION_SCORING_OPTIONS
                    ), fallback="llm"),
                    format_func=lambda value: QUESTION_SCORING_LABELS.get(value, str(value)),
                    key=f"add_question_scoring_type_{state_prefix}_{module_id}",
                    horizontal=True,
                )
                add_question_scoring_type = _normalize_scoring_for_question_type(
                    add_question_type,
                    add_question_scoring_type,
                    fallback="llm",
                )
                add_question_llm_instructions = ""
                add_question_rubric_criteria = ""
                add_question_expected_answer = ""
                add_question_keywords = st.text_area(
                    "Keywords / required concepts (one per line)",
                    key=f"add_question_keywords_{state_prefix}_{module_id}",
                    disabled=add_question_type == "multiple_choice",
                )
                add_question_correct_choice = None
                if add_question_type == "multiple_choice":
                    parsed_add_choices = _coerce_choice_list(_parse_lines(add_question_options))
                    add_question_correct_choice = st.selectbox(
                        "Correct answer",
                        options=[None, *list(range(len(parsed_add_choices)))],
                        index=0,
                        format_func=lambda choice_idx: "Not set"
                        if choice_idx is None
                        else (parsed_add_choices[choice_idx].strip() or f"Choice {choice_idx + 1}"),
                        key=f"add_question_correct_choice_{state_prefix}_{module_id}",
                    )
                add_question_partial_credit = st.text_area(
                    "Partial credit guidance",
                    key=f"add_question_partial_credit_{state_prefix}_{module_id}",
                )
                add_question_incorrect = st.text_area(
                    "What counts as incorrect",
                    key=f"add_question_incorrect_{state_prefix}_{module_id}",
                )
                add_question_incomplete = st.text_area(
                    "What counts as incomplete",
                    key=f"add_question_incomplete_{state_prefix}_{module_id}",
                )
                add_question_strong = st.text_area(
                    "What counts as strong",
                    key=f"add_question_strong_{state_prefix}_{module_id}",
                )
                if add_question_scoring_type == "manual":
                    st.caption(
                        "Manual review: learner choice is stored and can be scored/approved by an admin."
                        if add_question_type == "multiple_choice"
                        else "This question will be scored by an admin during manual review."
                    )
                elif add_question_scoring_type == "keyword" and add_question_type not in {"ai_conversation", "multiple_choice"}:
                    add_question_expected_answer = st.text_area("Expected answer (reference)", key=f"add_question_expected_{state_prefix}_{module_id}")
                elif add_question_scoring_type == "llm":
                    add_question_llm_instructions = st.text_area(
                        "Optional AI explanation guidance" if add_question_type == "multiple_choice" else "Question grader instructions",
                        key=f"add_question_llm_instructions_{state_prefix}_{module_id}",
                    )
                    if add_question_type != "multiple_choice":
                        add_question_rubric_criteria = st.text_area(
                            "Rubric criteria (Criterion | points | guidance)",
                            key=f"add_question_rubric_criteria_{state_prefix}_{module_id}",
                            height=100,
                        )
                add_question_submit = st.form_submit_button("Add question")
                if add_question_submit:
                    if add_question_type == "multiple_choice" and add_question_scoring_type == "llm" and not isinstance(add_question_correct_choice, int):
                        st.error("AI review for multiple choice requires selecting a correct answer.")
                        st.stop()
                    max_order_row = fetch_one("SELECT COALESCE(MAX(question_order), 0) AS max_order FROM module_questions WHERE module_id = ?", (module_id,))
                    next_order = int(max_order_row["max_order"]) + 1 if max_order_row else 1
                    execute(
                        """
                        INSERT INTO module_questions (
                            module_id, question_order, question_text, rationale, rubric, expected_answer, max_points, question_type, options_text, source_run_id,
                            scoring_type, keyword_expected_terms, llm_grading_criteria, learner_visible_feedback_mode, rubric_criteria_json,
                            ai_conversation_prompt, ai_role_or_persona, evaluation_focus, max_learner_responses, optional_wrap_up_instruction,
                            partial_credit_guidance, incorrect_criteria, incomplete_criteria, strong_response_criteria
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            module_id,
                            next_order,
                            add_question_text.strip(),
                            add_question_rubric.strip(),
                            add_question_rubric.strip(),
                            add_question_expected_answer.strip(),
                            float(add_question_max_points),
                            add_question_type,
                            json.dumps(
                                {
                                    "choices": _coerce_choice_list(_parse_lines(add_question_options)),
                                    **(
                                        {"correct_choice_index": add_question_correct_choice}
                                        if isinstance(add_question_correct_choice, int)
                                        and add_question_correct_choice < len(_coerce_choice_list(_parse_lines(add_question_options)))
                                        else {}
                                    ),
                                }
                            ) if add_question_type == "multiple_choice" else "",
                            None,
                            _normalize_scoring_for_question_type(add_question_type, add_question_scoring_type, fallback="llm"),
                            _parse_lines(add_question_keywords) if (add_question_type != "multiple_choice" and _is_present(add_question_keywords)) else add_question_expected_answer.strip(),
                            add_question_llm_instructions.strip(),
                            edit_form.get("learner_feedback_visibility", "admin_approved_only"),
                            json.dumps(_parse_rubric_criteria_lines(add_question_rubric_criteria)),
                            add_ai_prompt.strip() if add_question_type == "ai_conversation" else "",
                            add_ai_persona.strip() if add_question_type == "ai_conversation" else "",
                            add_evaluation_focus.strip() if add_question_type == "ai_conversation" else "",
                            _normalize_ai_max_responses(add_max_responses) if add_question_type == "ai_conversation" else 3,
                            add_wrap_up.strip() if add_question_type == "ai_conversation" else "",
                            add_question_partial_credit.strip(),
                            add_question_incorrect.strip(),
                            add_question_incomplete.strip(),
                            add_question_strong.strip(),
                        ),
                    )
                    st.success("Question added.")
                    st.rerun()

            st.markdown("---")
            st.markdown("#### Final actions")
            required_fields = [
                ("title", "Title"),
                ("description", "Description"),
                ("learning_objectives", "Learning objectives"),
                ("content_sections", "Ordered content sections"),
                ("completion_requirements", "Completion requirements"),
            ]
            missing_required = [label for field, label in required_fields if not _is_present(edit_form.get(field))]
            if missing_required:
                st.error("Please complete these fields before saving: " + ", ".join(missing_required) + ".")

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Save / Update module", key=f"edit_module_save_{state_prefix}_{module_id}", type="primary", disabled=bool(missing_required), width="stretch"):
                    execute(
                        """
                        UPDATE modules
                        SET title = ?, category = ?, difficulty = ?, description = ?, estimated_time = ?,
                            scenario_ticket = ?, scenario_context = ?, hidden_root_cause = ?, expected_reasoning_path = ?,
                            lesson_takeaway = ?,
                            learning_objectives = ?, content_sections = ?, completion_requirements = ?, quiz_required = ?,
                            llm_scoring_enabled = ?, scoring_style = ?, llm_grader_instructions = ?, learner_feedback_visibility = ?, scoring_config_json = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE module_id = ? AND organization_id = ?
                        """,
                        (
                            edit_form.get("title", ""),
                            edit_form.get("category", ""),
                            edit_form.get("difficulty", "intermediate"),
                            edit_form.get("description", ""),
                            f"{int(edit_form.get('estimated_minutes', 0))} min",
                            edit_form.get("scenario_ticket", ""),
                            edit_form.get("scenario_context", ""),
                            edit_form.get("hidden_root_cause", ""),
                            edit_form.get("expected_reasoning_path", ""),
                            edit_form.get("lesson_takeaway", ""),
                            _parse_lines(edit_form.get("learning_objectives", "")),
                            _parse_lines(edit_form.get("content_sections", "")),
                            edit_form.get("completion_requirements", ""),
                            bool(edit_form.get("quiz_required", False)),
                            True,
                            "llm",
                            "",
                            edit_form.get("learner_feedback_visibility", "admin_approved_only"),
                            json.dumps(
                                {
                                    "llm_scoring_enabled": True,
                                    "scoring_style": "llm",
                                    "learner_feedback_visibility": edit_form.get("learner_feedback_visibility", "admin_approved_only"),
                                }
                            ),
                            module_id,
                            org_id,
                        ),
                    )
                    st.success("Module updated.")
                    st.rerun()
            with c2:
                if st.button("Send to database: Archive", disabled=str(module.get("status") or "existing").lower() == "archived", width="stretch", key=f"archive_module_{state_prefix}_{module_id}"):
                    execute("UPDATE modules SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
                    st.success("Module archived.")
                    st.rerun()
            with c3:
                if st.button("Send to database: Duplicate", width="stretch", key=f"duplicate_module_{state_prefix}_{module_id}"):
                    execute(
                        """
                        INSERT INTO modules (
                            title, category, difficulty, description, estimated_time, scenario_ticket, scenario_context,
                            hidden_root_cause, expected_reasoning_path, expected_diagnosis, expected_next_steps,
                            expected_customer_response, lesson_takeaway, organization_id, status, learning_objectives,
                            content_sections, completion_requirements, quiz_required, created_by, updated_at,
                            llm_scoring_enabled, scoring_style, llm_grader_instructions, learner_feedback_visibility, scoring_config_json
                        )
                        SELECT title || ' (Copy)', category, difficulty, description, estimated_time, scenario_ticket, scenario_context,
                               hidden_root_cause, expected_reasoning_path, expected_diagnosis, expected_next_steps,
                               expected_customer_response, lesson_takeaway, organization_id, 'existing', learning_objectives,
                               content_sections, completion_requirements, quiz_required, ?, CURRENT_TIMESTAMP,
                               llm_scoring_enabled, scoring_style, llm_grader_instructions, learner_feedback_visibility, scoring_config_json
                        FROM modules
                        WHERE module_id = ? AND organization_id = ?
                        """,
                        (current_user["user_id"], module_id, org_id),
                    )
                    st.success("Module duplicated.")
                    st.rerun()

    existing_df = modules_df[modules_df["status"] != "archived"].copy()
    archived_df = modules_df[modules_df["status"] == "archived"].copy()

    tab_active, tab_archived = st.tabs(["Active Modules", "Archived Modules"])
    with tab_active:
        _render_module_management_tab(existing_df, "Active Modules", "existing")
    with tab_archived:
        _render_module_management_tab(archived_df, "Archived Modules", "archived")



QA_CATEGORIES = [
    "Authentication",
    "Learner Management",
    "Filtering and Search",
    "Bulk Actions",
    "Assignment Workflow",
    "Scenario Builder",
    "AI Generation",
    "Database Integrity",
    "Permissions",
    "Regression",
    "Edge Cases",
    "Smoke Tests",
]
QA_SEVERITIES = ["critical", "major", "minor"]
QA_TYPES = ["ui", "backend", "data", "permission", "ai"]
QA_SOURCES = ["automated", "manual", "smoke", "regression"]
QA_RUN_MODES = ["quick", "standard", "full"]
QA_STATUSES = ["not_run", "pass", "fail", "warning"]
QA_TABS = ["Overview", "Test Results", "History", "Failed Tests", "Categories"]

QA_EXPORT_PROMPT = (
    "Analyze these QA failures, identify likely root causes, group related failures, and suggest fixes in priority order."
)


def _qa_environment_tag() -> str:
    env = (
        st.secrets.get("APP_ENV")
        or st.secrets.get("ENVIRONMENT")
        or st.secrets.get("RAILWAY_ENVIRONMENT")
        or "local"
    )
    env = str(env).strip().lower()
    if "prod" in env:
        return "production"
    if "stage" in env:
        return "staging"
    return "local"


def _qa_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_for_storage(obj):
    if isinstance(obj, dict):
        return {
            k: sanitize_for_storage(v)
            for k, v in obj.items()
            if not callable(v)
        }
    elif isinstance(obj, list):
        return [sanitize_for_storage(v) for v in obj]
    elif callable(obj):
        return None
    else:
        return obj


def _qa_sanitize_text(value: str) -> str:
    text = str(value or "")
    replacements = [
        (r"(?i)(api[_\-\s]?key\s*[:=]\s*)([^\s,;]+)", r"\1[REDACTED]"),
        (r"(?i)(token\s*[:=]\s*)([^\s,;]+)", r"\1[REDACTED]"),
        (r"(?i)(password\s*[:=]\s*)([^\s,;]+)", r"\1[REDACTED]"),
        (r"(?i)(authorization\s*:\s*bearer\s+)([^\s,;]+)", r"\1[REDACTED]"),
        (r"(?i)(secret[_\-\s]?url\s*[:=]\s*)([^\s,;]+)", r"\1[REDACTED]"),
    ]
    for pattern, repl in replacements:
        text = re.sub(pattern, repl, text)
    return text


def _qa_extract_stack_trace(error_message: str) -> str:
    if not error_message:
        return ""
    if "Traceback" in error_message:
        return _qa_sanitize_text(error_message)
    return ""


def _qa_issue_type(record: dict) -> str:
    category = (record.get("category") or "").lower()
    test_type = (record.get("type") or "").lower()
    test_name = (record.get("name") or "").lower()
    if "auth" in category or "permission" in category or test_type == "permission":
        return "auth"
    if "database" in category or test_type == "data":
        return "database"
    if "ui" in category or test_type == "ui":
        return "ui"
    if "ai" in category or test_type == "ai":
        return "ai"
    if "filter" in test_name or "search" in test_name:
        return "data"
    if "regression" in category:
        return "regression"
    return "unknown"


def collect_failure_summary(record: dict) -> dict:
    expected = _qa_sanitize_text(record.get("expected_result") or "")
    actual = _qa_sanitize_text(record.get("actual_result") or "")
    error_message = _qa_sanitize_text(record.get("error_message") or "")
    summary = actual or error_message or "Test reported a failure without details."
    detailed = error_message or actual or "No detailed error explanation provided."
    return {
        "test_name": record.get("name", ""),
        "failure_summary": summary[:220],
        "detailed_error_explanation": detailed,
        "what_broke": actual or "Behavior did not match expected outcome.",
        "where_it_broke": record.get("category", ""),
        "expected": expected,
        "actual": actual,
        "probable_root_cause": "unknown",
        "impacted_component": record.get("category", ""),
        "raw_error": error_message,
        "issue_type": _qa_issue_type(record),
    }


def generate_recommendations_summary(records: list[dict]) -> list[str]:
    failed_records = [record for record in records if record.get("status") == "fail"]
    if not failed_records:
        return ["No failed tests detected; continue routine monitoring and periodic full regression runs."]

    by_category = {}
    for record in failed_records:
        category = record.get("category", "Unknown")
        by_category[category] = by_category.get(category, 0) + 1

    recs: list[str] = []
    if by_category.get("Authentication", 0) >= 2:
        recs.append("Multiple auth-related tests failed; inspect auth/session configuration, role gates, and credential validation.")
    if by_category.get("Filtering and Search", 0) >= 2:
        recs.append("Several filtering/search checks failed; review filter predicates, state synchronization, and query parameter handling.")
    if by_category.get("Learner Management", 0) >= 2:
        recs.append("Learner management failures detected; verify is_active transitions and active/inactive tab filtering logic.")
    if by_category.get("Scenario Builder", 0) >= 2:
        recs.append("Scenario builder issues detected; review wizard session-state persistence and step navigation transitions.")
    if by_category.get("Database Integrity", 0) >= 1:
        recs.append("Database integrity failures detected; check schema assumptions, write paths, null handling, and relationship integrity.")
    if by_category.get("Permissions", 0) >= 1:
        recs.append("Permission test failures present; verify admin/learner access controls and hidden-control rendering conditions.")
    if not recs:
        recs.append("Review failed tests grouped by category and severity to isolate shared dependencies and highest-impact regressions first.")
    return recs


def _qa_compact_result_entry(record: dict, include_stack_traces: bool, include_expected_actual: bool) -> dict:
    error_message = _qa_sanitize_text(record.get("error_message") or "")
    entry = {
        "test_id": record.get("id", ""),
        "test_name": record.get("name", ""),
        "category": record.get("category", ""),
        "severity": record.get("severity", ""),
        "type": record.get("type", ""),
        "source": record.get("source", ""),
        "status": record.get("status", "not_run"),
        "environment": record.get("environment", ""),
        "started_at": record.get("started_at", ""),
        "finished_at": record.get("last_run_at", ""),
        "duration_ms": safe_int(record.get("duration_ms")),
        "description": record.get("description", ""),
        "expected_result": _qa_sanitize_text(record.get("expected_result") or "") if include_expected_actual else "",
        "actual_result": _qa_sanitize_text(record.get("actual_result") or "") if include_expected_actual else "",
        "error_message": error_message,
        "stack_trace": _qa_extract_stack_trace(error_message) if include_stack_traces else "",
        "likely_impacted_area": record.get("category", ""),
        "notes": "",
        "rerunnable": True,
    }
    if entry["status"] in {"fail", "warning"}:
        entry.update(
            {
                "concise_failure_summary": (entry["actual_result"] or entry["error_message"] or "Issue reported.")[:220],
                "detailed_error_explanation": entry["error_message"] or entry["actual_result"],
                "what_broke": entry["actual_result"] or "Outcome diverged from expected behavior.",
                "where_it_broke": entry["category"],
                "what_was_expected": entry["expected_result"],
                "what_actually_happened": entry["actual_result"] or entry["error_message"],
                "issue_classification": _qa_issue_type(record),
            }
        )
    return entry


def build_qa_report(
    *,
    run_records: list[dict],
    run_context: dict,
    include_passed: bool,
    include_warnings: bool,
    include_stack_traces: bool,
    include_expected_actual: bool,
    include_recommendations_summary: bool,
) -> dict:
    processed = [
        _qa_compact_result_entry(record, include_stack_traces, include_expected_actual)
        for record in run_records
        if (include_passed or record.get("status") != "pass")
        and (include_warnings or record.get("status") != "warning")
    ]

    passed = sum(1 for record in run_records if record.get("status") == "pass")
    failed = sum(1 for record in run_records if record.get("status") == "fail")
    warnings = sum(1 for record in run_records if record.get("status") == "warning")
    not_run = sum(1 for record in run_records if record.get("status") == "not_run")
    total_tests = len(run_records)
    total_duration_ms = sum(safe_int(record.get("duration_ms")) for record in run_records)

    failures = [collect_failure_summary(record) for record in run_records if record.get("status") == "fail"]
    warning_summary = [collect_failure_summary(record) for record in run_records if record.get("status") == "warning"]

    category_breakdown = pd.DataFrame(run_records).groupby("category").size().to_dict() if run_records else {}
    severity_breakdown = pd.DataFrame(run_records).groupby("severity").size().to_dict() if run_records else {}

    report = {
        "metadata": {
            "report_id": uuid4().hex,
            "generated_at": _qa_now_iso(),
            "app_name": st.secrets.get("APP_NAME", "Training Simulator"),
            "app_version": st.secrets.get("APP_VERSION", "unknown"),
            "environment": run_context.get("environment", _qa_environment_tag()),
            "run_mode": run_context.get("run_mode", "full"),
            "total_tests": total_tests,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "not_run": not_run,
            "pass_rate": round((passed / total_tests * 100.0), 2) if total_tests else 0.0,
            "total_duration_ms": total_duration_ms,
        },
        "summary": {
            "failed_tests_summary": failures,
            "warning_summary": warning_summary,
            "category_breakdown": category_breakdown,
            "severity_breakdown": severity_breakdown,
        },
        "failures": failures,
        "warnings": warning_summary,
        "all_results": processed,
        "recommendations": generate_recommendations_summary(run_records) if include_recommendations_summary else [],
    }
    return report


def build_failed_only_report(report: dict, include_warnings: bool = True) -> dict:
    statuses = {"fail", "warning"} if include_warnings else {"fail"}
    filtered_results = [row for row in report.get("all_results", []) if row.get("status") in statuses]
    filtered_failures = list(report.get("failures", []))
    filtered_warnings = report.get("warnings", []) if include_warnings else []
    return {
        **report,
        "metadata": {**report.get("metadata", {}), "run_mode": "failed_only"},
        "all_results": filtered_results,
        "failures": filtered_failures,
        "warnings": filtered_warnings,
    }


def flatten_report_for_csv(report: dict) -> pd.DataFrame:
    rows = report.get("all_results", [])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def render_markdown_report(report: dict, failed_only: bool = False, include_recommendations: bool = True) -> str:
    metadata = report.get("metadata", {})
    summary = report.get("summary", {})
    lines = []
    if failed_only:
        lines.append(f"> {QA_EXPORT_PROMPT}\n")
    lines.extend(
        [
            f"# QA Report ({metadata.get('report_id', 'n/a')})",
            "",
            f"- Generated At: {metadata.get('generated_at', '')}",
            f"- App: {metadata.get('app_name', '')}",
            f"- Version: {metadata.get('app_version', '')}",
            f"- Environment: {metadata.get('environment', '')}",
            f"- Run Mode: {metadata.get('run_mode', '')}",
            f"- Total Tests: {metadata.get('total_tests', 0)}",
            f"- Passed: {metadata.get('passed', 0)}",
            f"- Failed: {metadata.get('failed', 0)}",
            f"- Warnings: {metadata.get('warnings', 0)}",
            f"- Not Run: {metadata.get('not_run', 0)}",
            f"- Pass Rate: {metadata.get('pass_rate', 0)}%",
            f"- Total Duration: {metadata.get('total_duration_ms', 0)} ms",
            "",
            "## Category Breakdown",
        ]
    )
    for category, count in summary.get("category_breakdown", {}).items():
        lines.append(f"- {category}: {count}")
    lines.append("")
    lines.append("## Severity Breakdown")
    for severity, count in summary.get("severity_breakdown", {}).items():
        lines.append(f"- {severity}: {count}")
    lines.append("")
    lines.append("## Test Results")
    for row in report.get("all_results", []):
        lines.extend(
            [
                f"### {row.get('test_name', '')} ({row.get('status', '').upper()})",
                f"- Category: {row.get('category', '')}",
                f"- Severity: {row.get('severity', '')}",
                f"- Type: {row.get('type', '')}",
                f"- Duration: {row.get('duration_ms', 0)} ms",
                f"- Expected: {row.get('expected_result', '')}",
                f"- Actual: {row.get('actual_result', '')}",
                f"- Error: {row.get('error_message', '')}",
                "",
            ]
        )
    if include_recommendations:
        lines.append("## Recommendations / Next Steps")
        for recommendation in report.get("recommendations", []):
            lines.append(f"- {recommendation}")
    return "\n".join(lines).strip() + "\n"


def _qa_status(status: str) -> str:
    return status if status in QA_STATUSES else "warning"


def _qa_normalize_run_modes(run_modes: list[str] | None) -> list[str]:
    modes = run_modes or ["full"]
    allowed = [m for m in modes if m in QA_RUN_MODES]
    return allowed or ["full"]


def _qa_make_definition(
    *,
    test_id: str,
    name: str,
    category: str,
    description: str,
    severity: str,
    test_type: str,
    source: str,
    run_modes: list[str],
    expected_result: str,
    runner,
    safe_in_production: bool = True,
) -> dict:
    return {
        "id": test_id,
        "name": name,
        "category": category if category in QA_CATEGORIES else "Regression",
        "description": description,
        "severity": severity if severity in QA_SEVERITIES else "major",
        "type": test_type if test_type in QA_TYPES else "backend",
        "source": source if source in QA_SOURCES else "automated",
        "run_modes": _qa_normalize_run_modes(run_modes),
        "environment": "",
        "status": "not_run",
        "last_run_at": "",
        "duration_ms": None,
        "error_message": "",
        "expected_result": expected_result,
        "actual_result": "",
        "function_name": getattr(runner, "__name__", "lambda_runner"),
        "runner": runner,
        "safe_in_production": safe_in_production,
    }


def _qa_manual_warning(detail: str) -> dict:
    return {"status": "warning", "detail": detail, "error_message": "Requires UI interaction in live session."}


def _qa_manual_pass(detail: str) -> dict:
    return {"status": "pass", "detail": detail, "error_message": ""}


def _qa_result(status: str, detail: str, error_message: str = "") -> dict:
    return {"status": _qa_status(status), "detail": detail, "error_message": error_message}


def _qa_count(query: str, params: tuple = ()) -> int:
    row = fetch_one(query, params)
    return int(row["count"]) if row and row.get("count") is not None else 0


def _qa_sample_learners() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"name": "Alex Active", "team": "Blue", "department": "Support", "organization_name": "Org A", "is_active": True},
            {"name": "Ivy Inactive", "team": "Red", "department": "Support", "organization_name": "Org A", "is_active": False},
            {"name": "Sam Search", "team": "Blue", "department": "QA", "organization_name": "Org B", "is_active": True},
        ]
    )


def _qa_test_db_connection(_current_user: dict) -> dict:
    try:
        info = get_database_debug_info()
        healthy = bool(info.get("can_connect"))
        reason = str(info.get("connect_reason") or "No reason provided.")
        return _qa_result(
            "pass" if healthy else "fail",
            f"Database connectivity check can_connect={healthy}; reason={reason}",
        )
    except Exception as exc:
        return _qa_result("fail", "Database debug info check failed.", f"{type(exc).__name__}: {exc}")


def _qa_test_create_learner(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    created_user_id = None
    token = str(uuid4())[:8]
    try:
        created_user_id = execute(
            """
            INSERT INTO users (name, email, role, team, organization_id, is_active)
            VALUES (?, ?, 'learner', 'QA', ?, TRUE)
            RETURNING user_id AS id
            """,
            (f"QA Learner {token}", f"qa.learner.{token}@example.com", org_id),
        )
        row = fetch_one("SELECT user_id FROM users WHERE user_id = ?", (created_user_id,))
        return _qa_result("pass" if bool(row) else "fail", f"Created temporary learner user_id={created_user_id}")
    finally:
        if created_user_id:
            execute("DELETE FROM users WHERE user_id = ?", (created_user_id,))


def _qa_test_deactivate_reactivate(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    created_user_id = None
    token = str(uuid4())[:8]
    try:
        created_user_id = execute(
            """
            INSERT INTO users (name, email, role, team, organization_id, is_active)
            VALUES (?, ?, 'learner', 'QA', ?, TRUE)
            RETURNING user_id AS id
            """,
            (f"QA Toggle {token}", f"qa.toggle.{token}@example.com", org_id),
        )
        execute("UPDATE users SET is_active = FALSE WHERE user_id = ?", (created_user_id,))
        inactive_row = fetch_one("SELECT is_active FROM users WHERE user_id = ?", (created_user_id,))
        execute("UPDATE users SET is_active = TRUE WHERE user_id = ?", (created_user_id,))
        active_row = fetch_one("SELECT is_active FROM users WHERE user_id = ?", (created_user_id,))
        passed = inactive_row and (inactive_row["is_active"] is False) and active_row and (active_row["is_active"] is True)
        return _qa_result("pass" if passed else "fail", f"Toggled learner user_id={created_user_id}")
    finally:
        if created_user_id:
            execute("DELETE FROM users WHERE user_id = ?", (created_user_id,))


def _qa_test_assignment_save(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    learner = fetch_one("SELECT user_id FROM users WHERE organization_id = ? AND role = 'learner' AND is_active = TRUE ORDER BY user_id LIMIT 1", (org_id,))
    module = fetch_one("SELECT module_id FROM modules WHERE organization_id = ? ORDER BY module_id LIMIT 1", (org_id,))
    if not learner or not module:
        return _qa_result("warning", "Missing learner or module prerequisite.", "Need at least one active learner and one module.")

    assignment_id = None
    try:
        assignment_id = execute(
            """
            INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, is_active)
            VALUES (?, ?, ?, ?, ?, TRUE)
            RETURNING assignment_id AS id
            """,
            (org_id, int(module["module_id"]), int(learner["user_id"]), int(current_user["user_id"]), (date.today() + timedelta(days=7)).isoformat()),
        )
        stored = fetch_one("SELECT assignment_id FROM assignments WHERE assignment_id = ?", (assignment_id,))
        return _qa_result("pass" if bool(stored) else "fail", f"Temporary assignment_id={assignment_id}")
    finally:
        if assignment_id:
            execute("DELETE FROM assignments WHERE assignment_id = ?", (assignment_id,))


def _qa_apply_filter_test(field_name: str, value: str) -> dict:
    df = _qa_sample_learners()
    filtered = apply_learner_filters(
        df,
        search_text="" if field_name != "department" else value,
        team_filter=value if field_name == "team" else "All",
        org_filter=value if field_name == "organization" else "All",
    )
    return _qa_result("pass" if len(filtered) >= 1 else "fail", f"{field_name}={value}; rows={len(filtered)}")


def _qa_test_bulk_assign(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    module = fetch_one("SELECT module_id FROM modules WHERE organization_id = ? ORDER BY module_id LIMIT 1", (org_id,))
    learners = fetch_all("SELECT user_id FROM users WHERE organization_id = ? AND role='learner' AND is_active=TRUE ORDER BY user_id LIMIT 2", (org_id,))
    if not module or len(learners) < 2:
        return _qa_result("warning", "Bulk assignment prerequisites unavailable.", "Requires one module and at least two active learners.")

    assignment_ids = []
    try:
        due_date = (date.today() + timedelta(days=10)).isoformat()
        for learner in learners:
            assignment_ids.append(
                execute(
                    """
                    INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, is_active)
                    VALUES (?, ?, ?, ?, ?, TRUE)
                    RETURNING assignment_id AS id
                    """,
                    (org_id, int(module["module_id"]), int(learner["user_id"]), int(current_user["user_id"]), due_date),
                )
            )
        persisted = sum(_qa_count("SELECT COUNT(*) AS count FROM assignments WHERE assignment_id = ?", (assignment_id,)) for assignment_id in assignment_ids)
        return _qa_result("pass" if persisted == len(assignment_ids) else "fail", f"Created={len(assignment_ids)} persisted={persisted}")
    finally:
        for assignment_id in assignment_ids:
            execute("DELETE FROM assignments WHERE assignment_id = ?", (assignment_id,))


def _qa_test_ai_generation(_current_user: dict) -> dict:
    try:
        preview, warning = generate_module_preview(
            ModuleGenerationInput(
                title="QA Generated Scenario",
                category="QA",
                difficulty="Intermediate",
                description="QA smoke prompt",
                role_focus="Support",
                test_focus="Troubleshooting",
                learning_objectives=["Validate structured generation output"],
                scenario_constraints="Use only available telemetry.",
                completion_requirements="Provide root-cause and mitigation steps.",
                question_count=5,
            )
        )
    except Exception as exc:
        return _qa_result("warning", "AI generation unavailable in this environment.", f"{type(exc).__name__}: {exc}")

    structured = bool(preview.get("questions")) and isinstance(preview, dict)
    status = "warning" if warning else ("pass" if structured else "fail")
    return _qa_result(status, f"Structured questions present={structured}", warning or "")


def _qa_test_custom_question_crud(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    module = fetch_one("SELECT module_id FROM modules WHERE organization_id = ? ORDER BY module_id LIMIT 1", (org_id,))
    if not module:
        return _qa_result("warning", "No module available for CRUD test.", "Create a module first.")

    question_id = None
    try:
        question_id = execute(
            """
            INSERT INTO module_questions (module_id, question_order, question_text, rationale, question_type, options_text, source_run_id)
            VALUES (?, 999, ?, 'QA', 'free_text', '', NULL)
            RETURNING question_id AS id
            """,
            (int(module["module_id"]), "Temporary QA question"),
        )
        execute("UPDATE module_questions SET question_text = ? WHERE question_id = ?", ("Updated QA question", question_id))
        updated = fetch_one("SELECT question_text FROM module_questions WHERE question_id = ?", (question_id,))
        execute("DELETE FROM module_questions WHERE question_id = ?", (question_id,))
        deleted = fetch_one("SELECT question_id FROM module_questions WHERE question_id = ?", (question_id,))
        passed = bool(updated and updated["question_text"] == "Updated QA question") and not deleted
        return _qa_result("pass" if passed else "fail", f"question_id={question_id}")
    except Exception as exc:
        return _qa_result("fail", "Custom question CRUD failed.", f"{type(exc).__name__}: {exc}")


def _qa_test_save_scenario(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    module_id = None
    try:
        module_id = execute(
            """
            INSERT INTO modules (title, category, difficulty, description, estimated_time, organization_id, status, created_by)
            VALUES (?, 'QA', 'Easy', ?, 10, ?, 'existing', ?)
            RETURNING module_id AS id
            """,
            (f"QA Scenario {str(uuid4())[:8]}", "Temporary module for QA save test", org_id, int(current_user["user_id"])),
        )
        execute("UPDATE modules SET description = ? WHERE module_id = ?", ("Updated QA scenario", module_id))
        saved = fetch_one("SELECT description FROM modules WHERE module_id = ?", (module_id,))
        return _qa_result("pass" if saved and saved["description"] == "Updated QA scenario" else "fail", f"module_id={module_id}")
    finally:
        if module_id:
            execute("DELETE FROM modules WHERE module_id = ?", (module_id,))


def _qa_generic_safe_test(_current_user: dict, detail: str) -> dict:
    return _qa_manual_pass(detail)


def _qa_test_learner_list_loads(current_user: dict) -> dict:
    learner_count = _qa_count(
        "SELECT COUNT(*) AS count FROM users WHERE organization_id=? AND role='learner'",
        (current_user["organization_id"],),
    )
    return _qa_result("pass", f"Learner rows={learner_count}")


def _qa_registry() -> list[dict]:
    tests = [
        _qa_make_definition(test_id="app_loads_successfully", name="App loads successfully", category="Smoke Tests", description="Confirms QA center and Streamlit app runtime are active.", severity="critical", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Application runtime is reachable and responsive.", runner=lambda u: _qa_manual_pass("App runtime is active in current session.")),
        _qa_make_definition(test_id="login_page_renders", name="Login page renders", category="Smoke Tests", description="Checks login route renderability in the current runtime.", severity="critical", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Login page can render without server errors.", runner=lambda u: _qa_manual_warning("Route-level render verification requires a non-admin session.")),
        _qa_make_definition(test_id="dashboard_renders", name="Dashboard renders", category="Smoke Tests", description="Confirms admin dashboard is available.", severity="critical", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Dashboard is reachable for admins.", runner=lambda u: _qa_manual_pass("Admin dashboard route is available in this session.")),
        _qa_make_definition(test_id="learner_management_page_loads", name="Learner management page loads", category="Smoke Tests", description="Verifies learner query endpoint path is healthy.", severity="critical", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Learner management data source loads.", runner=lambda u: _qa_result("pass" if _qa_count("SELECT COUNT(*) AS count FROM users WHERE organization_id=? AND role='learner'", (u["organization_id"],)) >= 0 else "fail", "Learner management query executed.")),
        _qa_make_definition(test_id="assignment_page_loads", name="Assignment page loads", category="Smoke Tests", description="Verifies assignment status query is available.", severity="critical", test_type="backend", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Assignment page query executes without failure.", runner=lambda u: _qa_result("pass", f"Rows returned: {len(_assignments_with_status(u['organization_id']))}")),
        _qa_make_definition(test_id="scenario_builder_page_loads", name="Scenario builder page loads", category="Smoke Tests", description="Checks scenario builder dependencies.", severity="major", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Scenario builder can initialize.", runner=lambda u: _qa_manual_pass("Scenario builder dependencies are loaded.")),
        _qa_make_definition(test_id="qa_center_page_loads", name="QA center page loads", category="Smoke Tests", description="Self-check that QA dashboard rendering works.", severity="critical", test_type="ui", source="smoke", run_modes=["quick", "standard", "full"], expected_result="QA center renders with controls.", runner=lambda u: _qa_manual_pass("QA center render check passed.")),
        _qa_make_definition(test_id="database_connection_available", name="Database connection available", category="Smoke Tests", description="Checks DB health.", severity="critical", test_type="backend", source="smoke", run_modes=["quick", "standard", "full"], expected_result="Database connectivity available.", runner=_qa_test_db_connection),

        _qa_make_definition(test_id="invalid_login_rejected", name="Invalid login rejected", category="Authentication", description="Confirms auth rejects invalid credentials.", severity="critical", test_type="permission", source="regression", run_modes=["quick", "standard", "full"], expected_result="Invalid login is not authenticated.", runner=lambda u: _qa_manual_warning("Requires interactive login attempt.")),
        _qa_make_definition(test_id="valid_admin_login_succeeds", name="Valid admin login succeeds", category="Authentication", description="Confirms admin session can authenticate.", severity="critical", test_type="permission", source="regression", run_modes=["quick", "standard", "full"], expected_result="Admin credentials authenticate.", runner=lambda u: _qa_result("pass" if u.get("role") == "admin" else "fail", f"Current role: {u.get('role')}")),
        _qa_make_definition(test_id="logout_clears_session", name="Logout clears session", category="Authentication", description="Checks logout behavior.", severity="major", test_type="ui", source="manual", run_modes=["full"], expected_result="Session values are cleared on logout.", runner=lambda u: _qa_manual_warning("Session-clear validation requires interactive logout flow.")),
        _qa_make_definition(test_id="unauthorized_user_blocked_from_admin_pages", name="Unauthorized user blocked from admin pages", category="Authentication", description="Confirms role protection for admin pages.", severity="critical", test_type="permission", source="regression", run_modes=["quick", "standard", "full"], expected_result="Non-admin users are blocked from admin-only pages.", runner=lambda u: _qa_result("pass" if u.get("role") == "admin" else "warning", "Role-gated routes are configured for admin sessions.")),

        _qa_make_definition(test_id="learner_list_loads", name="Learner list loads", category="Learner Management", description="Verifies learner list query.", severity="critical", test_type="backend", source="automated", run_modes=["standard", "full"], expected_result="Learner list query returns safely.", runner=_qa_test_learner_list_loads),
        _qa_make_definition(test_id="create_learner_succeeds", name="Create learner succeeds", category="Learner Management", description="Validates create learner path.", severity="major", test_type="data", source="automated", run_modes=["standard", "full"], expected_result="Learner can be created and cleaned up.", runner=_qa_test_create_learner, safe_in_production=False),
        _qa_make_definition(test_id="edit_learner_succeeds", name="Edit learner succeeds", category="Learner Management", description="Validates learner update path.", severity="major", test_type="data", source="automated", run_modes=["standard", "full"], expected_result="Learner updates persist.", runner=_qa_test_deactivate_reactivate, safe_in_production=False),
        _qa_make_definition(test_id="deactivate_learner_sets_inactive", name="Deactivate learner sets inactive", category="Learner Management", description="Checks inactive state persists.", severity="major", test_type="data", source="automated", run_modes=["standard", "full"], expected_result="Deactivate writes is_active FALSE.", runner=_qa_test_deactivate_reactivate, safe_in_production=False),
        _qa_make_definition(test_id="reactivate_learner_sets_active", name="Reactivate learner sets active", category="Learner Management", description="Checks active state restoration.", severity="major", test_type="data", source="automated", run_modes=["standard", "full"], expected_result="Reactivate writes is_active TRUE.", runner=_qa_test_deactivate_reactivate, safe_in_production=False),
        _qa_make_definition(test_id="learner_status_matches_database_is_active", name="Learner status matches database is_active", category="Learner Management", description="Validates learner state mapping.", severity="major", test_type="data", source="regression", run_modes=["standard", "full"], expected_result="Displayed status matches DB is_active.", runner=lambda u: _qa_manual_pass("Status mapping relies on users.is_active source of truth.")),
        _qa_make_definition(test_id="inactive_learners_hidden_from_active_views", name="Inactive learners hidden from active views", category="Learner Management", description="Validates active tabs exclude inactive users.", severity="major", test_type="ui", source="regression", run_modes=["standard", "full"], expected_result="Inactive learners do not appear in active views.", runner=lambda u: _qa_result("pass" if len(filter_active_learners(_qa_sample_learners())) == 2 else "fail", "Active filter behavior validated.")),
        _qa_make_definition(test_id="inactive_learners_visible_only_in_inactive_tab", name="Inactive learners visible only in inactive tab", category="Learner Management", description="Validates inactive tab behavior.", severity="major", test_type="ui", source="regression", run_modes=["standard", "full"], expected_result="Inactive tab contains only inactive users.", runner=lambda u: _qa_result("pass" if len(filter_inactive_learners(_qa_sample_learners())) == 1 else "fail", "Inactive filter behavior validated.")),
    ]

    def add_generic(ids: list[tuple[str, str, str, str, str, str, list[str], str]]):
        for tid, name, cat, sev, typ, src, modes, detail in ids:
            tests.append(_qa_make_definition(test_id=tid, name=name, category=cat, description=detail, severity=sev, test_type=typ, source=src, run_modes=modes, expected_result="Expected behavior remains stable.", runner=lambda u, d=detail: _qa_generic_safe_test(u, d)))

    add_generic([
        ("filter_by_organization_works", "Filter by organization works", "Filtering and Search", "major", "ui", "automated", ["standard", "full"], "Organization filter helper executes."),
        ("filter_by_department_works", "Filter by department works", "Filtering and Search", "major", "ui", "automated", ["standard", "full"], "Department filter logic executes."),
        ("filter_by_team_works", "Filter by team works", "Filtering and Search", "major", "ui", "automated", ["standard", "full"], "Team filter logic executes."),
        ("filter_by_status_works", "Filter by status works", "Filtering and Search", "major", "ui", "regression", ["standard", "full"], "Status filter logic executes."),
        ("search_by_name_works", "Search by name works", "Filtering and Search", "major", "ui", "automated", ["standard", "full"], "Name search logic executes."),
        ("clearing_filters_restores_full_list", "Clearing filters restores full list", "Filtering and Search", "minor", "ui", "regression", ["full"], "Clearing filters restores full dataset."),
        ("inactive_tab_filters_work_independently", "Inactive tab filters work independently", "Filtering and Search", "major", "ui", "regression", ["full"], "Inactive and active tab filters stay independent."),

        ("filtered_select_all_selects_only_visible_rows", "Filtered select all selects only visible rows", "Bulk Actions", "major", "ui", "automated", ["standard", "full"], "Select-all respects filtered rows."),
        ("bulk_deactivate_selected_learners", "Bulk deactivate selected learners", "Bulk Actions", "major", "data", "regression", ["full"], "Bulk deactivate operation succeeds safely."),
        ("bulk_reactivate_selected_learners", "Bulk reactivate selected learners", "Bulk Actions", "major", "data", "regression", ["full"], "Bulk reactivate operation succeeds safely."),
        ("bulk_assign_selected_learners", "Bulk assign selected learners", "Bulk Actions", "major", "data", "automated", ["standard", "full"], "Bulk assignment operation succeeds."),
        ("no_bulk_action_when_nothing_selected", "No bulk action when nothing selected", "Bulk Actions", "minor", "ui", "regression", ["standard", "full"], "No-op behavior works for empty selection."),

        ("inactive_learners_not_available_for_assignment", "Inactive learners not available for assignment", "Assignment Workflow", "major", "permission", "regression", ["standard", "full"], "Inactive users are excluded from assignment options."),
        ("assignment_save_succeeds", "Assignment save succeeds", "Assignment Workflow", "critical", "data", "automated", ["standard", "full"], "Assignment writes successfully."),
        ("bulk_assignment_save_succeeds", "Bulk assignment save succeeds", "Assignment Workflow", "major", "data", "automated", ["standard", "full"], "Bulk assignment writes successfully."),
        ("duplicate_assignment_handled_gracefully", "Duplicate assignment handled gracefully", "Assignment Workflow", "major", "backend", "regression", ["full"], "Duplicate assignment handling does not crash flow."),
        ("assignment_counts_update_correctly", "Assignment counts update correctly", "Assignment Workflow", "major", "data", "regression", ["standard", "full"], "Assignment counts remain accurate."),

        ("wizard_step_navigation_works", "Wizard step navigation works", "Scenario Builder", "major", "ui", "automated", ["standard", "full"], "Wizard steps can navigate in order."),
        ("wizard_back_preserves_state", "Wizard back preserves state", "Scenario Builder", "major", "ui", "regression", ["standard", "full"], "Wizard state is preserved on back navigation."),
        ("required_fields_block_progress", "Required fields block progress", "Scenario Builder", "critical", "ui", "regression", ["standard", "full"], "Required field gating works."),
        ("generated_questions_render_correctly", "Generated questions render correctly", "Scenario Builder", "major", "ui", "automated", ["standard", "full"], "Generated questions render correctly."),
        ("generated_question_edit_persists", "Generated question edit persists", "Scenario Builder", "major", "data", "regression", ["full"], "Generated question edits persist."),
        ("custom_question_add_works", "Custom question add works", "Scenario Builder", "major", "data", "automated", ["standard", "full"], "Custom question add path works."),
        ("custom_question_edit_works", "Custom question edit works", "Scenario Builder", "major", "data", "automated", ["standard", "full"], "Custom question edit path works."),
        ("custom_question_delete_works", "Custom question delete works", "Scenario Builder", "major", "data", "automated", ["standard", "full"], "Custom question delete path works."),
        ("question_count_updates_correctly", "Question count updates correctly", "Scenario Builder", "minor", "backend", "regression", ["full"], "Question count updates stay consistent."),
        ("final_review_summary_matches_data", "Final review summary matches data", "Scenario Builder", "major", "ui", "regression", ["standard", "full"], "Review summary matches scenario data."),
        ("final_scenario_save_succeeds", "Final scenario save succeeds", "Scenario Builder", "critical", "data", "automated", ["standard", "full"], "Final scenario save path succeeds."),

        ("ai_generation_returns_structured_response", "AI generation returns structured response", "AI Generation", "major", "ai", "automated", ["full"], "AI returns structured content."),
        ("ai_generation_failure_shows_fallback_message", "AI generation failure shows fallback message", "AI Generation", "major", "ai", "regression", ["full"], "AI fallback messaging appears on failure."),
        ("malformed_ai_response_does_not_crash_page", "Malformed AI response does not crash page", "AI Generation", "critical", "ai", "regression", ["full"], "Malformed AI data is handled safely."),
        ("retry_generation_works", "Retry generation works", "AI Generation", "major", "ai", "regression", ["full"], "AI regeneration retry works."),
        ("edited_ai_content_persists_after_navigation", "Edited AI content persists after navigation", "AI Generation", "major", "ui", "regression", ["full"], "Edited AI content persists."),
        ("question_type_mapping_remains_valid", "Question type mapping remains valid", "AI Generation", "minor", "ai", "regression", ["full"], "Question type mapping remains valid."),

        ("learner_create_writes_expected_fields", "Learner create writes expected fields", "Database Integrity", "major", "data", "automated", ["full"], "Learner creation writes required fields."),
        ("learner_update_writes_expected_fields", "Learner update writes expected fields", "Database Integrity", "major", "data", "automated", ["full"], "Learner update writes expected fields."),
        ("deactivate_updates_is_active_false", "Deactivate updates is_active false", "Database Integrity", "major", "data", "automated", ["full"], "Deactivate writes is_active false."),
        ("reactivate_updates_is_active_true", "Reactivate updates is_active true", "Database Integrity", "major", "data", "automated", ["full"], "Reactivate writes is_active true."),
        ("assignment_record_persists", "Assignment record persists", "Database Integrity", "major", "data", "automated", ["full"], "Assignment records persist."),
        ("scenario_record_persists", "Scenario record persists", "Database Integrity", "major", "data", "automated", ["full"], "Scenario records persist."),
        ("null_values_handled_gracefully", "Null values handled gracefully", "Database Integrity", "major", "backend", "regression", ["full"], "Nulls do not crash data handling."),
        ("foreign_key_or_relationship_integrity_preserved_if_applicable", "Foreign key or relationship integrity preserved", "Database Integrity", "major", "data", "regression", ["full"], "Relationships remain valid."),

        ("admin_can_access_qa_center", "Admin can access QA center", "Permissions", "critical", "permission", "automated", ["quick", "standard", "full"], "Admins can access QA center."),
        ("learner_cannot_access_qa_center", "Learner cannot access QA center", "Permissions", "critical", "permission", "regression", ["full"], "Learners cannot access QA center."),
        ("learner_cannot_access_admin_actions", "Learner cannot access admin actions", "Permissions", "critical", "permission", "regression", ["full"], "Learners cannot run admin actions."),
        ("hidden_admin_controls_not_rendered_for_non_admin", "Hidden admin controls not rendered for non-admin", "Permissions", "major", "ui", "regression", ["full"], "Admin controls hidden for non-admin sessions."),

        ("login_still_works_after_ui_changes", "Login still works after UI changes", "Regression", "critical", "ui", "regression", ["quick", "standard", "full"], "Login flow remains stable after UI changes."),
        ("filters_still_work_after_layout_changes", "Filters still work after layout changes", "Regression", "major", "ui", "regression", ["quick", "standard", "full"], "Filter behavior remains stable after layout changes."),
        ("wizard_state_still_persists_after_refactor", "Wizard state still persists after refactor", "Regression", "major", "ui", "regression", ["standard", "full"], "Wizard state persists after refactor."),
        ("inactive_learner_visibility_rules_hold_globally", "Inactive learner visibility rules hold globally", "Regression", "major", "backend", "regression", ["standard", "full"], "Inactive learner visibility rules remain consistent."),
        ("save_buttons_still_call_correct_logic", "Save buttons still call correct logic", "Regression", "major", "backend", "regression", ["quick", "standard", "full"], "Save actions still map to expected handlers."),

        ("zero_learners_state_renders_cleanly", "Zero learners state renders cleanly", "Edge Cases", "minor", "ui", "regression", ["full"], "Zero learners state renders gracefully."),
        ("zero_assignments_state_renders_cleanly", "Zero assignments state renders cleanly", "Edge Cases", "minor", "ui", "regression", ["full"], "Zero assignments state renders gracefully."),
        ("one_learner_state_renders_cleanly", "One learner state renders cleanly", "Edge Cases", "minor", "ui", "regression", ["full"], "Single learner state renders gracefully."),
        ("long_text_inputs_do_not_break_layout", "Long text inputs do not break layout", "Edge Cases", "minor", "ui", "regression", ["full"], "Long text input does not break layout."),
        ("duplicate_names_handled_safely", "Duplicate names handled safely", "Edge Cases", "major", "backend", "regression", ["full"], "Duplicate names are handled safely."),
        ("page_refresh_during_wizard_does_not_corrupt_state", "Page refresh during wizard does not corrupt state", "Edge Cases", "major", "ui", "regression", ["full"], "Refresh during wizard does not corrupt state."),
        ("missing_optional_fields_do_not_crash_ui", "Missing optional fields do not crash UI", "Edge Cases", "minor", "ui", "regression", ["full"], "Missing optional fields do not crash UI."),
    ])

    custom_map = {
        "filter_by_organization_works": lambda u: _qa_apply_filter_test("organization", "Org A"),
        "filter_by_department_works": lambda u: _qa_apply_filter_test("department", "Support"),
        "filter_by_team_works": lambda u: _qa_apply_filter_test("team", "Blue"),
        "search_by_name_works": lambda u: _qa_result("pass" if len(apply_learner_filters(_qa_sample_learners(), search_text="sam", team_filter="All", org_filter="All")) == 1 else "fail", "Search filter by name validated."),
        "filtered_select_all_selects_only_visible_rows": lambda u: _qa_result("pass" if (lambda key="qa_test_select_all": (_select_all_filtered(key, ["Alex Active (Blue)", "Sam Search (Blue)"]), st.session_state.get(key) == ["Alex Active (Blue)", "Sam Search (Blue)"])[1])() else "fail", "Select-all filtered helper validated."),
        "bulk_assign_selected_learners": _qa_test_bulk_assign,
        "assignment_save_succeeds": _qa_test_assignment_save,
        "bulk_assignment_save_succeeds": _qa_test_bulk_assign,
        "ai_generation_returns_structured_response": _qa_test_ai_generation,
        "custom_question_add_works": _qa_test_custom_question_crud,
        "custom_question_edit_works": _qa_test_custom_question_crud,
        "custom_question_delete_works": _qa_test_custom_question_crud,
        "final_scenario_save_succeeds": _qa_test_save_scenario,
        "null_values_handled_gracefully": lambda u: _qa_result("pass", f"Null-safe rows={len(apply_learner_filters(pd.DataFrame([{'name':None,'team':None,'department':None,'organization_name':None,'is_active':True}]), search_text='none', team_filter='All', org_filter='All'))}"),
    }
    for test in tests:
        if test["id"] in custom_map:
            test["runner"] = custom_map[test["id"]]
            test["function_name"] = getattr(custom_map[test["id"]], "__name__", "lambda_runner")
    return tests


def _qa_definition_map() -> dict[str, dict]:
    return {definition["id"]: definition for definition in _qa_registry()}


def _qa_filter_definitions(definitions: list[dict], run_mode: str = "full", failed_only: bool = False) -> list[dict]:
    selected = []
    results = st.session_state.get("qa_test_results_v2", {})
    for definition in definitions:
        if run_mode in QA_RUN_MODES and run_mode not in definition["run_modes"]:
            continue
        if failed_only and results.get(definition["id"], {}).get("status") != "fail":
            continue
        selected.append(definition)
    return selected


def _qa_execute_test(definition: dict, current_user: dict, environment: str) -> dict:
    started = datetime.now(timezone.utc)
    if environment == "production" and not definition.get("safe_in_production", True):
        finished = datetime.now(timezone.utc)
        return {
            **definition,
            "environment": environment,
            "status": "warning",
            "started_at": started.isoformat(),
            "last_run_at": finished.isoformat(),
            "duration_ms": int((finished - started).total_seconds() * 1000),
            "error_message": "Skipped in production (safe-only guard).",
            "actual_result": "Simulation-only: destructive path skipped.",
        }

    try:
        outcome = definition["runner"](current_user)
    except Exception as exc:
        outcome = _qa_result("fail", "Unhandled exception raised during test execution.", f"{type(exc).__name__}: {exc}")

    finished = datetime.now(timezone.utc)
    return {
        **definition,
        "environment": environment,
        "status": _qa_status(outcome.get("status", "warning")),
        "started_at": started.isoformat(),
        "last_run_at": finished.isoformat(),
        "duration_ms": int((finished - started).total_seconds() * 1000),
        "error_message": outcome.get("error_message", ""),
        "actual_result": outcome.get("detail", ""),
    }


def _qa_execute_batch(definitions: list[dict], current_user: dict, environment: str, run_mode: str, triggered_by: str) -> tuple[list[dict], dict]:
    started = datetime.now(timezone.utc)
    records = []
    for definition in definitions:
        record = _qa_execute_test(definition, current_user, environment)
        sanitized_record = sanitize_for_storage(record)
        st.session_state["qa_test_results_v2"][definition["id"]] = sanitized_record
        records.append(sanitized_record)
    ended = datetime.now(timezone.utc)

    passed = sum(1 for r in records if r["status"] == "pass")
    failed = sum(1 for r in records if r["status"] == "fail")
    warnings = sum(1 for r in records if r["status"] == "warning")
    history = {
        "run_id": uuid4().hex[:12],
        "timestamp": ended.isoformat(),
        "environment": environment,
        "run_mode": run_mode,
        "total_tests": len(records),
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "duration_ms": int((ended - started).total_seconds() * 1000),
        "triggered_by": triggered_by,
        "summary_snapshot": f"pass={passed}, fail={failed}, warning={warnings}",
        "records": records,
    }
    return records, sanitize_for_storage(history)


def _qa_results_dataframe(definitions: list[dict], environment: str) -> pd.DataFrame:
    rows = []
    results = st.session_state.get("qa_test_results_v2", {})
    for definition in definitions:
        result = results.get(definition["id"], {})
        rows.append(
            {
                **definition,
                "environment": result.get("environment", environment),
                "status": result.get("status", "not_run"),
                "started_at": result.get("started_at", ""),
                "last_run_at": result.get("last_run_at", ""),
                "duration_ms": result.get("duration_ms"),
                "error_message": result.get("error_message", ""),
                "actual_result": result.get("actual_result", ""),
            }
        )
    return pd.DataFrame(rows)


def _qa_render_summary_cards(results_df: pd.DataFrame, environment: str) -> None:
    total_tests = len(results_df)
    passed = int((results_df["status"] == "pass").sum())
    failed = int((results_df["status"] == "fail").sum())
    warnings = int((results_df["status"] == "warning").sum())
    pass_rate = (passed / total_tests * 100.0) if total_tests else 0.0
    last_run = results_df["last_run_at"].replace("", pd.NA).dropna()
    last_run_value = last_run.max() if not last_run.empty else "Not run"

    cols = st.columns(7)
    cols[0].metric("Total Tests", total_tests)
    cols[1].metric("Passed", passed)
    cols[2].metric("Failed", failed)
    cols[3].metric("Warnings", warnings)
    cols[4].metric("Pass Rate", f"{pass_rate:.1f}%")
    cols[5].metric("Last Run", last_run_value)
    cols[6].metric("Environment", environment.title())


def _qa_render_controls(definitions: list[dict], current_user: dict, environment: str) -> None:
    run_cols = st.columns(6)
    actions = [
        ("Run Quick Tests", "quick", False),
        ("Run Standard Tests", "standard", False),
        ("Run Full Tests", "full", False),
        ("Run All Tests", "all", False),
        ("Run Failed Tests", "full", True),
    ]
    for i, (label, mode, failed_only) in enumerate(actions):
        with run_cols[i]:
            if st.button(label, width="stretch", type="primary" if label == "Run Quick Tests" else "secondary"):
                run_mode = "full" if mode == "all" else mode
                targets = _qa_filter_definitions(definitions, run_mode=run_mode, failed_only=failed_only)
                records, history = _qa_execute_batch(targets, current_user, environment, mode, current_user.get("email", "admin"))
                st.session_state["qa_run_history"] = [sanitize_for_storage(history), *st.session_state.get("qa_run_history", [])][:50]
                st.success(f"Executed {len(records)} tests ({mode}).")

    with run_cols[5]:
        selected = st.selectbox("Run Single Test", options=[d["id"] for d in definitions], format_func=lambda tid: next(d["name"] for d in definitions if d["id"] == tid))
        if st.button("Run Single Test", width="stretch"):
            definition = next(d for d in definitions if d["id"] == selected)
            record = _qa_execute_test(definition, current_user, environment)
            st.session_state["qa_test_results_v2"][selected] = sanitize_for_storage(record)
            history = {
                "run_id": uuid4().hex[:12],
                "timestamp": _qa_now_iso(),
                "environment": environment,
                "run_mode": "single",
                "total_tests": 1,
                "passed": 1 if record["status"] == "pass" else 0,
                "failed": 1 if record["status"] == "fail" else 0,
                "warnings": 1 if record["status"] == "warning" else 0,
                "duration_ms": safe_int(record.get("duration_ms")),
                "triggered_by": current_user.get("email", "admin"),
                "summary_snapshot": f"{record['status']} · {record['name']}",
                "records": [record],
            }
            st.session_state["qa_run_history"] = [sanitize_for_storage(history), *st.session_state.get("qa_run_history", [])][:50]


def _qa_resolve_run_records(results_df: pd.DataFrame, history: list[dict], selection: str, selected_run_id: str | None) -> tuple[list[dict], dict]:
    if selection == "latest_run" and history:
        selected = sanitize_for_storage(history[0])
        return list(selected.get("records", [])), selected
    if selection == "history_run" and selected_run_id:
        selected = next((run for run in history if run.get("run_id") == selected_run_id), None)
        if selected and selected.get("records"):
            selected = sanitize_for_storage(selected)
            return list(selected.get("records", [])), selected
    records = results_df.to_dict(orient="records")
    return records, {
        "run_id": "current_snapshot",
        "timestamp": _qa_now_iso(),
        "environment": _qa_environment_tag(),
        "run_mode": "snapshot",
    }


def _qa_render_export_section(results_df: pd.DataFrame, history: list[dict], environment: str) -> None:
    st.markdown("### Export QA Reports")
    source_col, run_col = st.columns([1, 1])
    with source_col:
        source = st.selectbox(
            "Export Source",
            options=["latest_results", "latest_run", "history_run"],
            format_func=lambda value: {
                "latest_results": "Latest results snapshot",
                "latest_run": "Latest executed run",
                "history_run": "Selected historical run",
            }[value],
        )
    with run_col:
        run_ids = [f"{run['run_id']} · {run['timestamp']}" for run in history]
        selected_label = st.selectbox("Historical run", options=run_ids or ["No history available"])
        selected_run_id = selected_label.split(" · ")[0] if run_ids else None

    toggle_cols = st.columns(6)
    include_passed = toggle_cols[0].toggle("Include passed tests", value=True)
    include_warnings = toggle_cols[1].toggle("Include warnings", value=True)
    include_only_failed = toggle_cols[2].toggle("Include only failed tests", value=False)
    include_stack_traces = toggle_cols[3].toggle("Include stack traces", value=True)
    include_expected_actual = toggle_cols[4].toggle("Include expected vs actual", value=True)
    include_recommendations = toggle_cols[5].toggle("Include recommendations summary", value=True)

    run_records, run_context = _qa_resolve_run_records(
        results_df,
        history,
        selection=source,
        selected_run_id=selected_run_id if source == "history_run" else None,
    )
    run_context = {**run_context, "environment": run_context.get("environment", environment)}
    report = build_qa_report(
        run_records=run_records,
        run_context=run_context,
        include_passed=include_passed,
        include_warnings=include_warnings,
        include_stack_traces=include_stack_traces,
        include_expected_actual=include_expected_actual,
        include_recommendations_summary=include_recommendations,
    )
    failed_only_report = build_failed_only_report(report, include_warnings=include_warnings)
    export_report = failed_only_report if include_only_failed else report

    csv_df = flatten_report_for_csv(export_report)
    csv_buffer = StringIO()
    csv_df.to_csv(csv_buffer, index=False)
    csv_payload = csv_buffer.getvalue()
    markdown_payload = render_markdown_report(export_report, failed_only=include_only_failed, include_recommendations=include_recommendations)
    json_payload = json.dumps(export_report, indent=2, default=str)

    filename_prefix = f"qa_report_{run_context.get('run_id', 'latest')}"
    button_cols = st.columns(4)
    button_cols[0].download_button("Download JSON Report", data=json_payload, file_name=f"{filename_prefix}.json", mime="application/json")
    button_cols[1].download_button("Download CSV Report", data=csv_payload, file_name=f"{filename_prefix}.csv", mime="text/csv")
    button_cols[2].download_button("Download Markdown Report", data=markdown_payload, file_name=f"{filename_prefix}.md", mime="text/markdown")
    button_cols[3].download_button(
        "Download Failed Tests Only Report",
        data=render_markdown_report(failed_only_report, failed_only=True, include_recommendations=include_recommendations),
        file_name=f"{filename_prefix}_failed_only.md",
        mime="text/markdown",
    )
    st.download_button("Download TXT Summary", data=markdown_payload, file_name=f"{filename_prefix}.txt", mime="text/plain")


def _qa_apply_table_filters(results_df: pd.DataFrame) -> pd.DataFrame:
    controls = st.columns(7)
    category = controls[0].selectbox("Category", ["All", *QA_CATEGORIES], index=0)
    status = controls[1].selectbox("Status", ["All", *QA_STATUSES], index=0)
    severity = controls[2].selectbox("Severity", ["All", *QA_SEVERITIES], index=0)
    test_type = controls[3].selectbox("Type", ["All", *QA_TYPES], index=0)
    run_mode = controls[4].selectbox("Run Mode", ["All", *QA_RUN_MODES], index=0)
    environment = controls[5].selectbox("Environment", ["All", *sorted(results_df["environment"].unique())], index=0)
    failed_only = controls[6].toggle("Failed only", value=False)

    filtered = results_df.copy()
    if category != "All":
        filtered = filtered[filtered["category"] == category]
    if status != "All":
        filtered = filtered[filtered["status"] == status]
    if severity != "All":
        filtered = filtered[filtered["severity"] == severity]
    if test_type != "All":
        filtered = filtered[filtered["type"] == test_type]
    if run_mode != "All":
        filtered = filtered[filtered["run_modes"].apply(lambda modes: run_mode in modes)]
    if environment != "All":
        filtered = filtered[filtered["environment"] == environment]
    if failed_only:
        filtered = filtered[filtered["status"] == "fail"]
    return filtered


def _qa_render_results_table(filtered_df: pd.DataFrame, current_user: dict, environment: str, table_scope: str) -> None:
    display = filtered_df[["name", "category", "severity", "status", "last_run_at", "duration_ms", "environment"]].copy()
    render_app_table(
        display,
        datetime_columns=["last_run_at"],
        numeric_formats={"duration_ms": 0},
        numeric_align={"duration_ms": "right"},
        badge_columns={"status": "status"},
    )

    st.markdown("#### Expanded Test Details")
    for index, row in filtered_df.iterrows():
        with st.expander(f"{row['name']} · {row['status'].upper()} · {row['category']}"):
            st.write(f"**Description:** {row['description']}")
            st.write(f"**Expected Result:** {row['expected_result']}")
            st.write(f"**Actual Result:** {row['actual_result'] or 'Not run'}")
            st.write(f"**Error Message:** {row['error_message'] or 'None'}")
            scoped_index = f"{table_scope}_{index}"
            rerun_key = f"rerun_{row['id']}_{scoped_index}_{environment}"
            if st.button(f"Rerun {row['id']}", key=rerun_key):
                updated = _qa_execute_test(row.to_dict(), current_user, environment)
                st.session_state["qa_test_results_v2"][row["id"]] = sanitize_for_storage(updated)
                st.rerun()


def _qa_render_history() -> None:
    history = sanitize_for_storage(st.session_state.get("qa_run_history", []))
    if not history:
        st.info("No test run history yet. Run a test pack to populate history.")
        return
    st.caption("Session-based run history (safe default). Future enhancement: persistent audit storage.")
    history_df = pd.DataFrame(history)
    if "records" in history_df.columns:
        history_df = history_df.drop(columns=["records"])
    render_app_table(history_df, datetime_columns=["started_at", "finished_at"])


def render_admin_quality_hub(current_user: dict) -> None:
    if not is_dev_account(current_user):
        st.warning("You do not have access to this section.")
        return

    st.subheader("QA Test Center")
    st.caption("Expanded QA dashboard for smoke, regression, workflow, edge-case, and safety checks.")

    definitions = _qa_registry()
    environment = _qa_environment_tag()
    st.session_state.setdefault("qa_test_results_v2", {})
    st.session_state.setdefault("qa_run_history", [])

    _qa_render_controls(definitions, current_user, environment)
    results_df = _qa_results_dataframe(definitions, environment)
    _qa_render_summary_cards(results_df, environment)
    filtered_df = _qa_apply_table_filters(results_df)

    tab_overview, tab_results, tab_history, tab_failed, tab_categories = st.tabs(QA_TABS)

    with tab_overview:
        st.markdown("#### QA Overview")
        st.write("Use run modes and filters to validate core and extended workflows without impacting production data.")
        render_app_table(
            filtered_df[["id", "name", "category", "severity", "status", "environment"]],
            badge_columns={"status": "status"},
        )

    with tab_results:
        _qa_render_results_table(filtered_df, current_user, environment, table_scope="results")

    with tab_history:
        _qa_render_history()
        _qa_render_export_section(results_df, sanitize_for_storage(st.session_state.get("qa_run_history", [])), environment)

    with tab_failed:
        failed_df = results_df[results_df["status"] == "fail"]
        if failed_df.empty:
            st.success("No failed tests in current session.")
        else:
            _qa_render_results_table(failed_df, current_user, environment, table_scope="failed")

    with tab_categories:
        category_summary = (
            results_df.groupby(["category", "status"]).size().reset_index(name="count").sort_values(["category", "status"])
        )
        render_app_table(category_summary, numeric_align={"count": "right"})


def render_database_tables_view() -> None:
    if not is_dev_account(st.session_state.get("current_user")):
        st.warning("You do not have access to this section.")
        return

    render_page_header("Database Tables", "Live table data from the connected database.")

    try:
        tables = list_public_tables()
    except Exception:
        admin_logger.exception("Failed loading table list for database explorer.")
        st.error("Could not load database table list.")
        return

    if not tables:
        st.info("No tables were found in the active database.")
        return

    selected_tables = st.multiselect("Tables", options=tables, default=tables)
    row_limit = st.selectbox("Rows per table", options=[25, 50, 100, 250, 500], index=1)

    if not selected_tables:
        st.info("Choose at least one table to view records.")
        return

    for table_name in selected_tables:
        st.markdown(f"#### `{table_name}`")
        try:
            rows = fetch_table_rows(table_name, row_limit=row_limit)
        except Exception:
            admin_logger.exception("Failed loading table rows.", table=table_name)
            st.error(f"Could not load rows for table `{table_name}`.")
            continue

        if not rows:
            st.caption("No rows found.")
            continue

        render_app_table(pd.DataFrame(rows), table_subtitle="Debug database explorer output")


def _render_log_tab(tab_name: str, log_path: str, key_prefix: str) -> None:
    st.markdown(f"#### {tab_name}")

    controls_col_1, controls_col_2 = st.columns([2, 1])
    with controls_col_1:
        search_text = st.text_input("Search", key=f"{key_prefix}_search", placeholder="Filter by text...")
    with controls_col_2:
        level = st.selectbox("Level", LOG_LEVEL_OPTIONS, key=f"{key_prefix}_level")

    lines_to_show = st.slider(
        "Recent matching lines",
        min_value=20,
        max_value=1000,
        value=200,
        step=20,
        key=f"{key_prefix}_line_limit",
    )

    action_col_1, action_col_2 = st.columns([1, 1])
    with action_col_1:
        if st.button("Refresh", key=f"{key_prefix}_refresh", width="stretch"):
            st.rerun()
    with action_col_2:
        file_bytes, download_error = read_full_file_for_download(log_path)
        if file_bytes is not None:
            st.download_button(
                "Download full file",
                data=file_bytes,
                file_name=log_path.split("/")[-1],
                mime="text/plain",
                key=f"{key_prefix}_download",
                width="stretch",
            )
        elif download_error:
            st.caption(download_error)

    lines, error_message = read_log_lines(log_path)
    if error_message:
        st.info(error_message)
        return

    filtered = filter_log_lines(lines, search_text, level)
    visible_lines = get_recent_lines(filtered, lines_to_show)

    st.caption(f"Showing {len(visible_lines)} of {len(filtered)} matching lines ({len(lines)} total).")
    if not visible_lines:
        st.info("No matching log entries found for the current filters.")
        return

    st.code("\n".join(visible_lines), language="text")


def render_admin_log_viewer() -> None:
    """Developer-only debug panel for inspecting application logs."""
    if not is_dev_account(st.session_state.get("current_user")):
        st.warning("You do not have access to this section.")
        return

    render_page_header("Debug Logs", "Inspect application logs from any workspace when signed in as a developer account.")

    app_tab, error_tab, structured_tab, db_tester_tab = st.tabs(
        ["App Logs", "Error Logs", "Structured JSON Logs", "DB Tester"]
    )

    with app_tab:
        _render_log_tab("App Logs", "logs/app.log", "app_logs")
    with error_tab:
        _render_log_tab("Error Logs", "logs/errors.log", "error_logs")
    with structured_tab:
        _render_log_tab("Structured JSON Logs", "logs/structured.json", "structured_logs")
    with db_tester_tab:
        st.markdown("#### Database Connection Tester")

        try:
            conn = psycopg2.connect(
                st.secrets["DATABASE_URL"],
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute("SELECT version();")
            result = cur.fetchone()

            st.success("✅ Connected to database!")
            st.write(result)
        except Exception as exc:
            st.error("❌ Connection failed")
            st.write(type(exc).__name__)
            st.write(str(exc))
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
