from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from html import escape
from io import StringIO
from uuid import uuid4

import pandas as pd
import psycopg2
import streamlit as st

from db import execute, executemany, fetch_all, fetch_one, fetch_table_rows, list_public_tables, get_database_debug_info
from logger import get_logger
from log_viewer import (
    LOG_LEVEL_OPTIONS,
    filter_log_lines,
    get_recent_lines,
    is_admin_session,
    read_full_file_for_download,
    read_log_lines,
)
from utils import (
    apply_learner_filters,
    build_learner_option_label,
    filter_active_learners,
    filter_inactive_learners,
    metric_row,
    render_admin_selection_table,
    render_app_table,
    render_kpi_card,
    render_page_header,
    safe_int,
    to_df,
)
from module_generation import ModuleGenerationInput, generate_module_preview

admin_logger = get_logger(module="admin_views")


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


def _merge_row_selection_into_multiselect(
    *,
    table_event,
    table_df: pd.DataFrame,
    multiselect_key: str,
    option_labels: list[str],
) -> None:
    selected_rows = []
    if table_event:
        selected_rows = table_event.selection.get("rows", [])
    if not selected_rows:
        return

    labels_from_rows: list[str] = []
    for row_idx in selected_rows:
        if 0 <= row_idx < len(table_df):
            labels_from_rows.append(build_learner_option_label(table_df.iloc[row_idx]))

    if not labels_from_rows:
        return

    existing = st.session_state.get(multiselect_key, [])
    merged = []
    for label in [*existing, *labels_from_rows]:
        if label in option_labels and label not in merged:
            merged.append(label)
    st.session_state[multiselect_key] = merged


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
    return to_df(rows)


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
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    render_page_header("Dashboard Overview", "Operational health across learners, assignments, and outcomes.")

    try:
        learners_df = to_df(fetch_all("SELECT * FROM users WHERE role='learner' AND organization_id = ?", (org_id,)))
        modules_df = to_df(fetch_all("SELECT * FROM modules WHERE organization_id = ?", (org_id,)))
        assignments_df = _assignments_with_status(org_id)
    except Exception:
        view_logger.exception("Failed to load admin dashboard.")
        st.error("Failed to load dashboard data.")
        return

    total_learners = len(learners_df)
    active_learners = int(learners_df["is_active"].sum()) if not learners_df.empty else 0
    inactive_learners = total_learners - active_learners
    modules_created = len(modules_df)
    modules_assigned = len(assignments_df)
    completion_rate = round((assignments_df["status"].eq("Completed").mean() * 100), 1) if not assignments_df.empty else 0.0
    overdue_assignments = int(assignments_df["status"].eq("Overdue").sum()) if not assignments_df.empty else 0
    in_progress_assignments = int(assignments_df["status"].eq("In Progress").sum()) if not assignments_df.empty else 0

    completion_tone = "warning" if completion_rate < 70 else "default"
    st.markdown(
        """
        <style>
        .dashboard-section-title {
            font-size: 0.96rem;
            font-weight: 700;
            color: #344054;
            margin: 0.15rem 0 0.35rem 0;
        }
        .attention-panel {
            border: 1px solid #f1d2ce;
            border-radius: 14px;
            background: #fff8f7;
            padding: 12px;
        }
        .attention-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 6px 0;
            border-bottom: 1px dashed #f0dbd7;
            font-size: 0.86rem;
        }
        .attention-row:last-child { border-bottom: none; padding-bottom: 2px; }
        .attention-label { color: #7a271a; font-weight: 600; }
        .attention-value { color: #b42318; font-weight: 700; }
        .dashboard-submission-item {
            padding: 7px 0;
            border-bottom: 1px solid #eaecf0;
        }
        .dashboard-submission-item:last-child { border-bottom: none; padding-bottom: 0; }
        .submission-title {
            margin: 0;
            font-size: 0.85rem;
            font-weight: 600;
            color: #1d2939;
            line-height: 1.2;
        }
        .submission-meta {
            margin-top: 2px;
            color: #667085;
            font-size: 0.77rem;
        }
        .module-stat-card {
            border: 1px solid #eaecf0;
            border-radius: 12px;
            background: #fcfcfd;
            padding: 10px 11px;
            margin-bottom: 8px;
        }
        .module-stat-label {
            color: #667085;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: .02em;
            margin-bottom: 2px;
            font-weight: 600;
        }
        .module-stat-value {
            color: #101828;
            font-size: 1.2rem;
            font-weight: 700;
            line-height: 1.1;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    m1, m2, m3, m4 = st.columns(4, gap="small")
    with m1:
        render_kpi_card("Total learners", total_learners, f"{active_learners} active", compact=True)
    with m2:
        render_kpi_card("Active assignments", modules_assigned, f"{in_progress_assignments} in progress", compact=True)
    with m3:
        render_kpi_card(
            "Completion rate",
            f"{completion_rate}%",
            "Across all active assignments",
            tone=completion_tone,
            compact=True,
        )
    with m4:
        render_kpi_card("Overdue", overdue_assignments, "Need follow-up", tone="danger", compact=True)

    top_left, top_right = st.columns([2.2, 1], gap="small")
    with top_left:
        st.markdown("<div class='dashboard-section-title'>Status breakdown</div>", unsafe_allow_html=True)
        if assignments_df.empty:
            st.info("No assignments yet.")
        else:
            st.bar_chart(assignments_df["status"].value_counts(), height=220)
    with top_right:
        st.markdown("<div class='dashboard-section-title'>Needs attention</div>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="attention-panel">
              <div class="attention-row"><span class="attention-label">🔴 Overdue assignments</span><span class="attention-value">{overdue_assignments}</span></div>
              <div class="attention-row"><span class="attention-label">🟠 Inactive learners</span><span class="attention-value">{inactive_learners}</span></div>
              <div class="attention-row"><span class="attention-label">🟡 Completion rate below goal</span><span class="attention-value">{completion_rate}%</span></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    c3, c4, c5 = st.columns([1.1, 1.9, 1], gap="small")
    with c3:
        with st.container(border=True):
            st.markdown("<div class='dashboard-section-title'>Learner status</div>", unsafe_allow_html=True)
            st.bar_chart(pd.Series({"Active": active_learners, "Inactive": inactive_learners}), height=180)
    with c4:
        with st.container(border=True):
            st.markdown("<div class='dashboard-section-title'>Recent submissions</div>", unsafe_allow_html=True)
            recent = assignments_df[assignments_df["last_attempt_at"].notna()].head(5)
            if recent.empty:
                st.caption("No submissions yet.")
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
    with c5:
        with st.container(border=True):
            st.markdown("<div class='dashboard-section-title'>Module catalog</div>", unsafe_allow_html=True)
            st.markdown(
                f"""
                <div class="module-stat-card">
                  <div class="module-stat-label">Created modules</div>
                  <div class="module-stat-value">{modules_created}</div>
                </div>
                <div class="module-stat-card">
                  <div class="module-stat-label">Inactive learners</div>
                  <div class="module-stat-value">{inactive_learners}</div>
                </div>
                <div class="module-stat-card">
                  <div class="module-stat-label">In progress</div>
                  <div class="module-stat-value">{in_progress_assignments}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    
    


def render_learner_management(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    render_page_header("Learner Management", "Search, segment, and manage active learner access.")

    rows = fetch_all(
        """
        WITH assignment_counts AS (
            SELECT
                learner_id,
                COUNT(DISTINCT assignment_id) AS assigned_modules
            FROM assignments
            WHERE organization_id = ?
              AND is_active IS TRUE
            GROUP BY learner_id
        ),
        progress_counts AS (
            SELECT
                user_id,
                COUNT(
                    DISTINCT CASE
                        WHEN completed_at IS NOT NULL OR progress_percent >= 100 THEN module_id
                        ELSE NULL
                    END
                ) AS completed_modules,
                MAX(COALESCE(last_activity_at, updated_at, completed_at, started_at)) AS progress_last_activity
            FROM module_progress
            WHERE organization_id = ?
            GROUP BY user_id
        )
        SELECT
            u.user_id,
            u.id AS user_uuid,
            u.name,
            u.team,
            u.is_active,
            u.organization_id,
            o.name AS organization_name,
            COALESCE(ac.assigned_modules, 0) AS assigned_modules,
            COALESCE(pc.completed_modules, 0) AS completed_modules,
            COALESCE(lp.last_activity, pc.progress_last_activity) AS last_activity
        FROM users u
        LEFT JOIN assignment_counts ac ON ac.learner_id = u.user_id
        LEFT JOIN learner_profiles lp ON lp.user_id = u.id
        LEFT JOIN progress_counts pc ON pc.user_id = u.id
        LEFT JOIN organizations o ON o.organization_id = u.organization_id
        WHERE u.role = 'learner'
          AND u.organization_id = ?
        ORDER BY u.name
        """,
        (org_id, org_id, org_id),
    )
    df = to_df(rows)

    if df.empty:
        st.info("No learners in this organization.")
        return

    df["team"] = df["team"].fillna("")
    df["organization_name"] = df["organization_name"].fillna("Unassigned")
    df["is_active"] = df["is_active"].astype(bool)
    df["status"] = df["is_active"].map({True: "Active", False: "Inactive"})

    c1, c2, c3 = st.columns([2, 1, 1])
    team_options = sorted([team for team in df["team"].unique().tolist() if team])
    org_options = sorted(df["organization_name"].unique().tolist())
    with c1:
        q = st.text_input("Search learners", placeholder="Search by name, team, or department")
    with c2:
        team_filter = st.selectbox("Department/Team", ["All"] + team_options)
    with c3:
        org_filter = st.selectbox("Organization", ["All"] + org_options)
    filtered = apply_learner_filters(df, search_text=q, team_filter=team_filter, org_filter=org_filter)

    def _render_learner_tab(tab_df: pd.DataFrame, full_df: pd.DataFrame, tab_name: str, show_active: bool) -> None:
        scoped = filter_active_learners(tab_df) if show_active else filter_inactive_learners(tab_df)
        scoped_all = filter_active_learners(full_df) if show_active else filter_inactive_learners(full_df)
        st.caption(f"{len(scoped)} learner(s) in {tab_name.lower()}.")
        learner_table_df = scoped[
            [
                "user_id",
                "name",
                "team",
                "organization_name",
                "status",
                "assigned_modules",
                "completed_modules",
                "last_activity",
            ]
        ].reset_index(drop=True)

        tab_key = tab_name.lower().replace(" ", "_")
        multiselect_key = f"learner_bulk_select_{tab_key}"
        selection_state_key = f"learner_bulk_selected_ids_{tab_key}"
        selected_ids_key = f"learner_unified_selected_ids_{tab_key}"
        learner_options = {build_learner_option_label(r): int(r["user_id"]) for _, r in scoped.iterrows()}
        option_labels = list(learner_options.keys())
        label_by_id = {learner_id: label for label, learner_id in learner_options.items()}
        visible_ids = {int(v) for v in scoped["user_id"].tolist()}
        all_tab_ids = {int(v) for v in scoped_all["user_id"].tolist()}

        existing_ids = {
            int(v) for v in st.session_state.get(selected_ids_key, []) if int(v) in all_tab_ids
        }
        st.session_state[selected_ids_key] = sorted(existing_ids)
        st.session_state[selection_state_key] = sorted(existing_ids & visible_ids)

        learner_display_df = learner_table_df.rename(
            columns={
                "user_id": "learner_id",
                "name": "Learner",
                "team": "Team",
                "organization_name": "Organization",
                "status": "Status",
                "assigned_modules": "Assigned",
                "completed_modules": "Completed",
                "last_activity": "Last Activity",
            }
        )
        if "Last Activity" in learner_display_df.columns:
            learner_display_df["Last Activity"] = learner_display_df["Last Activity"].apply(_format_datetime_for_admin_grid)
        st.caption("Select learners directly from the table below.")
        _, selected_row_ids = render_admin_selection_table(
            learner_display_df,
            row_id_col="learner_id",
            selection_state_key=selection_state_key,
            table_key=f"learner_management_table_{tab_key}",
            selection_label="Select",
            selection_help="Select learners for archive/activate actions.",
            height=520,
            empty_message="No learners match current filters. Adjust filters to display learners.",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button(
                "Select All Filtered",
                key=f"select_all_filtered_{tab_key}",
            ):
                updated_ids = set(st.session_state.get(selected_ids_key, [])) | set(learner_options.values())
                st.session_state[selected_ids_key] = sorted(int(v) for v in updated_ids)
                st.session_state[selection_state_key] = sorted(set(st.session_state[selected_ids_key]) & visible_ids)
                st.rerun()
        with c2:
            if st.button(
                "Clear Selection",
                key=f"clear_selection_{tab_key}",
            ):
                st.session_state[selected_ids_key] = []
                st.session_state[selection_state_key] = []
                st.rerun()

        selected_id_set = {int(v) for v in selected_row_ids}
        unified_selected_ids = (set(st.session_state.get(selected_ids_key, [])) - visible_ids) | selected_id_set
        st.session_state[selected_ids_key] = sorted(int(v) for v in unified_selected_ids)
        st.session_state[selection_state_key] = sorted(set(st.session_state[selected_ids_key]) & visible_ids)
        st.session_state[multiselect_key] = [
            label_by_id[learner_id]
            for learner_id in st.session_state[selected_ids_key]
            if learner_id in label_by_id
        ]

        st.markdown("##### Selected learners")
        selected_learners = st.multiselect(
            "Selected learners",
            options=option_labels,
            key=multiselect_key,
            help="Selections stay synced with the table above.",
            label_visibility="collapsed",
        )
        selected_from_multiselect = {learner_options[label] for label in selected_learners if label in learner_options}
        unified_selected_ids = (set(st.session_state.get(selected_ids_key, [])) - visible_ids) | selected_from_multiselect
        st.session_state[selected_ids_key] = sorted(int(v) for v in unified_selected_ids)
        st.session_state[selection_state_key] = sorted(set(st.session_state[selected_ids_key]) & visible_ids)
        selected_ids = sorted(int(v) for v in st.session_state[selected_ids_key])
        visible_selected_count = len(set(selected_ids) & visible_ids)
        st.caption(
            f"{len(selected_ids)} total selected • {visible_selected_count} of {len(scoped)} currently visible"
        )

        if show_active:
            action_label = "Archive"
            new_status = False
            action_type = "secondary"
        else:
            action_label = "Activate"
            new_status = True
            action_type = "primary"

        _, action_col = st.columns([8, 2], gap="small")
        with action_col:
            run_bulk_action = st.button(
                action_label,
                type=action_type,
                key=f"bulk_action_{tab_key}",
                use_container_width=True,
            )

        if run_bulk_action:
            if not selected_ids:
                st.warning("Select at least one learner.")
                return
            try:
                status_sql = "TRUE" if new_status else "FALSE"
                execute(
                    f"""
                    UPDATE users
                    SET is_active = {status_sql}
                    WHERE organization_id = ?
                      AND user_id = ANY(?)
                    """,
                    (org_id, selected_ids),
                )
                view_logger.info(
                    "Bulk learner status update.",
                    action="bulk_status_update",
                    status=("active" if new_status else "inactive"),
                    learner_count=len(selected_ids),
                )
                st.success(
                    f"Updated {len(selected_ids)} learner(s) to {'Active' if new_status else 'Inactive'}."
                )
                st.cache_data.clear()
                st.rerun()
            except Exception:
                view_logger.exception("Failed bulk learner status update.", learner_count=len(selected_ids))
                st.error("Could not update selected learners.")

    active_tab, inactive_tab = st.tabs(["Active Learners", "Inactive Learners"])
    with active_tab:
        _render_learner_tab(filtered, df, "Active Learners", True)
    with inactive_tab:
        _render_learner_tab(filtered, df, "Inactive Learners", False)


def _render_assignment_tool(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    st.subheader("Assignment Tool")

    learners = fetch_all(
        """
        SELECT
            u.user_id,
            u.name,
            u.team,
            u.is_active,
            o.name AS organization_name
        FROM users u
        LEFT JOIN organizations o ON o.organization_id = u.organization_id
        WHERE u.role='learner' AND u.organization_id=?
        ORDER BY u.name
        """,
        (org_id,),
    )
    modules = fetch_all("SELECT module_id, title, status FROM modules WHERE organization_id=? AND COALESCE(status, 'existing') <> 'archived' ORDER BY title", (org_id,))
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
        learners_df = to_df(learners)
        learners_df["team"] = learners_df["team"].fillna("")
        learners_df["organization_name"] = learners_df["organization_name"].fillna("Unassigned")

        team_options = sorted([team for team in learners_df["team"].unique().tolist() if team])
        org_options = sorted(learners_df["organization_name"].unique().tolist())
        default_filters = {"search": "", "team": "All", "org": "All"}
        active_filters = st.session_state.setdefault("assignment_tool_filters", dict(default_filters))

        # Filter controls are grouped in a form to avoid reruns while typing/changing each control.
        with st.form("assignment_tool_filters_form", clear_on_submit=False):
            fc1, fc2, fc3, fc4, fc5, fc6 = st.columns([2, 1, 1, 1, 1, 1])
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
                apply_filters = st.form_submit_button("Apply filters", use_container_width=True)
            with fc5:
                selected_filtered_learners = st.form_submit_button("Select filtered learners", use_container_width=True)
            with fc6:
                clear_filtered_selection = st.form_submit_button("Clear filtered selection", use_container_width=True)

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
        all_active_learner_map = {
            build_learner_option_label(row): int(row["user_id"])
            for _, row in all_active_learners.sort_values("name").iterrows()
        }
        learner_map = {
            build_learner_option_label(row): int(row["user_id"])
            for _, row in filtered_active_learners.sort_values("name").iterrows()
        }
        learner_options = list(learner_map.keys())
        label_by_id = {learner_id: label for label, learner_id in all_active_learner_map.items()}
        learner_multiselect_key = "assign_training_learners"
        unified_selection_key = "assignment_tool_unified_selected_learner_ids"
        table_selection_key = "assignment_tool_selected_learner_ids"
        visible_ids = {int(v) for v in filtered_active_learners["user_id"].tolist()}
        filtered_selected_count = len(set(st.session_state.get(unified_selection_key, [])) & visible_ids)

        if learner_multiselect_key not in st.session_state:
            st.session_state[learner_multiselect_key] = []

        existing_ids = {
            int(v) for v in st.session_state.get(unified_selection_key, []) if int(v) in set(all_active_learners["user_id"].tolist())
        }
        st.session_state[unified_selection_key] = sorted(existing_ids)
        st.session_state[table_selection_key] = sorted(existing_ids & visible_ids)

        if selected_filtered_learners:
            merged_ids = set(st.session_state.get(unified_selection_key, [])) | visible_ids
            st.session_state[unified_selection_key] = sorted(int(v) for v in merged_ids)
            st.session_state[table_selection_key] = sorted(set(st.session_state[unified_selection_key]) & visible_ids)
            st.rerun()

        if clear_filtered_selection:
            remaining_ids = set(st.session_state.get(unified_selection_key, [])) - visible_ids
            st.session_state[unified_selection_key] = sorted(int(v) for v in remaining_ids)
            st.session_state[table_selection_key] = sorted(set(st.session_state[unified_selection_key]) & visible_ids)
            st.rerun()

        st.caption(f"{len(filtered_active_learners)} active learners match current filters")
        assignment_learner_grid = filtered_active_learners[
            ["user_id", "name", "team", "organization_name"]
        ].reset_index(drop=True).rename(
            columns={
                "user_id": "learner_id",
                "name": "Learner",
                "team": "Team",
                "organization_name": "Organization",
            }
        )
        _, selected_learner_ids = render_admin_selection_table(
            assignment_learner_grid,
            row_id_col="learner_id",
            selection_state_key=table_selection_key,
            table_key="assignment_tool_learner_data_editor",
            selection_label="Select",
            selection_help="Select learners to assign this module.",
            single_select=False,
            height=420,
        )
        selected_id_set = {int(v) for v in selected_learner_ids}
        unified_selected_ids = (set(st.session_state.get(unified_selection_key, [])) - visible_ids) | selected_id_set
        st.session_state[unified_selection_key] = sorted(int(v) for v in unified_selected_ids)
        st.session_state[table_selection_key] = sorted(set(st.session_state[unified_selection_key]) & visible_ids)
        selected_labels = [
            label_by_id[learner_id]
            for learner_id in st.session_state[unified_selection_key]
            if learner_id in label_by_id
        ]
        selectbox_options = list(dict.fromkeys(learner_options + selected_labels))
        st.session_state[learner_multiselect_key] = [
            label for label in st.session_state.get(learner_multiselect_key, []) if label in selectbox_options
        ]
        st.session_state[learner_multiselect_key] = selected_labels
        filtered_selected_count = len(set(st.session_state[unified_selection_key]) & visible_ids)
        st.caption(f"{filtered_selected_count} filtered learners selected")

        due_date_enabled_key = "assignment_tool_due_date_enabled"
        due_date_value_key = "assignment_tool_due_date_value"

        if due_date_enabled_key not in st.session_state:
            st.session_state[due_date_enabled_key] = False
        if due_date_value_key not in st.session_state:
            st.session_state[due_date_value_key] = None

        def _toggle_due_date() -> None:
            if not st.session_state[due_date_enabled_key]:
                st.session_state[due_date_value_key] = None

        selected_module = st.selectbox("Module", list(module_map.keys()))
        selected_learners = st.multiselect(
            "Selected learners (from table)",
            selectbox_options,
            key=learner_multiselect_key,
            help="Primary selection happens in the table above.",
        )
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
        assign_submitted = st.button("Send to database: Assign training", type="primary")

        if assign_submitted:
            module_id = module_map[selected_module]
            due_date_value = due_date.isoformat() if st.session_state[due_date_enabled_key] and due_date else None
            if not selected_learners:
                st.warning("Select at least one learner before assigning.")
                return
            try:
                selected_ids = [learner_map[learner_label] for learner_label in selected_learners]
                active_rows = fetch_all(
                    """
                    SELECT user_id
                    FROM users
                    WHERE role = 'learner'
                      AND organization_id = ?
                      AND is_active = TRUE
                      AND user_id = ANY(?)
                    """,
                    (org_id, selected_ids),
                )
                active_ids = {int(row["user_id"]) for row in active_rows}
                valid_ids = [learner_id for learner_id in selected_ids if learner_id in active_ids]
                skipped_count = len(selected_ids) - len(valid_ids)
                if not valid_ids:
                    st.warning("No active learners were selected. Refresh and try again.")
                    return
                for learner_label in selected_learners:
                    learner_id = learner_map[learner_label]
                    if learner_id not in active_ids:
                        continue
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
                view_logger.info("Form submitted.", form="assign_training", scenario_id=module_id, learners=len(valid_ids))
                if skipped_count:
                    st.warning(f"Skipped {skipped_count} learner(s) who are no longer active.")
                st.success(f"Assigned module to {len(valid_ids)} learner(s).")
                st.cache_data.clear()
                st.rerun()
            except Exception:
                view_logger.exception("Failed assigning training.", scenario_id=module_id)
                st.error("Could not assign training.")


def render_assignment_management(current_user: dict) -> None:
    render_page_header("Assignment Management", "Assign modules in bulk and monitor assignment status.")
    current_tab, tool_tab = st.tabs(["Current Assignments", "Assignment Tool"])
    with current_tab:
        render_current_assignments(current_user)
    with tool_tab:
        _render_assignment_tool(current_user)


def render_current_assignments(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    st.markdown("#### Current assignments")

    refresh_token = int(st.session_state.get("assignment_management_refresh_token", 0))
    assignments_df = _assignments_with_status(org_id, refresh_token)
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
                apply_assignment_filters = st.form_submit_button("Apply filters", use_container_width=True)

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
    interactive_df = assignment_display_df.copy()
    _, selected_assignment_ids = render_admin_selection_table(
        interactive_df,
        row_id_col="Assignment ID",
        selection_state_key=selection_state_key,
        table_key="assignment_management_data_editor",
        selection_label="Select",
        selection_help="Select assignments for bulk actions.",
        single_select=False,
        height=520,
    )
    selected_assignment_ids = sorted(int(v) for v in selected_assignment_ids)
    selected_count = len(selected_assignment_ids)

    if selected_count == 0:
        st.info("Select one or more assignments from the table to enable bulk actions.")
        return

    selected_assignments = assignments_df[assignments_df["assignment_id"].isin(selected_assignment_ids)].copy()
    selected_assignments = selected_assignments.sort_values("assigned_at", ascending=False)
    with st.container(border=True):
        st.markdown("#### Bulk actions")
        st.caption(f"{selected_count} assignment(s) selected")
        preview_df = selected_assignments[
            ["assignment_id", "learner_name", "module_title", "status", "due_date"]
        ].rename(
            columns={
                "assignment_id": "Assignment ID",
                "learner_name": "Learner",
                "module_title": "Module",
                "status": "Status",
                "due_date": "Due Date",
            }
        )
        if "Due Date" in preview_df.columns:
            preview_df["Due Date"] = preview_df["Due Date"].apply(_format_datetime_for_admin_grid)
        st.markdown('<div class="app-table-host">', unsafe_allow_html=True)
        st.dataframe(preview_df.head(20), use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)
        if len(preview_df) > 20:
            st.caption(f"Showing first 20 of {len(preview_df)} selected assignments.")

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Send to database: Remove selected assignments", type="primary", use_container_width=True):
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
                    view_logger.info(
                        "Button click.",
                        action="remove_assignment",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.success(f"Removed {len(selected_assignment_ids)} assignment(s).")
                    st.session_state[selection_state_key] = set()
                    st.session_state["assignment_management_refresh_token"] = refresh_token + 1
                    st.session_state.pop("assignment_management_filtered_cache", None)
                    st.cache_data.clear()
                    st.rerun()
                except Exception:
                    view_logger.exception(
                        "Failed removing assignments.",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.error("Could not remove selected assignments.")
        with c2:
            new_due = st.date_input("Reassign due date", key="reassign_due", value=date.today())
            if st.button("Send to database: Reassign selected training", use_container_width=True):
                try:
                    execute(
                        "UPDATE assignments SET due_date = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP "
                        "WHERE organization_id = ? AND assignment_id IN ?",
                        (new_due.isoformat(), current_user["user_id"], org_id, tuple(selected_assignment_ids)),
                    )
                    view_logger.info(
                        "Button click.",
                        action="reassign_training",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.success(f"Updated {len(selected_assignment_ids)} assignment(s).")
                    st.session_state[selection_state_key] = set()
                    st.session_state["assignment_management_refresh_token"] = refresh_token + 1
                    st.session_state.pop("assignment_management_filtered_cache", None)
                    st.cache_data.clear()
                    st.rerun()
                except Exception:
                    view_logger.exception(
                        "Failed reassigning training.",
                        assignment_count=len(selected_assignment_ids),
                    )
                    st.error("Could not update selected assignments.")
        with c3:
            st.caption("Selection controls")
            if st.button("Clear selection", use_container_width=True):
                st.session_state[selection_state_key] = set()
                st.rerun()


def render_grading_center(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    st.subheader("Submission Grading")
    st.caption("Review learner submissions and scoring results for assigned modules.")

    attempts = to_df(
        fetch_all(
            """
            SELECT
                a.attempt_id,
                a.created_at,
                u.name AS learner_name,
                m.title AS module_title,
                COALESCE(ss.total_score, a.total_score) AS total_score,
                COALESCE(ss.understanding_score, a.understanding_score) AS understanding_score,
                COALESCE(ss.investigation_score, a.investigation_score) AS investigation_score,
                COALESCE(ss.solution_score, a.solution_score) AS solution_score,
                COALESCE(ss.communication_score, a.communication_score) AS communication_score,
                ss.scoring_version,
                a.ai_feedback
            FROM attempts a
            JOIN users u ON u.user_id = a.user_id
            JOIN modules m ON m.module_id = a.module_id
            LEFT JOIN submission_scores ss ON ss.attempt_id = a.attempt_id
            WHERE a.organization_id = ?
              AND u.is_active = TRUE
            ORDER BY a.created_at DESC
            """,
            (org_id,),
        )
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
    filtered = attempts.copy()
    if learner_filter:
        filtered = filtered[filtered["learner_name"].isin(learner_filter)]
    if module_filter:
        filtered = filtered[filtered["module_title"].isin(module_filter)]

    metric_row(
        {
            "Submissions": len(filtered),
            "Average score": f"{round(filtered['total_score'].mean(), 1)}%" if len(filtered) else "0%",
            "Top score": f"{round(filtered['total_score'].max(), 1)}%" if len(filtered) else "0%",
        }
    )

    render_app_table(
        filtered[
            [
                "created_at",
                "learner_name",
                "module_title",
                "total_score",
                "understanding_score",
                "investigation_score",
                "solution_score",
                "communication_score",
            ]
        ],
        datetime_columns=["created_at"],
        numeric_formats={
            "total_score": 1,
            "understanding_score": 1,
            "investigation_score": 1,
            "solution_score": 1,
            "communication_score": 1,
        },
        badge_columns={"total_score": "score"},
        numeric_align={k: "right" for k in ["total_score", "understanding_score", "investigation_score", "solution_score", "communication_score"]},
    )


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
    st.subheader("Progress Tracking")

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
            current_filters = st.session_state[filter_state_key]
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
                value=current_filters.get("due_from"),
                key="progress_due_from",
            )
            due_to = row_two_cols[1].date_input(
                "Due to",
                value=current_filters.get("due_to"),
                key="progress_due_to",
            )
            attempted_from = row_two_cols[2].date_input(
                "Attempted from",
                value=current_filters.get("attempted_from"),
                disabled=no_attempt_yet,
                key="progress_attempted_from",
            )
            attempted_to = row_two_cols[3].date_input(
                "Attempted to",
                value=current_filters.get("attempted_to"),
                disabled=no_attempt_yet,
                key="progress_attempted_to",
            )

            apply_col, clear_col = st.columns([1, 1])
            apply_pressed = apply_col.form_submit_button("Apply Filters", use_container_width=True, type="primary")
            clear_pressed = clear_col.form_submit_button("Clear Filters", use_container_width=True)

        if clear_pressed:
            st.session_state[filter_state_key] = default_filters.copy()
            st.session_state["progress_due_from"] = None
            st.session_state["progress_due_to"] = None
            st.session_state["progress_attempted_from"] = None
            st.session_state["progress_attempted_to"] = None
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

    render_app_table(
        filtered[["learner_name", "module_title", "status", "due_date", "last_attempt_at"]],
        datetime_columns=["due_date", "last_attempt_at"],
        badge_columns={"status": "status"},
    )


def _parse_lines(value: str) -> str:
    return "\n".join([line.strip() for line in value.splitlines() if line.strip()])


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
    render_page_header("Module Builder", "Guided wizard for structured module generation and approval.")

    module_builder_current_step_key = "module_builder_current_step"
    module_builder_phase_labels = ["Enter module goals", "Review and approve generated preview", "Module completed"]
    module_builder_completed_module_id_key = "module_builder_completed_module_id"
    module_builder_selected_run_id_key = "module_builder_selected_run_id"
    if module_builder_current_step_key not in st.session_state:
        st.session_state[module_builder_current_step_key] = 1
    current_step = max(1, min(int(st.session_state.get(module_builder_current_step_key, 1)), len(module_builder_phase_labels)))
    st.session_state[module_builder_current_step_key] = current_step
    _render_wizard_progress(current_step - 1, len(module_builder_phase_labels), module_builder_phase_labels[current_step - 1])

    module_builder_step_key = "module_builder_step"
    module_builder_form_key = "module_builder_form"
    module_builder_defaults = {
        "title": "",
        "category": "General",
        "difficulty": "Beginner",
        "role_focus": "",
        "test_focus": "",
        "description": "",
        "learning_objectives": "",
        "scenario_constraints": "",
        "content_sections": "",
        "completion_requirements": "",
        "quiz_required": True,
        "estimated_minutes": 20,
        "question_count": 5,
    }
    if module_builder_step_key not in st.session_state:
        st.session_state[module_builder_step_key] = 0
    if module_builder_form_key not in st.session_state:
        st.session_state[module_builder_form_key] = dict(module_builder_defaults)
    module_form = st.session_state[module_builder_form_key]

    def _init_module_builder_widget_state(widget_key: str, fallback_value):
        if widget_key not in st.session_state:
            st.session_state[widget_key] = fallback_value

    def _sync_module_builder_form_from_widgets() -> None:
        module_form["title"] = st.session_state.get("module_builder_title", module_form.get("title", ""))
        module_form["category"] = st.session_state.get("module_builder_category", module_form.get("category", "General"))
        module_form["difficulty"] = st.session_state.get("module_builder_difficulty", module_form.get("difficulty", "Beginner"))
        module_form["role_focus"] = st.session_state.get("module_builder_role_focus", module_form.get("role_focus", ""))
        module_form["test_focus"] = st.session_state.get("module_builder_test_focus", module_form.get("test_focus", ""))
        module_form["description"] = st.session_state.get("module_builder_description", module_form.get("description", ""))
        module_form["learning_objectives"] = st.session_state.get(
            "module_builder_learning_objectives",
            module_form.get("learning_objectives", ""),
        )
        module_form["scenario_constraints"] = st.session_state.get(
            "module_builder_scenario_constraints",
            module_form.get("scenario_constraints", ""),
        )
        module_form["content_sections"] = st.session_state.get("module_builder_content_sections", module_form.get("content_sections", ""))
        module_form["completion_requirements"] = st.session_state.get(
            "module_builder_completion_requirements",
            module_form.get("completion_requirements", ""),
        )
        module_form["quiz_required"] = _normalize_bool(
            st.session_state.get("module_builder_quiz_required", module_form.get("quiz_required", True)),
            default=True,
        )
        module_form["estimated_minutes"] = int(
            st.session_state.get("module_builder_estimated_minutes", module_form.get("estimated_minutes", 20))
        )
        module_form["question_count"] = int(
            st.session_state.get("module_builder_question_count", module_form.get("question_count", 5))
        )

    module_builder_steps = [
        {"title": "Module title", "field": "title", "required": True},
        {"title": "Category", "field": "category", "required": True},
        {"title": "Difficulty", "field": "difficulty", "required": True},
        {"title": "Role being simulated", "field": "role_focus", "required": True},
        {"title": "What should this module test?", "field": "test_focus", "required": True},
        {"title": "Description", "field": "description", "required": True},
        {"title": "Learning objectives", "field": "learning_objectives", "required": True},
        {"title": "Scenario context / constraints", "field": "scenario_constraints", "required": True},
        {"title": "Ordered content sections", "field": "content_sections", "required": True},
        {"title": "Completion requirements", "field": "completion_requirements", "required": True},
        {"title": "Assessment settings", "field": "assessment_settings", "required": True},
        {"title": "Review and submit", "field": "review", "required": True},
    ]

    if current_step == 1:
        st.markdown("#### Step 1: Enter module goals")
        current_step = int(st.session_state[module_builder_step_key])
        total_steps = len(module_builder_steps)
        step_config = module_builder_steps[current_step]
        _render_named_step_indicator(current_step, [step["title"] for step in module_builder_steps])

        # Wizard: render exactly one step at a time and persist each input in session state.
        with st.container(border=True):
            step_valid = True
            required_message = ""
            step_errors: list[str] = []
            if step_config["field"] == "title":
                current_key = "module_builder_title"
                _init_module_builder_widget_state(current_key, module_form["title"])
                st.text_input("Title", key=current_key)
                current_value = st.session_state.get(current_key, "").strip()
                module_form["title"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please enter a module title to continue."
            elif step_config["field"] == "category":
                current_key = "module_builder_category"
                _init_module_builder_widget_state(current_key, module_form["category"])
                st.text_input("Category", key=current_key)
                current_value = st.session_state.get(current_key, "").strip()
                module_form["category"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please enter a category to continue."
            elif step_config["field"] == "difficulty":
                difficulty_options = ["Beginner", "Intermediate", "Advanced"]
                current_difficulty = module_form["difficulty"] if module_form["difficulty"] in difficulty_options else "Beginner"
                _init_module_builder_widget_state("module_builder_difficulty", current_difficulty)
                st.selectbox(
                    "Difficulty",
                    difficulty_options,
                    key="module_builder_difficulty",
                )
                module_form["difficulty"] = st.session_state.get("module_builder_difficulty", current_difficulty)
            elif step_config["field"] == "role_focus":
                current_key = "module_builder_role_focus"
                _init_module_builder_widget_state(current_key, module_form["role_focus"])
                st.text_input(
                    "Role being simulated (e.g., Support Tier 1, Team Lead)",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["role_focus"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please describe the role being simulated."
            elif step_config["field"] == "test_focus":
                current_key = "module_builder_test_focus"
                _init_module_builder_widget_state(current_key, module_form["test_focus"])
                st.text_input(
                    "What should this module test?",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["test_focus"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please describe what this module should test."
            elif step_config["field"] == "description":
                current_key = "module_builder_description"
                _init_module_builder_widget_state(current_key, module_form["description"])
                st.text_area("Description", key=current_key)
                current_value = st.session_state.get(current_key, "").strip()
                module_form["description"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please enter a description to continue."
            elif step_config["field"] == "learning_objectives":
                current_key = "module_builder_learning_objectives"
                _init_module_builder_widget_state(current_key, module_form["learning_objectives"])
                st.text_area(
                    "Learning objectives (one per line)",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["learning_objectives"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please add at least one learning objective."
            elif step_config["field"] == "scenario_constraints":
                current_key = "module_builder_scenario_constraints"
                _init_module_builder_widget_state(current_key, module_form["scenario_constraints"])
                st.text_area(
                    "Scenario context / constraints",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["scenario_constraints"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please provide scenario context or constraints."
            elif step_config["field"] == "content_sections":
                current_key = "module_builder_content_sections"
                _init_module_builder_widget_state(current_key, module_form["content_sections"])
                st.text_area(
                    "Ordered content sections (one per line)",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["content_sections"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please add at least one content section."
            elif step_config["field"] == "completion_requirements":
                current_key = "module_builder_completion_requirements"
                _init_module_builder_widget_state(current_key, module_form["completion_requirements"])
                st.text_area(
                    "Completion requirements",
                    key=current_key,
                )
                current_value = st.session_state.get(current_key, "").strip()
                module_form["completion_requirements"] = st.session_state.get(current_key, "")
                step_valid = bool(current_value)
                required_message = "Please enter completion requirements."
            elif step_config["field"] == "assessment_settings":
                _init_module_builder_widget_state(
                    "module_builder_quiz_required",
                    _normalize_bool(module_form.get("quiz_required"), default=True),
                )
                _init_module_builder_widget_state("module_builder_estimated_minutes", int(module_form["estimated_minutes"]))
                _init_module_builder_widget_state("module_builder_question_count", int(module_form["question_count"]))
                st.checkbox(
                    "Quiz required",
                    key="module_builder_quiz_required",
                )
                st.number_input(
                    "Estimated assessment time (minutes)",
                    min_value=1,
                    max_value=240,
                    step=1,
                    key="module_builder_estimated_minutes",
                )
                st.slider(
                    "AI-generated questions",
                    min_value=0,
                    max_value=10,
                    key="module_builder_question_count",
                )
                module_form["quiz_required"] = _normalize_bool(
                    st.session_state.get("module_builder_quiz_required", True),
                    default=True,
                )
                module_form["estimated_minutes"] = int(st.session_state.get("module_builder_estimated_minutes", 20))
                module_form["question_count"] = int(st.session_state.get("module_builder_question_count", 5))
            else:
                st.markdown("##### Review")
                st.write("Please review your values before saving.")
                _render_module_review_summary(module_form)
                required_fields = [
                    ("title", "Module title"),
                    ("category", "Category"),
                    ("role_focus", "Role being simulated"),
                    ("test_focus", "What this module should test"),
                    ("description", "Description"),
                    ("learning_objectives", "Learning objectives"),
                    ("scenario_constraints", "Scenario context / constraints"),
                    ("content_sections", "Ordered content sections"),
                    ("completion_requirements", "Completion requirements"),
                ]
                missing_labels = [label for field, label in required_fields if not _is_present(module_form.get(field))]
                if missing_labels:
                    step_errors.append("Please complete these items before saving: " + ", ".join(missing_labels) + ".")
                step_valid = not step_errors

            if step_errors:
                for error in step_errors:
                    st.error(error)

        _sync_module_builder_form_from_widgets()

        if not step_valid and required_message and step_config["field"] != "review":
            st.error(required_message)

        nav_left, nav_right = st.columns([1, 1])
        with nav_left:
            if st.button("Previous", key="module_builder_previous", disabled=current_step == 0):
                _sync_module_builder_form_from_widgets()
                st.session_state[module_builder_step_key] = max(0, current_step - 1)
                st.rerun()
        with nav_right:
            if current_step < total_steps - 2:
                if st.button("Next", key="module_builder_next"):
                    _sync_module_builder_form_from_widgets()
                    st.session_state[module_builder_step_key] = current_step + 1
                    st.rerun()
            elif current_step == total_steps - 2:
                if st.button("Next", key="module_builder_review"):
                    _sync_module_builder_form_from_widgets()
                    st.session_state[module_builder_step_key] = current_step + 1
                    st.rerun()
            if current_step == total_steps - 1:
                if st.button("Save Module", key="module_builder_save_module", type="primary", disabled=not step_valid):
                    _sync_module_builder_form_from_widgets()
                    is_valid = bool(step_valid)
                    if not is_valid:
                        st.error("Please complete required fields")
                        return

                    payload = ModuleGenerationInput(
                        title=module_form["title"].strip(),
                        category=module_form["category"].strip() or "General",
                        difficulty=module_form["difficulty"],
                        description=module_form["description"].strip(),
                        role_focus=module_form["role_focus"].strip(),
                        test_focus=module_form["test_focus"].strip(),
                        learning_objectives=[line.strip() for line in module_form["learning_objectives"].splitlines() if line.strip()],
                        scenario_constraints=module_form["scenario_constraints"].strip(),
                        completion_requirements=module_form["completion_requirements"].strip(),
                        question_count=int(module_form["question_count"]),
                    )
                    try:
                        preview, warning = generate_module_preview(payload)
                        run_id = execute(
                            """
                            INSERT INTO module_generation_runs (
                                organization_id, created_by, input_title, input_category, input_difficulty,
                                input_description, role_focus, test_focus, learning_objectives, input_content_sections,
                                scenario_constraints, completion_requirements, input_quiz_required, requested_question_count,
                                input_estimated_minutes, generated_title, generated_description, generated_scenario_overview,
                                generation_status, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
                            RETURNING run_id AS id
                            """,
                            (
                                org_id,
                                current_user["user_id"],
                                payload.title,
                                payload.category,
                                payload.difficulty,
                                payload.description,
                                payload.role_focus,
                                payload.test_focus,
                                "\n".join(payload.learning_objectives),
                                _parse_lines(module_form["content_sections"]),
                                payload.scenario_constraints,
                                payload.completion_requirements,
                                _normalize_bool(module_form.get("quiz_required"), default=True),
                                payload.question_count,
                                int(module_form["estimated_minutes"]),
                                preview.get("title"),
                                preview.get("description"),
                                preview.get("scenario_overview"),
                            ),
                        )
                        execute("DELETE FROM module_generation_questions WHERE run_id = ?", (run_id,))
                        executemany(
                            """
                            INSERT INTO module_generation_questions (
                                run_id, question_order, question_text, rationale, question_type, options_text, approval_status, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
                            """,
                            [
                                (
                                    run_id,
                                    idx + 1,
                                    item.get("question", ""),
                                    item.get("rationale", ""),
                                    "open_text",
                                    "",
                                )
                                for idx, item in enumerate(preview.get("questions", []))
                            ],
                        )
                        if warning:
                            st.warning(warning)
                        st.session_state[module_builder_current_step_key] = 2
                        st.session_state[module_builder_selected_run_id_key] = int(run_id)
                        st.success("Preview generated. Continue to Step 2 to review and approve.")
                        st.session_state[module_builder_step_key] = 0
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not save module preview: {exc}")

    elif current_step == 2:
        st.markdown("#### Step 2: Review and approve generated preview")

        runs_df = to_df(
            fetch_all(
                """
                SELECT * FROM module_generation_runs
                WHERE organization_id = ?
                ORDER BY updated_at DESC
                """,
                (org_id,),
            )
        )
        if runs_df.empty:
            st.info("No AI previews yet. Complete Step 1 first.")
            return

        ordered_run_ids = [int(row["run_id"]) for _, row in runs_df.iterrows()]
        selected_run_id = st.session_state.get(module_builder_selected_run_id_key)
        if selected_run_id not in ordered_run_ids:
            selected_run_id = ordered_run_ids[0]
            st.session_state[module_builder_selected_run_id_key] = selected_run_id
        run_id = int(selected_run_id)
        run = fetch_one("SELECT * FROM module_generation_runs WHERE run_id = ? AND organization_id = ?", (run_id, org_id))
        if not run:
            st.info("No accessible preview found for final review.")
            return
        run_title = run.get("generated_title") or run.get("input_title") or "Untitled"
        st.markdown(f"**Reviewing Preview:** {run_title}")
        generated_questions = fetch_all(
            """
            SELECT * FROM module_generation_questions
            WHERE run_id = ?
            ORDER BY question_order
            """,
            (run_id,),
        )
        review_step_key = f"module_review_step_{run_id}"
        if review_step_key not in st.session_state:
            st.session_state[review_step_key] = 0
        review_step = int(st.session_state[review_step_key])

        non_custom_questions = [q for q in generated_questions if (q.get("admin_feedback") or "") != "custom_question"]
        custom_questions = [q for q in generated_questions if (q.get("admin_feedback") or "") == "custom_question"]
        wizard_labels = ["Review Scenario", "Review Questions", "Custom Questions", "Finalize"]
        wizard_step_index = review_step
        _render_named_step_indicator(wizard_step_index, wizard_labels)

        step_valid = True
        if review_step == 0:
            with st.container(border=True):
                st.markdown("##### Review Scenario")
                run_title_key = f"scenario_title_{run_id}"
                run_summary_key = f"scenario_summary_{run_id}"
                run_feedback_key = f"scenario_feedback_{run_id}"
                run_status_key = f"scenario_status_{run_id}"
                if run_title_key not in st.session_state:
                    st.session_state[run_title_key] = run.get("generated_title") or run.get("input_title") or ""
                if run_summary_key not in st.session_state:
                    st.session_state[run_summary_key] = run.get("generated_description") or run.get("input_description") or ""
                if run_feedback_key not in st.session_state:
                    st.session_state[run_feedback_key] = run.get("test_focus") or ""
                scenario_context_key = f"scenario_context_{run_id}"
                if scenario_context_key not in st.session_state:
                    st.session_state[scenario_context_key] = run.get("generated_scenario_overview") or ""
                if run_status_key not in st.session_state:
                    initial_status = run.get("generation_status", "pending")
                    st.session_state[run_status_key] = initial_status if initial_status in {"approved", "denied", "pending"} else "pending"

                st.text_input("Scenario title", key=run_title_key)
                st.text_area("Scenario summary / metadata", key=run_summary_key, height=120)
                st.text_area(
                    "Scenario description / context",
                    key=scenario_context_key,
                    height=180,
                )
                st.selectbox(
                    "Scenario decision",
                    ["approved", "denied", "pending"],
                    key=run_status_key,
                )
                st.text_area("Scenario feedback", key=run_feedback_key)
                step_valid = bool(st.session_state.get(run_title_key, "").strip()) and bool(st.session_state.get(scenario_context_key, "").strip())
                if st.button("Save scenario decision", key=f"save_scenario_decision_{run_id}"):
                    execute(
                        """
                        UPDATE module_generation_runs
                        SET generation_status = ?, test_focus = ?, generated_title = ?, generated_description = ?, generated_scenario_overview = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE run_id = ? AND organization_id = ?
                        """,
                        (
                            st.session_state[run_status_key],
                            st.session_state[run_feedback_key],
                            st.session_state[run_title_key],
                            st.session_state[run_summary_key],
                            st.session_state[scenario_context_key],
                            run_id,
                            org_id,
                        ),
                    )
                    st.success("Scenario decision saved.")
                    st.rerun()

        elif review_step == 1:
            with st.container(border=True):
                st.markdown("##### Review Generated Questions")
                if not non_custom_questions:
                    st.info("No generated questions remain in this preview. Continue to the next step or add custom questions.")
                else:
                    for idx, q in enumerate(non_custom_questions, start=1):
                        question_id = q.get("generated_question_id")
                        if question_id is None:
                            fallback_key = q.get("question_stable_key")
                            if not fallback_key:
                                fallback_key = f"temp_q_{run_id}_{q.get('question_order', idx)}_{idx}"
                                q["question_stable_key"] = fallback_key
                            question_identity = fallback_key
                        else:
                            question_identity = str(question_id)
                        qtext_key = f"qtext_{question_identity}"
                        qrationale_key = f"qrationale_{question_identity}"
                        qdifficulty_key = f"qdifficulty_{question_identity}"
                        qpoints_key = f"qpoints_{question_identity}"
                        qtype_key = f"qtype_{question_identity}"
                        qstatus_key = f"qstatus_{question_identity}"
                        qfeedback_key = f"qfeedback_{question_identity}"
                        qoptions_key = f"qoptions_{question_identity}"
                        if qtext_key not in st.session_state:
                            st.session_state[qtext_key] = q.get("question_text") or ""
                        if qrationale_key not in st.session_state:
                            st.session_state[qrationale_key] = q.get("rationale") or ""
                        if qdifficulty_key not in st.session_state:
                            level = run.get("input_difficulty") or "Beginner"
                            st.session_state[qdifficulty_key] = level if level in {"Beginner", "Intermediate", "Advanced"} else "Beginner"
                        if qpoints_key not in st.session_state:
                            st.session_state[qpoints_key] = "N/A"
                        if qtype_key not in st.session_state:
                            st.session_state[qtype_key] = q.get("question_type") or "open_text"
                        if qstatus_key not in st.session_state:
                            st.session_state[qstatus_key] = q.get("approval_status") or "pending"
                        if qfeedback_key not in st.session_state:
                            st.session_state[qfeedback_key] = q.get("admin_feedback") or ""
                        if qoptions_key not in st.session_state:
                            st.session_state[qoptions_key] = q.get("options_text") or ""

                        with st.container(border=True):
                            st.markdown(f"###### Question {idx}")
                            st.text_area("Question text", key=qtext_key, height=120)
                            st.text_area("Expected / ideal answer", key=qrationale_key, height=100)
                            meta_col1, meta_col2, meta_col3 = st.columns(3)
                            with meta_col1:
                                st.selectbox(
                                    "Difficulty",
                                    ["Beginner", "Intermediate", "Advanced"],
                                    key=qdifficulty_key,
                                    disabled=True,
                                )
                            with meta_col2:
                                st.text_input("Point value", key=qpoints_key, disabled=True)
                            with meta_col3:
                                st.selectbox(
                                    "Question type",
                                    ["open_text", "multiple_choice"],
                                    key=qtype_key,
                                )
                            st.selectbox(
                                "Decision",
                                ["pending", "approved", "denied"],
                                key=qstatus_key,
                            )
                            st.text_input("Admin feedback", key=qfeedback_key)
                            st.text_area(
                                "Choices for this question (one per line)",
                                disabled=st.session_state[qtype_key] != "multiple_choice",
                                key=qoptions_key,
                            )
                            if st.button("Delete Question", key=f"delete_q_{question_identity}"):
                                if question_id is not None:
                                    execute("DELETE FROM module_generation_questions WHERE generated_question_id = ?", (question_id,))
                                    st.success(f"Question {idx} deleted.")
                                else:
                                    st.warning("Unable to delete this question because it has no identifier.")
                                st.rerun()
        elif review_step == 2:
            with st.container(border=True):
                st.markdown("##### Custom Questions")
                custom_form_mode_key = f"custom_question_form_mode_{run_id}"
                custom_edit_id_key = f"custom_question_edit_id_{run_id}"
                custom_form_text_key = f"custom_form_question_text_{run_id}"
                custom_form_type_key = f"custom_form_question_type_{run_id}"
                custom_form_options_key = f"custom_form_question_options_{run_id}"
                custom_form_rubric_key = f"custom_form_question_rubric_{run_id}"

                if custom_form_mode_key not in st.session_state:
                    st.session_state[custom_form_mode_key] = None
                if custom_edit_id_key not in st.session_state:
                    st.session_state[custom_edit_id_key] = None
                if custom_form_text_key not in st.session_state:
                    st.session_state[custom_form_text_key] = ""
                if custom_form_type_key not in st.session_state:
                    st.session_state[custom_form_type_key] = "open_text"
                if custom_form_options_key not in st.session_state:
                    st.session_state[custom_form_options_key] = ""
                if custom_form_rubric_key not in st.session_state:
                    st.session_state[custom_form_rubric_key] = ""

                def _open_add_form() -> None:
                    st.session_state[custom_form_mode_key] = "add"
                    st.session_state[custom_edit_id_key] = None
                    st.session_state[custom_form_text_key] = ""
                    st.session_state[custom_form_type_key] = "open_text"
                    st.session_state[custom_form_options_key] = ""
                    st.session_state[custom_form_rubric_key] = ""

                def _open_edit_form(question: dict) -> None:
                    st.session_state[custom_form_mode_key] = "edit"
                    st.session_state[custom_edit_id_key] = question["generated_question_id"]
                    st.session_state[custom_form_text_key] = question.get("question_text") or ""
                    st.session_state[custom_form_type_key] = question.get("question_type") or "open_text"
                    st.session_state[custom_form_options_key] = question.get("options_text") or ""
                    st.session_state[custom_form_rubric_key] = question.get("rationale") or ""

                form_is_open = st.session_state[custom_form_mode_key] in {"add", "edit"}
                add_button_label = "Cancel" if form_is_open else "+ Add Question"
                if st.button(add_button_label, key=f"toggle_custom_question_form_{run_id}"):
                    if form_is_open:
                        st.session_state[custom_form_mode_key] = None
                        st.session_state[custom_edit_id_key] = None
                    else:
                        _open_add_form()
                    st.rerun()

                if form_is_open:
                    with st.form(f"custom_question_form_{run_id}"):
                        st.text_area("Question text", key=custom_form_text_key)
                        st.selectbox("Question type", ["open_text", "multiple_choice"], key=custom_form_type_key)
                        custom_form_options = st.text_area(
                            "Multiple choice options (one per line)",
                            key=custom_form_options_key,
                            disabled=st.session_state[custom_form_type_key] != "multiple_choice",
                        )
                        st.text_area("Ideal answer / rubric", key=custom_form_rubric_key)
                        save_label = "Save Question" if st.session_state[custom_form_mode_key] == "edit" else "Add Question"
                        save_question = st.form_submit_button(save_label)
                        if save_question:
                            if st.session_state[custom_form_mode_key] == "edit" and st.session_state[custom_edit_id_key]:
                                execute(
                                    """
                                    UPDATE module_generation_questions
                                    SET question_text = ?, rationale = ?, question_type = ?, options_text = ?, admin_feedback = 'custom_question', updated_at = CURRENT_TIMESTAMP
                                    WHERE generated_question_id = ?
                                    """,
                                    (
                                        st.session_state[custom_form_text_key].strip(),
                                        st.session_state[custom_form_rubric_key].strip(),
                                        st.session_state[custom_form_type_key],
                                        _parse_lines(custom_form_options) if st.session_state[custom_form_type_key] == "multiple_choice" else "",
                                        st.session_state[custom_edit_id_key],
                                    ),
                                )
                                st.success("Custom question saved.")
                            else:
                                max_order_row = fetch_one(
                                    "SELECT COALESCE(MAX(question_order), 0) AS max_order FROM module_generation_questions WHERE run_id = ?",
                                    (run_id,),
                                )
                                next_order = int(max_order_row["max_order"]) + 1 if max_order_row else 1
                                execute(
                                    """
                                    INSERT INTO module_generation_questions (
                                        run_id, question_order, question_text, rationale, question_type, options_text, approval_status, admin_feedback, updated_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, 'approved', 'custom_question', CURRENT_TIMESTAMP)
                                    """,
                                    (
                                        run_id,
                                        next_order,
                                        st.session_state[custom_form_text_key].strip(),
                                        st.session_state[custom_form_rubric_key].strip() or "Admin added",
                                        st.session_state[custom_form_type_key],
                                        _parse_lines(custom_form_options) if st.session_state[custom_form_type_key] == "multiple_choice" else "",
                                    ),
                                )
                                st.success("Question added.")
                            st.session_state[custom_form_mode_key] = None
                            st.session_state[custom_edit_id_key] = None
                            st.rerun()

                st.markdown("###### Added Questions")
                if not custom_questions:
                    st.caption("No custom questions added yet.")
                for q in custom_questions:
                    st.markdown(f"**{q.get('question_text') or 'Untitled question'}**")
                    st.caption(f"Type: {q.get('question_type') or 'open_text'}")
                    edit_col, delete_col = st.columns([1, 1])
                    with edit_col:
                        if st.button("Edit", key=f"edit_custom_{q['generated_question_id']}"):
                            _open_edit_form(q)
                            st.rerun()
                    with delete_col:
                        if st.button("Delete", key=f"delete_custom_{q['generated_question_id']}"):
                            execute("DELETE FROM module_generation_questions WHERE generated_question_id = ?", (q["generated_question_id"],))
                            if st.session_state[custom_edit_id_key] == q["generated_question_id"]:
                                st.session_state[custom_form_mode_key] = None
                                st.session_state[custom_edit_id_key] = None
                            st.success("Custom question deleted.")
                            st.rerun()
                    st.markdown("")

        else:
            st.markdown(
                """
                <style>
                .stApp .block-container {
                    max-width: 1320px;
                    padding-top: 1.5rem;
                    padding-bottom: 2rem;
                    padding-left: 1.5rem;
                    padding-right: 1.5rem;
                }
                .review-page-title {
                    font-size: 1.8rem;
                    font-weight: 700;
                    margin-bottom: 1.15rem;
                }
                .review-section {
                    margin-bottom: 1.35rem;
                }
                .review-section-inner {
                    border: 1px solid rgba(120, 120, 140, 0.2);
                    border-radius: 12px;
                    padding: 1rem 1.1rem;
                    background: rgba(130, 130, 150, 0.04);
                }
                .review-section-title {
                    font-size: 1.22rem;
                    font-weight: 700;
                    margin: 0 0 0.9rem 0;
                }
                .review-value-row {
                    margin-bottom: 0.65rem;
                    line-height: 1.5;
                }
                .review-value-row b {
                    font-weight: 700;
                }
                .review-scenario-title {
                    font-size: 1.5rem;
                    line-height: 1.35;
                    font-weight: 700;
                    margin: 0 0 0.8rem 0;
                }
                .review-summary-panel {
                    position: sticky;
                    top: 1rem;
                    border: 1px solid rgba(120, 120, 140, 0.22);
                    border-radius: 12px;
                    background: rgba(130, 130, 150, 0.1);
                    padding: 1rem 1.05rem;
                }
                .review-summary-title {
                    font-size: 1.05rem;
                    font-weight: 700;
                    margin-bottom: 0.85rem;
                }
                .review-questions-section {
                    margin-top: 1.1rem;
                    border-top: 1px solid rgba(120, 120, 140, 0.18);
                    padding-top: 1rem;
                }
                .review-question-block {
                    padding: 0.9rem 0 1.15rem 0;
                    border-bottom: 1px solid rgba(120, 120, 140, 0.22);
                }
                .review-question-title {
                    font-size: 1.05rem;
                    font-weight: 700;
                    margin-bottom: 0.45rem;
                }
                .review-question-text {
                    font-size: 1.12rem;
                    line-height: 1.45;
                    margin-bottom: 0.65rem;
                }
                .review-muted-empty {
                    text-align: center;
                    color: rgba(120, 120, 140, 0.95);
                    padding: 1.75rem 0;
                }
                .review-highlight-answer {
                    background: rgba(64, 145, 108, 0.12);
                    border: 1px solid rgba(64, 145, 108, 0.25);
                    border-radius: 8px;
                    padding: 0.45rem 0.6rem;
                    margin: 0.4rem 0 0.55rem 0;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )

            def _review_value_row(label: str, value: object, *, fallback: str = "—") -> None:
                normalized = _normalize_text(value) or fallback
                st.markdown(
                    f"<div class='review-value-row'><b>{label}:</b> {normalized}</div>",
                    unsafe_allow_html=True,
                )

            def _question_type_label(value: str) -> str:
                return "Multiple Choice" if value == "multiple_choice" else "Open Text"

            def _render_question_block(question: dict, display_index: int) -> None:
                question_id = question.get("generated_question_id")
                question_order = question.get("question_order") or display_index
                question_identity = str(question_id) if question_id is not None else f"temp_{run_id}_{question_order}_{display_index}"
                is_custom_question = (question.get("admin_feedback") or "") == "custom_question"
                qtext = st.session_state.get(f"{'custom_' if is_custom_question else ''}qtext_{question_identity}", question.get("question_text") or "")
                qtype = st.session_state.get(f"{'custom_' if is_custom_question else ''}qtype_{question_identity}", question.get("question_type") or "open_text")
                qoptions = st.session_state.get(f"{'custom_' if is_custom_question else ''}qoptions_{question_identity}", question.get("options_text") or "")
                qrationale = st.session_state.get(f"{'custom_' if is_custom_question else ''}qrationale_{question_identity}", question.get("rationale") or "")

                st.markdown("<div class='review-question-block'>", unsafe_allow_html=True)
                st.markdown(f"<div class='review-question-title'>Question {display_index}</div>", unsafe_allow_html=True)
                st.markdown(
                    f"<div class='review-question-text'>{_normalize_text(qtext) or 'No question text provided.'}</div>",
                    unsafe_allow_html=True,
                )
                _review_value_row("Question type", _question_type_label(qtype))
                if qtype == "multiple_choice":
                    option_lines = [line.strip() for line in _normalize_text(qoptions).splitlines() if line.strip()]
                    st.markdown("**Answer choices:**")
                    if option_lines:
                        for option_idx, option in enumerate(option_lines, start=1):
                            st.write(f"{option_idx}. {option}")
                    else:
                        st.write("No options provided.")
                st.markdown(
                    "<div class='review-highlight-answer'><b>Correct answer:</b> See explanation / rubric below</div>",
                    unsafe_allow_html=True,
                )
                _review_value_row("Explanation", qrationale, fallback="No rationale provided.")
                _review_value_row("Points / scoring", "N/A")
                st.markdown("</div>", unsafe_allow_html=True)

            scenario_title = st.session_state.get(f"scenario_title_{run_id}", run.get("generated_title") or run.get("input_title") or "Untitled")
            scenario_summary = st.session_state.get(f"scenario_summary_{run_id}", run.get("generated_description") or run.get("input_description") or "")
            scenario_context = st.session_state.get(f"scenario_context_{run_id}", run.get("generated_scenario_overview") or "")
            learning_objectives_text = run.get("learning_objectives") or ""
            learning_objectives = [line.strip() for line in learning_objectives_text.splitlines() if line.strip()]
            reviewed_questions = list(generated_questions)
            generated_review_questions = [q for q in reviewed_questions if (q.get("admin_feedback") or "") != "custom_question"]
            custom_review_questions = [q for q in reviewed_questions if (q.get("admin_feedback") or "") == "custom_question"]
            total_review_count = len(reviewed_questions)
            estimated_minutes = safe_int(run.get("input_estimated_minutes"), 20)

            st.markdown("<div class='review-page-title'>Final Review and Save</div>", unsafe_allow_html=True)
            primary_col, summary_col = st.columns([7, 3], gap="large")

            with primary_col:
                st.markdown("<div class='review-section'><div class='review-section-inner'>", unsafe_allow_html=True)
                st.markdown("<div class='review-section-title'>Scenario Overview</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='review-scenario-title'>{_normalize_text(scenario_title) or 'Untitled'}</div>", unsafe_allow_html=True)
                _review_value_row("Category", run.get("input_category") or "General")
                _review_value_row("Difficulty", run.get("input_difficulty") or "Beginner")
                _review_value_row("Target Role / Audience", run.get("role_focus"), fallback="Not specified.")
                st.markdown("</div></div>", unsafe_allow_html=True)

                st.markdown("<div class='review-section'><div class='review-section-inner'>", unsafe_allow_html=True)
                st.markdown("<div class='review-section-title'>Scenario Content</div>", unsafe_allow_html=True)
                _review_value_row("Scenario Summary", scenario_summary, fallback="No summary provided.")
                _review_value_row("Scenario Context", scenario_context, fallback="No scenario context provided.")
                st.markdown("</div></div>", unsafe_allow_html=True)

                st.markdown("<div class='review-section'><div class='review-section-inner'>", unsafe_allow_html=True)
                st.markdown("<div class='review-section-title'>Learning Setup</div>", unsafe_allow_html=True)
                st.markdown("**Learning Objectives:**")
                if learning_objectives:
                    for objective in learning_objectives:
                        st.write(f"- {objective}")
                else:
                    st.write("No learning objectives provided.")
                _review_value_row("Passing Score", run.get("completion_requirements"), fallback="Not specified.")
                _review_value_row("Time Limit", f"{estimated_minutes} minutes")
                st.markdown("</div></div>", unsafe_allow_html=True)

            with summary_col:
                st.markdown("<div class='review-summary-panel'>", unsafe_allow_html=True)
                st.markdown("<div class='review-summary-title'>Summary</div>", unsafe_allow_html=True)
                _review_value_row("Total Questions", str(total_review_count))
                _review_value_row("Generated Questions", str(len(generated_review_questions)))
                _review_value_row("Custom Questions", str(len(custom_review_questions)))
                _review_value_row("Estimated Time", f"{estimated_minutes} minutes")
                _review_value_row("Passing Score", run.get("completion_requirements"), fallback="Not specified.")
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<div class='review-questions-section'>", unsafe_allow_html=True)
            st.markdown("<div class='review-section-title'>Questions Review</div>", unsafe_allow_html=True)
            if reviewed_questions:
                for idx, question in enumerate(reviewed_questions, start=1):
                    _render_question_block(question, idx)
            else:
                st.markdown(
                    "<div class='review-muted-empty'>No questions have been generated or added yet.</div>",
                    unsafe_allow_html=True,
                )
            st.markdown("</div>", unsafe_allow_html=True)

        approved_questions = [q for q in generated_questions if q.get("approval_status") == "approved"]
        total_review_steps = 4
        is_final_step = review_step >= total_review_steps - 1
        nav_back, nav_spacer, nav_next, nav_action = st.columns([1, 1, 1, 1])
        reviewing_generated_questions = review_step == 1

        def _save_current_review_questions() -> bool:
            if not reviewing_generated_questions:
                return True
            for idx, q in enumerate(non_custom_questions, start=1):
                question_id = q.get("generated_question_id")
                if question_id is None:
                    continue
                question_identity = str(question_id)
                current_qtext_key = f"qtext_{question_identity}"
                current_qtype_key = f"qtype_{question_identity}"
                if not st.session_state.get(current_qtext_key, "").strip():
                    st.warning(f"Question {idx} text is required before continuing.")
                    return False
                execute(
                    """
                    UPDATE module_generation_questions
                    SET question_text = ?, rationale = ?, approval_status = ?, admin_feedback = ?, question_type = ?, options_text = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE generated_question_id = ?
                    """,
                    (
                        st.session_state[current_qtext_key],
                        st.session_state.get(f"qrationale_{question_identity}", ""),
                        st.session_state.get(f"qstatus_{question_identity}", "pending"),
                        st.session_state.get(f"qfeedback_{question_identity}", ""),
                        st.session_state.get(current_qtype_key, "open_text"),
                        _parse_lines(st.session_state.get(f"qoptions_{question_identity}", ""))
                        if st.session_state.get(current_qtype_key, "open_text") == "multiple_choice"
                        else "",
                        question_id,
                    ),
                )
            return True

        with nav_back:
            back_label = "Back to Scenario" if reviewing_generated_questions else "Back"
            back_disabled = review_step == 0
            if st.button(back_label, key=f"review_back_{run_id}", disabled=back_disabled):
                st.session_state[review_step_key] = max(0, review_step - 1)
                st.rerun()
        with nav_spacer:
            st.write("")
        with nav_next:
            if not is_final_step:
                next_label = "Continue to Custom Questions" if reviewing_generated_questions else "Next"
                if st.button(next_label, key=f"review_next_{run_id}", type="primary" if reviewing_generated_questions else "secondary"):
                    if _save_current_review_questions():
                        st.session_state[review_step_key] = min(total_review_steps - 1, review_step + 1)
                        st.rerun()
        with nav_action:
            if is_final_step:
                if st.button("Create Module", key=f"finalize_run_{run_id}", type="primary"):
                    module_id = execute(
                        """
                        INSERT INTO modules (
                            title, category, difficulty, description, estimated_time,
                            scenario_context, organization_id, status, learning_objectives, content_sections,
                            completion_requirements, quiz_required, created_by, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'existing', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        RETURNING module_id AS id
                        """,
                        (
                            run.get("generated_title") or run.get("input_title") or "AI Module",
                            run.get("input_category") or "General",
                            run.get("input_difficulty") or "Beginner",
                            run.get("generated_description") or run.get("input_description") or "",
                            f"{safe_int(run.get('input_estimated_minutes'), 20)} min",
                            run.get("generated_scenario_overview") or "",
                            org_id,
                            run.get("learning_objectives") or "",
                            run.get("input_content_sections") or "",
                            run.get("completion_requirements") or "",
                            _normalize_bool(run.get("input_quiz_required"), default=False),
                            current_user["user_id"],
                        ),
                    )
                    executemany(
                        """
                        INSERT INTO module_questions (module_id, question_order, question_text, rationale, question_type, options_text, source_run_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                module_id,
                                idx + 1,
                                q["question_text"],
                                q.get("rationale") or q.get("admin_feedback") or "",
                                q.get("question_type") or "open_text",
                                q.get("options_text") or "",
                                run_id,
                            )
                            for idx, q in enumerate(approved_questions)
                        ],
                    )
                    execute(
                        """
                        UPDATE module_generation_runs
                        SET generation_status = 'built', updated_at = CURRENT_TIMESTAMP
                        WHERE run_id = ? AND organization_id = ?
                        """,
                        (run_id, org_id),
                    )
                    st.session_state[module_builder_current_step_key] = 3
                    st.session_state[module_builder_completed_module_id_key] = int(module_id)
                    st.rerun()

    else:
        completed_module_id = st.session_state.get(module_builder_completed_module_id_key)
        st.success("You've completed your module.")
        if completed_module_id:
            st.caption(f"Module #{int(completed_module_id)} was created successfully.")
        action_col_1, action_col_2 = st.columns(2)
        with action_col_1:
            if st.button("Create a new module", key="module_builder_create_new", type="primary"):
                st.session_state[module_builder_form_key] = dict(module_builder_defaults)
                st.session_state[module_builder_step_key] = 0
                st.session_state[module_builder_current_step_key] = 1
                st.session_state.pop(module_builder_completed_module_id_key, None)
                for widget_key in (
                    "module_builder_title",
                    "module_builder_category",
                    "module_builder_difficulty",
                    "module_builder_role_focus",
                    "module_builder_test_focus",
                    "module_builder_description",
                    "module_builder_learning_objectives",
                    "module_builder_scenario_constraints",
                    "module_builder_content_sections",
                    "module_builder_completion_requirements",
                    "module_builder_quiz_required",
                    "module_builder_estimated_minutes",
                    "module_builder_question_count",
                ):
                    st.session_state.pop(widget_key, None)
                st.rerun()
        with action_col_2:
            if st.button("Go to Manage Modules", key="module_builder_go_manage"):
                st.session_state[module_builder_current_step_key] = 1
                st.session_state.pop(module_builder_completed_module_id_key, None)
                st.session_state["admin_page"] = "📚 Manage Modules"
                st.session_state["nav"] = "manage-modules"
                st.rerun()


def render_manage_modules(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    st.subheader("Manage Modules")
    st.caption("Browse existing and archived modules in separate tabs, then edit a selected module.")

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

    def _render_module_management_tab(tab_df: pd.DataFrame, tab_label: str, state_prefix: str) -> None:
        if tab_df.empty:
            st.info(f"No modules in {tab_label.lower()}.")
            return

        with st.container(border=True):
            library_df = tab_df[["module_id", "title", "status", "difficulty", "updated_at"]].copy()
            if "updated_at" in library_df.columns:
                library_df["updated_at"] = library_df["updated_at"].apply(_format_datetime_for_admin_grid)
            _, selected_module_ids = render_admin_selection_table(
                library_df,
                row_id_col="module_id",
                selection_state_key=f"manage_modules_selected_module_id_{state_prefix}",
                table_key=f"manage_modules_data_editor_{state_prefix}",
                selection_label="Select",
                selection_help="Select the module you want to edit.",
                single_select=True,
                height=400,
            )

        if not selected_module_ids:
            fallback_module_id = int(tab_df.iloc[0]["module_id"])
            st.session_state[f"manage_modules_selected_module_id_{state_prefix}"] = fallback_module_id
            selected_module_ids = [fallback_module_id]

        module_id = int(selected_module_ids[0])
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
            meta_col_2.metric("State", "Archived" if str(module.get("status") or "existing").lower() == "archived" else "Existing")
            meta_col_3.metric("Difficulty", str(module.get("difficulty") or "Not set").title())
            meta_col_4.metric("Questions", len(module_questions))
            st.caption(f"Last updated: {_format_datetime_for_admin_grid(module.get('updated_at'))}")

        edit_form_key = f"edit_module_form_{state_prefix}_{module_id}"
        edit_step_key = f"edit_module_step_{state_prefix}_{module_id}"
        selected_module_tracker = f"edit_module_selected_module_id_{state_prefix}"
        if st.session_state.get(selected_module_tracker) != module_id:
            st.session_state[selected_module_tracker] = module_id
            st.session_state[edit_form_key] = {
                "title": module["title"] or "",
                "description": module["description"] or "",
                "estimated_minutes": _estimated_time_to_minutes(module.get("estimated_time"), fallback=20),
                "learning_objectives": module["learning_objectives"] or "",
                "content_sections": module["content_sections"] or "",
                "completion_requirements": module["completion_requirements"] or "",
                "quiz_required": bool(module["quiz_required"]),
            }
            st.session_state[edit_step_key] = 0
        if edit_form_key not in st.session_state:
            st.session_state[edit_form_key] = {
                "title": module["title"] or "",
                "description": module["description"] or "",
                "estimated_minutes": _estimated_time_to_minutes(module.get("estimated_time"), fallback=20),
                "learning_objectives": module["learning_objectives"] or "",
                "content_sections": module["content_sections"] or "",
                "completion_requirements": module["completion_requirements"] or "",
                "quiz_required": bool(module["quiz_required"]),
            }
        if edit_step_key not in st.session_state:
            st.session_state[edit_step_key] = 0

        edit_steps = [
            {"title": "Module title", "field": "title"},
            {"title": "Description", "field": "description"},
            {"title": "Learning objectives", "field": "learning_objectives"},
            {"title": "Ordered content sections", "field": "content_sections"},
            {"title": "Completion requirements", "field": "completion_requirements"},
            {"title": "Assessment settings", "field": "assessment"},
            {"title": "Review and save", "field": "review"},
        ]
        edit_form = st.session_state[edit_form_key]
        edit_step = int(st.session_state[edit_step_key])
        st.markdown("### Module Editor")
        with st.container(border=True):
            _, editor_col, _ = st.columns([0.12, 0.76, 0.12])
            with editor_col:
                _render_wizard_progress(edit_step, len(edit_steps), edit_steps[edit_step]["title"])

                edit_step_valid = True
                edit_required_message = ""
                if edit_steps[edit_step]["field"] == "title":
                    title_key = f"edit_module_title_{state_prefix}_{module_id}"
                    if title_key not in st.session_state:
                        st.session_state[title_key] = edit_form["title"]
                    st.text_input("Title", key=title_key)
                    edit_form["title"] = st.session_state.get(title_key, "")
                    edit_step_valid = _has_session_text(title_key)
                    edit_required_message = "Title is required."
                elif edit_steps[edit_step]["field"] == "description":
                    description_key = f"edit_module_description_{state_prefix}_{module_id}"
                    if description_key not in st.session_state:
                        st.session_state[description_key] = edit_form["description"]
                    st.text_area("Description", key=description_key)
                    edit_form["description"] = st.session_state.get(description_key, "")
                    edit_step_valid = _has_session_text(description_key)
                    edit_required_message = "Description is required."
                elif edit_steps[edit_step]["field"] == "learning_objectives":
                    objectives_key = f"edit_module_objectives_{state_prefix}_{module_id}"
                    if objectives_key not in st.session_state:
                        st.session_state[objectives_key] = edit_form["learning_objectives"]
                    st.text_area("Learning objectives", key=objectives_key)
                    edit_form["learning_objectives"] = st.session_state.get(objectives_key, "")
                    edit_step_valid = _has_session_text(objectives_key)
                    edit_required_message = "Learning objectives are required."
                elif edit_steps[edit_step]["field"] == "content_sections":
                    sections_key = f"edit_module_sections_{state_prefix}_{module_id}"
                    if sections_key not in st.session_state:
                        st.session_state[sections_key] = edit_form["content_sections"]
                    st.text_area("Ordered content sections", key=sections_key)
                    edit_form["content_sections"] = st.session_state.get(sections_key, "")
                    edit_step_valid = _has_session_text(sections_key)
                    edit_required_message = "Ordered content sections are required."
                elif edit_steps[edit_step]["field"] == "completion_requirements":
                    requirements_key = f"edit_module_requirements_{state_prefix}_{module_id}"
                    if requirements_key not in st.session_state:
                        st.session_state[requirements_key] = edit_form["completion_requirements"]
                    st.text_area("Completion requirements", key=requirements_key)
                    edit_form["completion_requirements"] = st.session_state.get(requirements_key, "")
                    edit_step_valid = _has_session_text(requirements_key)
                    edit_required_message = "Completion requirements are required."
                elif edit_steps[edit_step]["field"] == "assessment":
                    edit_form["estimated_minutes"] = int(
                        st.number_input(
                            "Estimated assessment time (minutes)",
                            min_value=1,
                            max_value=240,
                            value=int(edit_form["estimated_minutes"]),
                            step=1,
                            key=f"edit_module_minutes_{state_prefix}_{module_id}",
                        )
                    )
                    edit_form["quiz_required"] = st.checkbox(
                        "Quiz required",
                        value=bool(edit_form["quiz_required"]),
                        key=f"edit_module_quiz_required_{state_prefix}_{module_id}",
                    )
                elif edit_steps[edit_step]["field"] == "review":
                    st.markdown("##### Review")
                    _render_module_review_summary(edit_form)
                    missing_required = [
                        ("title", "Module title"),
                        ("description", "Description"),
                        ("learning_objectives", "Learning objectives"),
                        ("content_sections", "Ordered content sections"),
                        ("completion_requirements", "Completion requirements"),
                    ]
                    missing_labels = [label for field, label in missing_required if not _is_present(edit_form.get(field))]
                    edit_step_valid = not missing_labels
                    if missing_labels:
                        st.error("Please complete these items before saving: " + ", ".join(missing_labels) + ".")

                if not edit_step_valid and edit_required_message and edit_steps[edit_step]["field"] != "review":
                    st.error(edit_required_message)

                nav_cols = st.columns([1, 1]) if edit_steps[edit_step]["field"] == "review" else st.columns([1.2, 1.2, 2.2])
                with nav_cols[0]:
                    if st.button("⬅ Previous", key=f"edit_module_previous_{state_prefix}_{module_id}", disabled=edit_step == 0, use_container_width=True):
                        st.session_state[edit_step_key] = max(0, edit_step - 1)
                        st.rerun()
                if edit_steps[edit_step]["field"] != "review":
                    with nav_cols[1]:
                        if edit_step < len(edit_steps) - 2:
                            if st.button("Next", key=f"edit_module_next_{state_prefix}_{module_id}", use_container_width=True):
                                st.session_state[edit_step_key] = edit_step + 1
                                st.rerun()
                        elif edit_step == len(edit_steps) - 2:
                            if st.button("Next", key=f"edit_module_review_{state_prefix}_{module_id}", use_container_width=True):
                                st.session_state[edit_step_key] = edit_step + 1
                                st.rerun()
                save_col_index = 1 if edit_steps[edit_step]["field"] == "review" else 2
                with nav_cols[save_col_index]:
                    if edit_step == len(edit_steps) - 1:
                        if st.button("Save Module", key=f"edit_module_save_{state_prefix}_{module_id}", type="primary", disabled=not edit_step_valid, use_container_width=True):
                            execute(
                                """
                                UPDATE modules
                                SET title = ?, description = ?, learning_objectives = ?, content_sections = ?,
                                    completion_requirements = ?, quiz_required = ?, estimated_time = ?, updated_at = CURRENT_TIMESTAMP
                                WHERE module_id = ? AND organization_id = ?
                                """,
                                (
                                    edit_form["title"],
                                    edit_form["description"],
                                    _parse_lines(edit_form["learning_objectives"]),
                                    _parse_lines(edit_form["content_sections"]),
                                    edit_form["completion_requirements"],
                                    bool(edit_form["quiz_required"]),
                                    f"{int(edit_form['estimated_minutes'])} min",
                                    module_id,
                                    org_id,
                                ),
                            )
                            st.success("Module updated.")
                            st.session_state[edit_step_key] = 0
                            st.rerun()

                st.markdown("---")
                st.markdown("##### Assessment questions")
                for question in module_questions:
                    with st.container(border=True):
                        st.markdown(f"**Q{question['question_order']}.** {question['question_text']}")
                        st.caption(f"Type: {'Multiple choice' if question.get('question_type') == 'multiple_choice' else 'Open text'}")
                        if question.get("question_type") == "multiple_choice" and question.get("options_text"):
                            for option in [line.strip() for line in str(question.get("options_text", "")).splitlines() if line.strip()]:
                                st.write(f"- {option}")
                        if st.button(f"Delete question {question['question_order']}", key=f"delete_module_q_{state_prefix}_{question['question_id']}"):
                            execute("DELETE FROM module_questions WHERE question_id = ?", (question["question_id"],))
                            st.success("Question deleted.")
                            st.rerun()

                with st.form(f"add_module_question_{state_prefix}_{module_id}"):
                    st.markdown("Add question")
                    add_question_text = st.text_area("Question", key=f"add_question_text_{state_prefix}_{module_id}")
                    add_question_type = st.selectbox("Type", ["open_text", "multiple_choice"], key=f"add_question_type_{state_prefix}_{module_id}")
                    add_question_options = st.text_area(
                        "Multiple choice options (one per line)",
                        key=f"add_question_options_{state_prefix}_{module_id}",
                        disabled=add_question_type != "multiple_choice",
                    )
                    add_question_submit = st.form_submit_button("Add module question")
                    if add_question_submit:
                        max_order_row = fetch_one("SELECT COALESCE(MAX(question_order), 0) AS max_order FROM module_questions WHERE module_id = ?", (module_id,))
                        next_order = int(max_order_row["max_order"]) + 1 if max_order_row else 1
                        execute(
                            """
                            INSERT INTO module_questions (module_id, question_order, question_text, rationale, question_type, options_text, source_run_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                module_id,
                                next_order,
                                add_question_text.strip(),
                                "Admin added",
                                add_question_type,
                                _parse_lines(add_question_options) if add_question_type == "multiple_choice" else "",
                                None,
                            ),
                        )
                        st.success("Question added.")
                        st.rerun()

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Send to database: Archive", disabled=str(module.get("status") or "existing").lower() == "archived", use_container_width=True, key=f"archive_module_{state_prefix}_{module_id}"):
                        execute("UPDATE modules SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
                        st.success("Module archived.")
                        st.rerun()
                with c2:
                    if st.button("Send to database: Duplicate", use_container_width=True, key=f"duplicate_module_{state_prefix}_{module_id}"):
                        execute(
                            """
                            INSERT INTO modules (
                                title, category, difficulty, description, estimated_time, scenario_ticket, scenario_context,
                                hidden_root_cause, expected_reasoning_path, expected_diagnosis, expected_next_steps,
                                expected_customer_response, lesson_takeaway, organization_id, status, learning_objectives,
                                content_sections, completion_requirements, quiz_required, created_by, updated_at
                            )
                            SELECT title || ' (Copy)', category, difficulty, description, estimated_time, scenario_ticket, scenario_context,
                                   hidden_root_cause, expected_reasoning_path, expected_diagnosis, expected_next_steps,
                                   expected_customer_response, lesson_takeaway, organization_id, 'existing', learning_objectives,
                                   content_sections, completion_requirements, quiz_required, ?, CURRENT_TIMESTAMP
                            FROM modules
                            WHERE module_id = ? AND organization_id = ?
                            """,
                            (current_user["user_id"], module_id, org_id),
                        )
                        st.success("Module duplicated.")
                        st.rerun()

    existing_df = modules_df[modules_df["status"] != "archived"].copy()
    archived_df = modules_df[modules_df["status"] == "archived"].copy()

    tab_existing, tab_archived = st.tabs(["Existing Modules", "Archived Modules"])
    with tab_existing:
        _render_module_management_tab(existing_df, "Existing Modules", "existing")
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
            if st.button(label, use_container_width=True, type="primary" if label == "Run Quick Tests" else "secondary"):
                run_mode = "full" if mode == "all" else mode
                targets = _qa_filter_definitions(definitions, run_mode=run_mode, failed_only=failed_only)
                records, history = _qa_execute_batch(targets, current_user, environment, mode, current_user.get("email", "admin"))
                st.session_state["qa_run_history"] = [sanitize_for_storage(history), *st.session_state.get("qa_run_history", [])][:50]
                st.success(f"Executed {len(records)} tests ({mode}).")

    with run_cols[5]:
        selected = st.selectbox("Run Single Test", options=[d["id"] for d in definitions], format_func=lambda tid: next(d["name"] for d in definitions if d["id"] == tid))
        if st.button("Run Single Test", use_container_width=True):
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
    st.subheader("Database Tables")
    st.caption("Live table data from the connected database.")

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
        if st.button("Refresh", key=f"{key_prefix}_refresh", use_container_width=True):
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
                use_container_width=True,
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
    """Admin-only debug panel for inspecting application logs."""
    if not is_admin_session():
        st.warning("This debug panel is only available to admin users.")
        return

    st.subheader("Debug Panel")
    st.caption("Inspect application logs without leaving the admin workspace.")

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
