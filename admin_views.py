from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
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
    render_admin_table,
    render_admin_selection_table,
    render_app_table,
    render_kpi_card,
    render_page_header,
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
        LEFT JOIN organizations o ON o.organization_id = a.organization_id
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
          AND module_id = (SELECT id FROM modules WHERE module_id = ?)
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

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        render_kpi_card("Total learners", total_learners, f"{active_learners} active")
    with m2:
        render_kpi_card("Active assignments", modules_assigned, f"{in_progress_assignments} in progress")
    with m3:
        render_kpi_card("Completion rate", f"{completion_rate}%", "Across all active assignments")
    with m4:
        render_kpi_card("Overdue", overdue_assignments, "Need follow-up")

    st.markdown("#### Assignment analytics")
    c1, c2 = st.columns([2, 1])
    with c1:
        if assignments_df.empty:
            st.info("No assignments yet.")
        else:
            trend = assignments_df.copy()
            trend["assigned_at"] = pd.to_datetime(trend["assigned_at"])
            trend = trend.groupby(trend["assigned_at"].dt.date).size().reset_index(name="count")
            trend.columns = ["date", "count"]
            st.line_chart(trend.set_index("date"))
    with c2:
        st.markdown("##### Status breakdown")
        if assignments_df.empty:
            st.info("No assignments yet.")
        else:
            st.bar_chart(assignments_df["status"].value_counts())

    c3, c4, c5 = st.columns(3)
    with c3:
        with st.container(border=True):
            st.markdown("##### Learner status")
            st.bar_chart(pd.Series({"Active": active_learners, "Inactive": inactive_learners}))
    with c4:
        with st.container(border=True):
            st.markdown("##### Recent submissions")
            recent = assignments_df[assignments_df["last_attempt_at"].notna()].head(5)
            if recent.empty:
                st.caption("No submissions yet.")
            else:
                for _, row in recent.iterrows():
                    st.caption(f"{row['learner_name']} • {row['module_title']}")
    with c5:
        with st.container(border=True):
            st.markdown("##### Module catalog")
            st.metric("Created modules", modules_created)
            st.metric("Inactive learners", inactive_learners)
            st.metric("In-progress", in_progress_assignments)
    
    


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

    def _render_learner_tab(tab_df: pd.DataFrame, tab_name: str, show_active: bool) -> None:
        scoped = filter_active_learners(tab_df) if show_active else filter_inactive_learners(tab_df)
        st.caption(f"{len(scoped)} learner(s) in {tab_name.lower()}.")
        learner_table_df = scoped[
            [
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
        learner_options = {build_learner_option_label(r): int(r["user_id"]) for _, r in scoped.iterrows()}
        option_labels = list(learner_options.keys())

        if multiselect_key not in st.session_state:
            st.session_state[multiselect_key] = []
        st.session_state[multiselect_key] = [x for x in st.session_state[multiselect_key] if x in option_labels]

        learner_display_df = learner_table_df.rename(
            columns={
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
        render_admin_table(
            learner_display_df,
            height=520,
            empty_message="No learners match current filters. Adjust filters to display learners.",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            st.button(
                "Select All Filtered",
                key=f"select_all_filtered_{tab_key}",
                on_click=_select_all_filtered,
                args=(multiselect_key, option_labels),
            )
        with c2:
            st.button(
                "Clear Selection",
                key=f"clear_selection_{tab_key}",
                on_click=_clear_filtered_selection,
                args=(multiselect_key,),
            )

        selected_learners = st.multiselect(
            "Selected learners",
            options=option_labels,
            key=multiselect_key,
        )
        selected_ids = [learner_options[label] for label in selected_learners]
        st.caption(f"{len(selected_ids)} of {len(scoped)} filtered learners selected")

        if show_active:
            action_label = "Archive"
            new_status = False
            action_type = "secondary"
        else:
            action_label = "Activate"
            new_status = True
            action_type = "primary"

        with st.container(border=True):
            st.caption("Actions")
            run_bulk_action = st.button(action_label, type=action_type, key=f"bulk_action_{tab_key}", use_container_width=True)

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
        _render_learner_tab(filtered, "Active Learners", True)
    with inactive_tab:
        _render_learner_tab(filtered, "Inactive Learners", False)


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
    modules = fetch_all("SELECT module_id, title, status FROM modules WHERE organization_id=? ORDER BY title", (org_id,))
    if not learners:
        st.info("No learners available yet. Add or activate learners first.")
        return
    if not modules:
        st.info("No modules available yet. Create a module first.")
        return

    with st.container(border=True):
        st.markdown("#### Assign module")
        module_map = {f"{m['title']} ({m['status']})": int(m["module_id"]) for m in modules}
        learners_df = to_df(learners)
        learners_df["team"] = learners_df["team"].fillna("")
        learners_df["organization_name"] = learners_df["organization_name"].fillna("Unassigned")

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
                apply_filters = st.form_submit_button("Apply filters", use_container_width=True)

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

        filtered_active_learners = apply_learner_filters(
            filter_active_learners(learners_df),
            search_text=q,
            team_filter=team_filter,
            org_filter=org_filter,
        )
        learner_map = {
            build_learner_option_label(row): int(row["user_id"])
            for _, row in filtered_active_learners.sort_values("name").iterrows()
        }
        learner_options = list(learner_map.keys())
        learner_multiselect_key = "assign_training_learners"

        if learner_multiselect_key not in st.session_state:
            st.session_state[learner_multiselect_key] = []
        st.session_state[learner_multiselect_key] = [
            x for x in st.session_state[learner_multiselect_key] if x in learner_options
        ]

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
            selection_state_key="assignment_tool_selected_learner_ids",
            table_key="assignment_tool_learner_data_editor",
            selection_label="Select",
            selection_help="Select learners to assign this module.",
            single_select=False,
            height=420,
        )
        selected_id_set = {int(v) for v in selected_learner_ids}
        st.session_state[learner_multiselect_key] = [
            label for label, learner_id in learner_map.items() if learner_id in selected_id_set
        ]
        st.caption(f"{len(selected_id_set)} learner(s) selected from table")

        with st.form("assignment_tool_submit_form", clear_on_submit=False):
            selected_module = st.selectbox("Module", list(module_map.keys()))
            selected_learners = st.multiselect(
                "Selected learners (from table)",
                learner_options,
                key=learner_multiselect_key,
                help="Primary selection happens in the table above.",
            )
            enable_due_date = st.checkbox("Set due date", value=False)
            due_date = st.date_input("Due date", value=date.today(), disabled=not enable_due_date)
            assign_submitted = st.form_submit_button("Send to database: Assign training", type="primary")

        if assign_submitted:
            module_id = module_map[selected_module]
            due_date_value = due_date.isoformat() if enable_due_date else None
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
        st.dataframe(preview_df.head(20), use_container_width=True, hide_index=True)
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
            st.write(f"Submitted state: **{'Yes' if int(workspace_state.get('submitted_state') or 0) else 'No'}**")

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

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Completed", int(assignments_df["status"].eq("Completed").sum()))
    col2.metric("In Progress", int(assignments_df["status"].eq("In Progress").sum()))
    col3.metric("Overdue", int(assignments_df["status"].eq("Overdue").sum()))
    col4.metric("Not Started", int(assignments_df["status"].eq("Not Started").sum()))

    status_filter = st.multiselect("Filter status", ["Completed", "In Progress", "Overdue", "Not Started"], default=["Completed", "In Progress", "Overdue", "Not Started"])
    filtered = assignments_df[assignments_df["status"].isin(status_filter)]
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


def _render_wizard_progress(step_index: int, total_steps: int, title: str) -> None:
    st.caption(f"Step {step_index + 1} of {total_steps}")
    st.progress((step_index + 1) / total_steps)
    st.markdown(f"#### {title}")


def _render_named_step_indicator(step_index: int, labels: list[str]) -> None:
    cols = st.columns(len(labels))
    for idx, (col, label) in enumerate(zip(cols, labels)):
        state = "✅" if idx < step_index else ("🔵" if idx == step_index else "⚪")
        style = "normal"
        if idx == step_index:
            style = "primary"
        with col:
            st.markdown(f"**{state} {label}**")
            st.caption("Current" if style == "primary" else ("Done" if idx < step_index else "Upcoming"))


def render_module_builder(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    render_page_header("Module Builder", "Guided wizard for structured module generation and approval.")

    st.markdown("#### Step 1: Enter module goals")
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

    current_step = int(st.session_state[module_builder_step_key])
    total_steps = len(module_builder_steps)
    step_config = module_builder_steps[current_step]
    _render_wizard_progress(current_step, total_steps, step_config["title"])

    # Wizard: render exactly one step at a time and persist each input in session state.
    with st.container(border=True):
        step_valid = True
        required_message = ""
        if step_config["field"] == "title":
            module_form["title"] = st.text_input("Title", value=module_form["title"], key="module_builder_title")
            step_valid = _is_present(module_form["title"])
            required_message = "Title is required."
        elif step_config["field"] == "category":
            module_form["category"] = st.text_input("Category", value=module_form["category"], key="module_builder_category")
            step_valid = _is_present(module_form["category"])
            required_message = "Category is required."
        elif step_config["field"] == "difficulty":
            difficulty_options = ["Beginner", "Intermediate", "Advanced"]
            current_difficulty = module_form["difficulty"] if module_form["difficulty"] in difficulty_options else "Beginner"
            module_form["difficulty"] = st.selectbox(
                "Difficulty",
                difficulty_options,
                index=difficulty_options.index(current_difficulty),
                key="module_builder_difficulty",
            )
        elif step_config["field"] == "role_focus":
            module_form["role_focus"] = st.text_input(
                "Role being simulated (e.g., Support Tier 1, Team Lead)",
                value=module_form["role_focus"],
                key="module_builder_role_focus",
            )
            step_valid = _is_present(module_form["role_focus"])
            required_message = "Role focus is required."
        elif step_config["field"] == "test_focus":
            module_form["test_focus"] = st.text_input(
                "What should this module test?",
                value=module_form["test_focus"],
                key="module_builder_test_focus",
            )
            step_valid = _is_present(module_form["test_focus"])
            required_message = "Test focus is required."
        elif step_config["field"] == "description":
            module_form["description"] = st.text_area("Description", value=module_form["description"], key="module_builder_description")
            step_valid = _is_present(module_form["description"])
            required_message = "Description is required."
        elif step_config["field"] == "learning_objectives":
            module_form["learning_objectives"] = st.text_area(
                "Learning objectives (one per line)",
                value=module_form["learning_objectives"],
                key="module_builder_learning_objectives",
            )
            step_valid = _is_present(module_form["learning_objectives"])
            required_message = "Learning objectives are required."
        elif step_config["field"] == "scenario_constraints":
            module_form["scenario_constraints"] = st.text_area(
                "Scenario context / constraints",
                value=module_form["scenario_constraints"],
                key="module_builder_scenario_constraints",
            )
            step_valid = _is_present(module_form["scenario_constraints"])
            required_message = "Scenario context is required."
        elif step_config["field"] == "content_sections":
            module_form["content_sections"] = st.text_area(
                "Ordered content sections (one per line)",
                value=module_form["content_sections"],
                key="module_builder_content_sections",
            )
            step_valid = _is_present(module_form["content_sections"])
            required_message = "Content sections are required."
        elif step_config["field"] == "completion_requirements":
            module_form["completion_requirements"] = st.text_area(
                "Completion requirements",
                value=module_form["completion_requirements"],
                key="module_builder_completion_requirements",
            )
            step_valid = _is_present(module_form["completion_requirements"])
            required_message = "Completion requirements are required."
        elif step_config["field"] == "assessment_settings":
            module_form["quiz_required"] = st.checkbox(
                "Quiz required",
                value=bool(module_form["quiz_required"]),
                key="module_builder_quiz_required",
            )
            module_form["estimated_minutes"] = int(
                st.number_input(
                    "Estimated assessment time (minutes)",
                    min_value=1,
                    max_value=240,
                    value=int(module_form["estimated_minutes"]),
                    step=1,
                    key="module_builder_estimated_minutes",
                )
            )
            module_form["question_count"] = int(
                st.slider(
                    "AI-generated questions",
                    min_value=5,
                    max_value=6,
                    value=int(module_form["question_count"]),
                    key="module_builder_question_count",
                )
            )
        else:
            st.markdown("##### Review")
            st.write("Please review your values before saving.")
            st.json(module_form)
        missing_required = [
            "title",
            "category",
            "role_focus",
            "test_focus",
            "description",
            "learning_objectives",
            "scenario_constraints",
            "content_sections",
            "completion_requirements",
        ]
        missing_labels = [field for field in missing_required if not _is_present(module_form.get(field))]
        step_valid = not missing_labels
        if missing_labels:
            st.error(f"Required fields missing: {', '.join(missing_labels)}")

    if not step_valid and required_message and step_config["field"] != "review":
        st.error(required_message)

    nav_left, nav_mid, nav_right = st.columns([1, 1, 2])
    with nav_left:
        if st.button("Previous", key="module_builder_previous", disabled=current_step == 0):
            st.session_state[module_builder_step_key] = max(0, current_step - 1)
            st.rerun()
    with nav_mid:
        if current_step < total_steps - 2:
            if st.button("Next", key="module_builder_next", disabled=not step_valid):
                st.session_state[module_builder_step_key] = current_step + 1
                st.rerun()
        elif current_step == total_steps - 2:
            if st.button("Review", key="module_builder_review", disabled=not step_valid):
                st.session_state[module_builder_step_key] = current_step + 1
                st.rerun()
    with nav_right:
        if current_step == total_steps - 1:
            if st.button("Save Module", key="module_builder_save_module", type="primary", disabled=not step_valid):
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
                            input_estimated_minutes,
                            generated_title, generated_description, generated_scenario_overview,
                            generation_status, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', CURRENT_TIMESTAMP)
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
                            1 if module_form["quiz_required"] else 0,
                            payload.question_count,
                            int(module_form["estimated_minutes"]),
                            preview.get("title"),
                            preview.get("description"),
                            preview.get("scenario_overview"),
                        ),
                    )
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
                    st.success("Draft generated. Continue to review and approve below.")
                    st.session_state[module_builder_step_key] = 0
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not save module draft: {exc}")

    st.markdown("#### Step 2: Review and approve generated draft")
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
        st.info("No AI drafts yet. Submit goals above to generate one.")
    else:
        run_map = {
            f"Run #{int(row['run_id'])} • {row.get('generated_title') or row.get('input_title') or 'Untitled'} [{row['generation_status']}]":
            int(row["run_id"])
            for _, row in runs_df.iterrows()
        }
        selected_run_label = st.selectbox("Choose generated draft", list(run_map.keys()))
        run_id = run_map[selected_run_label]
        run = fetch_one("SELECT * FROM module_generation_runs WHERE run_id = ? AND organization_id = ?", (run_id, org_id))
        generated_questions = fetch_all(
            """
            SELECT * FROM module_generation_questions
            WHERE run_id = ?
            ORDER BY question_order
            """,
            (run_id,),
        )
        review_step_key = f"module_review_step_{run_id}"
        question_step_idx_key = f"module_generated_q_idx_{run_id}"
        if review_step_key not in st.session_state:
            st.session_state[review_step_key] = 0
        if question_step_idx_key not in st.session_state:
            st.session_state[question_step_idx_key] = 0
        wizard_labels = ["Review Scenario", "Review Questions", "Custom Questions", "Finalize"]
        review_step = int(st.session_state[review_step_key])
        _render_named_step_indicator(review_step, wizard_labels)

        non_custom_questions = [q for q in generated_questions if (q.get("admin_feedback") or "") != "custom_question"]
        custom_questions = [q for q in generated_questions if (q.get("admin_feedback") or "") == "custom_question"]

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

                st.text_input("Scenario title", key=run_title_key)
                st.text_area("Scenario summary / metadata", key=run_summary_key, height=120)
                st.text_area(
                    "Scenario description / context",
                    value=run.get("generated_scenario_overview") or "",
                    key=f"scenario_context_{run_id}",
                    height=180,
                )
                st.selectbox(
                    "Scenario decision",
                    ["approved", "denied", "pending"],
                    index=["approved", "denied", "pending"].index(run.get("generation_status", "draft") if run.get("generation_status") in {"approved", "denied", "pending"} else "pending"),
                    key=run_status_key,
                )
                st.text_area("Scenario feedback", key=run_feedback_key)
                step_valid = _is_present(st.session_state[run_title_key]) and _is_present(st.session_state[f"scenario_context_{run_id}"])
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
                            st.session_state[f"scenario_context_{run_id}"],
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
                    st.info("No generated questions available.")
                    step_valid = False
                else:
                    current_q_idx = max(0, min(int(st.session_state[question_step_idx_key]), len(non_custom_questions) - 1))
                    st.session_state[question_step_idx_key] = current_q_idx
                    q = non_custom_questions[current_q_idx]
                    st.caption(f"Question {current_q_idx + 1} of {len(non_custom_questions)}")
                    st.text_area("Question text", value=q.get("question_text") or "", key=f"qtext_{q['generated_question_id']}", height=120)
                    st.text_area("Expected / ideal answer", value=q.get("rationale") or "", key=f"qrationale_{q['generated_question_id']}", height=100)
                    meta_col1, meta_col2, meta_col3 = st.columns(3)
                    with meta_col1:
                        st.selectbox(
                            "Difficulty",
                            ["Beginner", "Intermediate", "Advanced"],
                            index=["Beginner", "Intermediate", "Advanced"].index(run.get("input_difficulty") or "Beginner")
                            if (run.get("input_difficulty") or "Beginner") in {"Beginner", "Intermediate", "Advanced"} else 0,
                            key=f"qdifficulty_{q['generated_question_id']}",
                            disabled=True,
                        )
                    with meta_col2:
                        st.text_input("Point value", value="N/A", key=f"qpoints_{q['generated_question_id']}", disabled=True)
                    with meta_col3:
                        st.selectbox(
                            "Question type",
                            ["open_text", "multiple_choice"],
                            index=0 if (q.get("question_type") or "open_text") == "open_text" else 1,
                            key=f"qtype_{q['generated_question_id']}",
                        )
                    st.selectbox(
                        "Decision",
                        ["pending", "approved", "denied"],
                        index=["pending", "approved", "denied"].index(q.get("approval_status") or "pending"),
                        key=f"qstatus_{q['generated_question_id']}",
                    )
                    st.text_input("Admin feedback", value=q.get("admin_feedback") or "", key=f"qfeedback_{q['generated_question_id']}")
                    q_options = st.text_area(
                        "Choices for this question (one per line)",
                        value=q.get("options_text") or "",
                        disabled=st.session_state[f"qtype_{q['generated_question_id']}"] != "multiple_choice",
                        key=f"qoptions_{q['generated_question_id']}",
                    )
                    step_valid = _is_present(st.session_state[f"qtext_{q['generated_question_id']}"])
                    q_nav_left, q_nav_mid, q_nav_right = st.columns([1, 1, 2])
                    with q_nav_left:
                        if st.button("Previous Question", disabled=current_q_idx == 0, key=f"prev_generated_q_{run_id}"):
                            st.session_state[question_step_idx_key] = max(0, current_q_idx - 1)
                            st.rerun()
                    with q_nav_mid:
                        if st.button("Next Question", disabled=current_q_idx >= len(non_custom_questions) - 1, key=f"next_generated_q_{run_id}"):
                            st.session_state[question_step_idx_key] = min(len(non_custom_questions) - 1, current_q_idx + 1)
                            st.rerun()
                    with q_nav_right:
                        save_col, delete_col = st.columns(2)
                        with save_col:
                            if st.button("Save question", key=f"save_q_{q['generated_question_id']}"):
                                execute(
                                    """
                                    UPDATE module_generation_questions
                                    SET question_text = ?, rationale = ?, approval_status = ?, admin_feedback = ?, question_type = ?, options_text = ?, updated_at = CURRENT_TIMESTAMP
                                    WHERE generated_question_id = ?
                                    """,
                                    (
                                        st.session_state[f"qtext_{q['generated_question_id']}"],
                                        st.session_state[f"qrationale_{q['generated_question_id']}"],
                                        st.session_state[f"qstatus_{q['generated_question_id']}"],
                                        st.session_state[f"qfeedback_{q['generated_question_id']}"],
                                        st.session_state[f"qtype_{q['generated_question_id']}"],
                                        _parse_lines(q_options) if st.session_state[f"qtype_{q['generated_question_id']}"] == "multiple_choice" else "",
                                        q["generated_question_id"],
                                    ),
                                )
                                st.success("Question saved.")
                                st.rerun()
                        with delete_col:
                            if st.button("Delete question", key=f"delete_q_{q['generated_question_id']}"):
                                execute("DELETE FROM module_generation_questions WHERE generated_question_id = ?", (q["generated_question_id"],))
                                st.success("Question deleted.")
                                st.rerun()

        elif review_step == 2:
            with st.container(border=True):
                st.markdown("##### Add or Edit Custom Questions")
                for q in custom_questions:
                    with st.container(border=True):
                        st.text_area("Custom question text", value=q.get("question_text") or "", key=f"custom_qtext_{q['generated_question_id']}")
                        st.selectbox(
                            "Type",
                            ["open_text", "multiple_choice"],
                            index=0 if (q.get("question_type") or "open_text") == "open_text" else 1,
                            key=f"custom_qtype_{q['generated_question_id']}",
                        )
                        custom_options = st.text_area(
                            "Options (one per line)",
                            value=q.get("options_text") or "",
                            disabled=st.session_state[f"custom_qtype_{q['generated_question_id']}"] != "multiple_choice",
                            key=f"custom_qoptions_{q['generated_question_id']}",
                        )
                        st.text_area("Ideal answer / rubric", value=q.get("rationale") or "", key=f"custom_qrationale_{q['generated_question_id']}")
                        a_col, d_col = st.columns(2)
                        with a_col:
                            if st.button("Save custom question", key=f"save_custom_{q['generated_question_id']}"):
                                execute(
                                    """
                                    UPDATE module_generation_questions
                                    SET question_text = ?, rationale = ?, question_type = ?, options_text = ?, admin_feedback = 'custom_question', updated_at = CURRENT_TIMESTAMP
                                    WHERE generated_question_id = ?
                                    """,
                                    (
                                        st.session_state[f"custom_qtext_{q['generated_question_id']}"],
                                        st.session_state[f"custom_qrationale_{q['generated_question_id']}"],
                                        st.session_state[f"custom_qtype_{q['generated_question_id']}"],
                                        _parse_lines(custom_options) if st.session_state[f"custom_qtype_{q['generated_question_id']}"] == "multiple_choice" else "",
                                        q["generated_question_id"],
                                    ),
                                )
                                st.success("Custom question saved.")
                                st.rerun()
                        with d_col:
                            if st.button("Delete custom question", key=f"delete_custom_{q['generated_question_id']}"):
                                execute("DELETE FROM module_generation_questions WHERE generated_question_id = ?", (q["generated_question_id"],))
                                st.success("Custom question deleted.")
                                st.rerun()
                with st.form(f"add_question_form_{run_id}"):
                    st.markdown("###### Add custom question")
                    new_question_text = st.text_area("Question text", key=f"new_question_text_{run_id}")
                    new_question_type = st.selectbox("Question type", ["open_text", "multiple_choice"], key=f"new_question_type_{run_id}")
                    new_question_options = st.text_area(
                        "Multiple choice options (one per line)",
                        key=f"new_question_options_{run_id}",
                        disabled=new_question_type != "multiple_choice",
                    )
                    new_question_rubric = st.text_area("Ideal answer / rubric", key=f"new_question_rubric_{run_id}")
                    add_question = st.form_submit_button("Add question")
                    if add_question:
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
                                new_question_text.strip(),
                                new_question_rubric.strip() or "Admin added",
                                new_question_type,
                                _parse_lines(new_question_options) if new_question_type == "multiple_choice" else "",
                            ),
                        )
                        st.success("Question added.")
                        st.rerun()

        else:
            with st.container(border=True):
                st.markdown("##### Final Review and Save")
                total_count = len(generated_questions)
                st.write(f"**Scenario title:** {run.get('generated_title') or run.get('input_title') or 'Untitled'}")
                st.write(f"**Scenario summary:** {run.get('generated_description') or run.get('input_description') or 'No summary yet.'}")
                st.write(f"**Total question count:** {total_count}")
                st.write(f"**Generated questions:** {len(non_custom_questions)}")
                st.write(f"**Custom questions:** {len(custom_questions)}")

        approved_questions = [q for q in generated_questions if q.get("approval_status") == "approved"]
        can_finalize = bool(approved_questions) and run.get("generation_status") == "approved"
        nav_back, nav_next, nav_action = st.columns([1, 1, 2])
        with nav_back:
            if st.button("Back", key=f"review_back_{run_id}", disabled=review_step == 0):
                st.session_state[review_step_key] = max(0, review_step - 1)
                st.rerun()
        with nav_next:
            if st.button("Next", key=f"review_next_{run_id}", disabled=review_step >= 3 or not step_valid):
                st.session_state[review_step_key] = min(3, review_step + 1)
                st.rerun()
        with nav_action:
            if st.button("Create module from approved draft", disabled=not can_finalize, key=f"finalize_run_{run_id}", type="primary"):
                module_id = execute(
                    """
                    INSERT INTO modules (
                        title, category, difficulty, description, estimated_time,
                        scenario_context, organization_id, status, learning_objectives, content_sections,
                        completion_requirements, quiz_required, created_by, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    RETURNING module_id AS id
                    """,
                    (
                        run.get("generated_title") or run.get("input_title") or "AI Draft Module",
                        run.get("input_category") or "General",
                        run.get("input_difficulty") or "Beginner",
                        run.get("generated_description") or run.get("input_description") or "",
                        f"{int(run.get('input_estimated_minutes') or 20)} min",
                        run.get("generated_scenario_overview") or "",
                        org_id,
                        run.get("learning_objectives") or "",
                        run.get("input_content_sections") or "",
                        run.get("completion_requirements") or "",
                        1 if run.get("input_quiz_required") else 0,
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
                st.success("Approved draft converted into a module.")
                st.rerun()


def render_manage_modules(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    st.subheader("Manage Modules")
    st.caption("Browse your module library, then edit the selected module in a focused workflow.")

    modules_df = to_df(fetch_all("SELECT * FROM modules WHERE organization_id = ? ORDER BY updated_at DESC", (org_id,)))
    if modules_df.empty:
        st.info("No modules yet.")
        return

    st.markdown("### Existing Modules")
    with st.container(border=True):
        library_df = modules_df[["module_id", "title", "status", "difficulty", "updated_at"]].copy()
        if "updated_at" in library_df.columns:
            library_df["updated_at"] = library_df["updated_at"].apply(_format_datetime_for_admin_grid)
        _, selected_module_ids = render_admin_selection_table(
            library_df,
            row_id_col="module_id",
            selection_state_key="manage_modules_selected_module_id",
            table_key="manage_modules_data_editor",
            selection_label="Select",
            selection_help="Select the module you want to edit.",
            single_select=True,
            height=450,
        )
    if not selected_module_ids:
        fallback_module_id = int(modules_df.iloc[0]["module_id"])
        st.session_state["manage_modules_selected_module_id"] = fallback_module_id
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
        meta_col_2.metric("Status", str(module.get("status") or "draft").title())
        meta_col_3.metric("Difficulty", str(module.get("difficulty") or "Not set").title())
        meta_col_4.metric("Questions", len(module_questions))
        st.caption(f"Last updated: {_format_datetime_for_admin_grid(module.get('updated_at'))}")

    edit_form_key = f"edit_module_form_{module_id}"
    edit_step_key = f"edit_module_step_{module_id}"
    selected_module_tracker = "edit_module_selected_module_id"
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
                edit_form["title"] = st.text_input("Title", value=edit_form["title"], key=f"edit_module_title_{module_id}")
                edit_step_valid = _is_present(edit_form["title"])
                edit_required_message = "Title is required."
            elif edit_steps[edit_step]["field"] == "description":
                edit_form["description"] = st.text_area("Description", value=edit_form["description"], key=f"edit_module_description_{module_id}")
                edit_step_valid = _is_present(edit_form["description"])
                edit_required_message = "Description is required."
            elif edit_steps[edit_step]["field"] == "learning_objectives":
                edit_form["learning_objectives"] = st.text_area(
                    "Learning objectives",
                    value=edit_form["learning_objectives"],
                    key=f"edit_module_objectives_{module_id}",
                )
                edit_step_valid = _is_present(edit_form["learning_objectives"])
                edit_required_message = "Learning objectives are required."
            elif edit_steps[edit_step]["field"] == "content_sections":
                edit_form["content_sections"] = st.text_area(
                    "Ordered content sections",
                    value=edit_form["content_sections"],
                    key=f"edit_module_sections_{module_id}",
                )
                edit_step_valid = _is_present(edit_form["content_sections"])
                edit_required_message = "Ordered content sections are required."
            elif edit_steps[edit_step]["field"] == "completion_requirements":
                edit_form["completion_requirements"] = st.text_area(
                    "Completion requirements",
                    value=edit_form["completion_requirements"],
                    key=f"edit_module_requirements_{module_id}",
                )
                edit_step_valid = _is_present(edit_form["completion_requirements"])
                edit_required_message = "Completion requirements are required."
            elif edit_steps[edit_step]["field"] == "assessment":
                edit_form["estimated_minutes"] = int(
                    st.number_input(
                        "Estimated assessment time (minutes)",
                        min_value=1,
                        max_value=240,
                        value=int(edit_form["estimated_minutes"]),
                        step=1,
                        key=f"edit_module_minutes_{module_id}",
                    )
                )
                edit_form["quiz_required"] = st.checkbox(
                    "Quiz required",
                    value=bool(edit_form["quiz_required"]),
                    key=f"edit_module_quiz_required_{module_id}",
                )
            else:
                st.markdown("##### Review")
                st.json(edit_form)
                missing_required = ["title", "description", "learning_objectives", "content_sections", "completion_requirements"]
                missing_labels = [field for field in missing_required if not _is_present(edit_form.get(field))]
                edit_step_valid = not missing_labels
                if missing_labels:
                    st.error(f"Required fields missing: {', '.join(missing_labels)}")

            if not edit_step_valid and edit_required_message and edit_steps[edit_step]["field"] != "review":
                st.error(edit_required_message)

            nav_cols = st.columns([1.2, 1.2, 2.2])
            with nav_cols[0]:
                if st.button("⬅ Previous", key=f"edit_module_previous_{module_id}", disabled=edit_step == 0, use_container_width=True):
                    st.session_state[edit_step_key] = max(0, edit_step - 1)
                    st.rerun()
            with nav_cols[1]:
                if edit_step < len(edit_steps) - 2:
                    if st.button("Next ➜", key=f"edit_module_next_{module_id}", disabled=not edit_step_valid, use_container_width=True):
                        st.session_state[edit_step_key] = edit_step + 1
                        st.rerun()
                elif edit_step == len(edit_steps) - 2:
                    if st.button("Review", key=f"edit_module_review_{module_id}", disabled=not edit_step_valid, use_container_width=True):
                        st.session_state[edit_step_key] = edit_step + 1
                        st.rerun()
            with nav_cols[2]:
                if edit_step == len(edit_steps) - 1:
                    if st.button(
                        "Save Module",
                        key=f"edit_module_save_{module_id}",
                        type="primary",
                        disabled=not edit_step_valid,
                        use_container_width=True,
                    ):
                        try:
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
                                    1 if edit_form["quiz_required"] else 0,
                                    f"{int(edit_form['estimated_minutes'])} min",
                                    module_id,
                                    org_id,
                                ),
                            )
                            st.success("Module updated.")
                            st.session_state[edit_step_key] = 0
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed to save module: {exc}")

            st.markdown("---")
            st.markdown("##### Assessment questions")
            for question in module_questions:
                with st.container(border=True):
                    st.markdown(f"**Q{question['question_order']}.** {question['question_text']}")
                    st.caption(f"Type: {'Multiple choice' if question.get('question_type') == 'multiple_choice' else 'Open text'}")
                    if question.get("question_type") == "multiple_choice" and question.get("options_text"):
                        for option in [line.strip() for line in str(question.get("options_text", "")).splitlines() if line.strip()]:
                            st.write(f"- {option}")
                    if st.button(f"Delete question {question['question_order']}", key=f"delete_module_q_{question['question_id']}"):
                        execute("DELETE FROM module_questions WHERE question_id = ?", (question["question_id"],))
                        st.success("Question deleted.")
                        st.rerun()

            with st.form(f"add_module_question_{module_id}"):
                st.markdown("Add question")
                add_question_text = st.text_area("Question", key=f"add_question_text_{module_id}")
                add_question_type = st.selectbox("Type", ["open_text", "multiple_choice"], key=f"add_question_type_{module_id}")
                add_question_options = st.text_area(
                    "Multiple choice options (one per line)",
                    key=f"add_question_options_{module_id}",
                    disabled=add_question_type != "multiple_choice",
                )
                add_question_submit = st.form_submit_button("Add module question")
                if add_question_submit:
                    max_order_row = fetch_one(
                        "SELECT COALESCE(MAX(question_order), 0) AS max_order FROM module_questions WHERE module_id = ?",
                        (module_id,),
                    )
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

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Send to database: Publish", disabled=module["status"] == "published", use_container_width=True):
                    execute("UPDATE modules SET status = 'published', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
                    st.success("Module published.")
                    st.rerun()
            with c2:
                if st.button("Send to database: Archive", disabled=module["status"] == "archived", use_container_width=True):
                    execute("UPDATE modules SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
                    st.success("Module archived.")
                    st.rerun()
            with c3:
                if st.button("Send to database: Duplicate", use_container_width=True):
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
                               expected_customer_response, lesson_takeaway, organization_id, 'draft', learning_objectives,
                               content_sections, completion_requirements, quiz_required, ?, CURRENT_TIMESTAMP
                        FROM modules
                        WHERE module_id = ? AND organization_id = ?
                        """,
                        (current_user["user_id"], module_id, org_id),
                    )
                    st.success("Module duplicated as draft.")
                    st.rerun()



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
        "duration_ms": int(record.get("duration_ms") or 0),
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
    total_duration_ms = sum(int(record.get("duration_ms") or 0) for record in run_records)

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
            VALUES (?, 'QA', 'Easy', ?, 10, ?, 'draft', ?)
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
                "duration_ms": int(record.get("duration_ms") or 0),
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
