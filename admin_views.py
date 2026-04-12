from __future__ import annotations

from datetime import date, timedelta
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
    to_df,
)
from module_generation import ModuleGenerationInput, generate_module_preview

admin_logger = get_logger(module="admin_views")


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


def _assignments_with_status(org_id: int) -> pd.DataFrame:
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
    st.subheader("Admin Dashboard")

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

    metric_row(
        {
            "Total learners": total_learners,
            "Active learners": active_learners,
            "Inactive learners": inactive_learners,
            "Modules created": modules_created,
            "Modules assigned": modules_assigned,
            "Completion rate": f"{completion_rate}%",
            "Overdue assignments": overdue_assignments,
            "In-progress assignments": in_progress_assignments,
        }
    )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Assignment status")
        if assignments_df.empty:
            st.info("No assignments yet.")
        else:
            st.bar_chart(assignments_df["status"].value_counts())
    with c2:
        st.markdown("#### Learner status")
        if learners_df.empty:
            st.info("No learners found.")
        else:
            st.bar_chart(pd.Series({"Active": active_learners, "Inactive": inactive_learners}))


def render_learner_management(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    st.subheader("Learner Management")

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

    c1, c2, c3 = st.columns(3)
    with c1:
        q = st.text_input("Search learners")
    with c2:
        team_options = sorted([team for team in df["team"].unique().tolist() if team])
        team_filter = st.selectbox("Team", ["All"] + team_options)
    with c3:
        org_options = sorted(df["organization_name"].unique().tolist())
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

        learner_table_event = st.dataframe(
            learner_table_df,
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="multi-row",
            key=f"learner_table_{tab_key}",
        )
        _merge_row_selection_into_multiselect(
            table_event=learner_table_event,
            table_df=scoped.reset_index(drop=True),
            multiselect_key=multiselect_key,
            option_labels=option_labels,
        )

        c1, c2, c3 = st.columns([1, 1, 2])
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
            "Select learners",
            options=option_labels,
            key=multiselect_key,
        )
        selected_ids = [learner_options[label] for label in selected_learners]
        st.caption(f"{len(selected_ids)} of {len(scoped)} filtered learners selected")

        if show_active:
            action_label = "Deactivate selected learners"
            new_status = False
            action_type = "secondary"
        else:
            action_label = "Activate selected learners"
            new_status = True
            action_type = "primary"

        with c3:
            run_bulk_action = st.button(action_label, type=action_type, key=f"bulk_action_{tab_key}")

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

        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            q = st.text_input("Search learners", key="assignment_search_learners")
        with fc2:
            team_options = sorted([team for team in learners_df["team"].unique().tolist() if team])
            team_filter = st.selectbox("Team/Department", ["All"] + team_options, key="assignment_team_filter")
        with fc3:
            org_options = sorted(learners_df["organization_name"].unique().tolist())
            org_filter = st.selectbox("Organization", ["All"] + org_options, key="assignment_org_filter")

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
        learner_table_event = st.dataframe(
            filtered_active_learners[["name", "team", "organization_name"]].reset_index(drop=True),
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="assignment_tool_learners_table",
        )
        _merge_row_selection_into_multiselect(
            table_event=learner_table_event,
            table_df=filtered_active_learners.reset_index(drop=True),
            multiselect_key=learner_multiselect_key,
            option_labels=learner_options,
        )

        select_col, clear_col = st.columns(2)
        with select_col:
            st.button(
                "Select all filtered learners",
                key="assignment_select_all_filtered",
                on_click=_select_all_filtered,
                args=(learner_multiselect_key, learner_options),
            )
        with clear_col:
            st.button(
                "Clear learner selection",
                key="assignment_clear_filtered",
                on_click=_clear_filtered_selection,
                args=(learner_multiselect_key,),
            )

        selected_module = st.selectbox("Module", list(module_map.keys()))
        selected_learners = st.multiselect("Learners", learner_options, key=learner_multiselect_key)
        enable_due_date = st.checkbox("Set due date", value=False)
        due_date = st.date_input("Due date", value=date.today(), disabled=not enable_due_date)

        if st.button("Send to database: Assign training", type="primary"):
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
                st.rerun()
            except Exception:
                view_logger.exception("Failed assigning training.", scenario_id=module_id)
                st.error("Could not assign training.")


def render_assignment_management(current_user: dict) -> None:
    st.subheader("Assignments")
    current_tab, tool_tab = st.tabs(["Current Assignments", "Assignment Tool"])
    with current_tab:
        render_current_assignments(current_user)
    with tool_tab:
        _render_assignment_tool(current_user)


def render_current_assignments(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    st.subheader("Current Assignments")

    assignments_df = _assignments_with_status(org_id)
    if assignments_df.empty:
        st.info("No assignments yet.")
        return

    assignments_df["team"] = assignments_df["team"].fillna("")
    assignments_df["organization_name"] = assignments_df["organization_name"].fillna("Unassigned")

    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        q = st.text_input("Search learner or module", key="current_assignments_search")
    with f2:
        team_options = sorted([team for team in assignments_df["team"].unique().tolist() if team])
        team_filter = st.selectbox("Team/Department", ["All"] + team_options, key="current_assignments_team")
    with f3:
        org_options = sorted(assignments_df["organization_name"].unique().tolist())
        org_filter = st.selectbox("Organization", ["All"] + org_options, key="current_assignments_org")
    with f4:
        status_options = sorted(assignments_df["status"].unique().tolist())
        status_filter = st.selectbox("Status", ["All"] + status_options, key="current_assignments_status")
    with f5:
        module_options = sorted(assignments_df["module_title"].unique().tolist())
        module_filter = st.selectbox("Module", ["All"] + module_options, key="current_assignments_module")

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

    st.caption(f"{len(filtered_assignments)} assignment(s) match current filters")
    assignment_table_df = filtered_assignments[
        ["assignment_id", "learner_name", "team", "organization_name", "module_title", "due_date", "status", "last_attempt_at"]
    ].reset_index(drop=True)
    assignment_table_event = st.dataframe(
        assignment_table_df,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        key="current_assignments_table",
    )

    assignment_records = filtered_assignments.reset_index(drop=True)
    assignment_map = {
        f"#{r['assignment_id']} • {r['learner_name']} • {r['module_title']} ({r['status']})": int(r["assignment_id"])
        for _, r in assignment_records.iterrows()
    }
    if not assignment_map:
        st.info("No assignments available for remove/reassign actions with current filters.")
        return

    selected_from_table = None
    selected_rows = assignment_table_event.selection.get("rows", []) if assignment_table_event else []
    if selected_rows:
        selected_idx = selected_rows[0]
        if 0 <= selected_idx < len(assignment_records):
            selected_row = assignment_records.iloc[selected_idx]
            selected_from_table = (
                f"#{selected_row['assignment_id']} • {selected_row['learner_name']} • "
                f"{selected_row['module_title']} ({selected_row['status']})"
            )
            st.caption(f"Selected from table: Assignment #{int(selected_row['assignment_id'])}")

    assignment_labels = list(assignment_map.keys())
    default_label = selected_from_table if selected_from_table in assignment_map else assignment_labels[0]
    selected_assignment_label = st.selectbox(
        "Select assignment",
        assignment_labels,
        index=assignment_labels.index(default_label),
    )
    selected_assignment_id = assignment_map[selected_assignment_label]

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Send to database: Remove assignment"):
            try:
                assignment_row = fetch_one(
                    """
                    SELECT module_id, learner_id
                    FROM assignments
                    WHERE assignment_id = ? AND organization_id = ?
                    """,
                    (selected_assignment_id, org_id),
                )
                execute("UPDATE assignments SET is_active = FALSE WHERE assignment_id = ? AND organization_id = ?", (selected_assignment_id, org_id))
                if assignment_row:
                    _cleanup_assignment_tracking_records(
                        organization_id=org_id,
                        module_id=int(assignment_row["module_id"]),
                        learner_id=int(assignment_row["learner_id"]),
                    )
                view_logger.info("Button click.", action="remove_assignment", assignment_id=selected_assignment_id)
                st.success("Assignment removed.")
                st.rerun()
            except Exception:
                view_logger.exception("Failed removing assignment.", assignment_id=selected_assignment_id)
                st.error("Could not remove assignment.")
    with c2:
        new_due = st.date_input("Reassign due date", key="reassign_due", value=date.today())
        if st.button("Send to database: Reassign training"):
            try:
                execute(
                    "UPDATE assignments SET due_date = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP WHERE assignment_id = ? AND organization_id = ?",
                    (new_due.isoformat(), current_user["user_id"], selected_assignment_id, org_id),
                )
                view_logger.info("Button click.", action="reassign_training", assignment_id=selected_assignment_id)
                st.success("Assignment updated.")
                st.rerun()
            except Exception:
                view_logger.exception("Failed reassigning training.", assignment_id=selected_assignment_id)
                st.error("Could not update assignment.")


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

    st.dataframe(
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
        hide_index=True,
        use_container_width=True,
    )

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
    st.dataframe(
        filtered[["learner_name", "module_title", "status", "due_date", "last_attempt_at"]],
        hide_index=True,
        use_container_width=True,
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
    st.subheader("Module Builder")
    st.caption("Build from admin input → AI draft preview → approve/deny scenario + questions → publish.")

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

    modules_df = to_df(fetch_all("SELECT * FROM modules WHERE organization_id = ? ORDER BY updated_at DESC", (org_id,)))
    if modules_df.empty:
        st.info("No modules yet.")
        return

    st.dataframe(modules_df[["module_id", "title", "status", "difficulty", "updated_at"]], hide_index=True, use_container_width=True)
    module_map = {f"#{int(r['module_id'])} • {r['title']} ({r['status']})": int(r["module_id"]) for _, r in modules_df.iterrows()}
    selected_label = st.selectbox("Select module", list(module_map.keys()))
    module_id = module_map[selected_label]
    module = fetch_one("SELECT * FROM modules WHERE module_id = ? AND organization_id = ?", (module_id, org_id))

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

    edit_nav_left, edit_nav_mid, edit_nav_right = st.columns([1, 1, 2])
    with edit_nav_left:
        if st.button("Previous", key=f"edit_module_previous_{module_id}", disabled=edit_step == 0):
            st.session_state[edit_step_key] = max(0, edit_step - 1)
            st.rerun()
    with edit_nav_mid:
        if edit_step < len(edit_steps) - 2:
            if st.button("Next", key=f"edit_module_next_{module_id}", disabled=not edit_step_valid):
                st.session_state[edit_step_key] = edit_step + 1
                st.rerun()
        elif edit_step == len(edit_steps) - 2:
            if st.button("Review", key=f"edit_module_review_{module_id}", disabled=not edit_step_valid):
                st.session_state[edit_step_key] = edit_step + 1
                st.rerun()
    with edit_nav_right:
        if edit_step == len(edit_steps) - 1:
            if st.button("Save Module", key=f"edit_module_save_{module_id}", type="primary", disabled=not edit_step_valid):
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

    st.markdown("##### Assessment questions")
    module_questions = fetch_all(
        "SELECT * FROM module_questions WHERE module_id = ? ORDER BY question_order",
        (module_id,),
    )
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
        if st.button("Send to database: Publish", disabled=module["status"] == "published"):
            execute("UPDATE modules SET status = 'published', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
            st.success("Module published.")
            st.rerun()
    with c2:
        if st.button("Send to database: Archive", disabled=module["status"] == "archived"):
            execute("UPDATE modules SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE module_id = ? AND organization_id = ?", (module_id, org_id))
            st.success("Module archived.")
            st.rerun()
    with c3:
        if st.button("Send to database: Duplicate"):
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



def _qa_assert_query_has_rows(name: str, query: str, params: tuple = ()) -> dict:
    try:
        rows = fetch_all(query, params)
    except Exception as exc:
        return {
            "name": name,
            "passed": False,
            "detail": f"Exception: {type(exc).__name__}: {exc}",
            "root_cause": "Query execution failed. Check database connectivity, table existence, and schema migrations.",
            "recommended_action": "Inspect the stack trace in app logs, validate DB credentials, then run migrations/seed scripts.",
            "category": "Data Integrity",
        }
    passed = len(rows) > 0
    detail = f"Rows returned: {len(rows)}"
    root_cause = "Healthy query with at least one row."
    recommended_action = "No action needed."
    if not passed:
        root_cause = "Query ran successfully but returned zero rows."
        recommended_action = (
            "This usually means the table is empty for the current environment. "
            "Seed test data or verify your QA fixtures are loaded."
        )
    return {
        "name": name,
        "passed": passed,
        "detail": detail,
        "root_cause": root_cause,
        "recommended_action": recommended_action,
        "category": "Data Integrity",
    }


def _qa_assert_scalar(
    name: str,
    query: str,
    params: tuple = (),
    minimum: int = 0,
    category: str = "Data Integrity",
) -> dict:
    row = fetch_one(query, params)
    value = int(row["count"]) if row and row.get("count") is not None else 0
    passed = value >= minimum
    return {
        "name": name,
        "passed": passed,
        "detail": f"Count={value} (expected >= {minimum})",
        "root_cause": "Threshold met." if passed else "Required minimum threshold not met for this environment.",
        "recommended_action": "No action needed." if passed else "Create or activate additional records required by this check.",
        "category": category,
    }


def _qa_assignment_lifecycle_test(current_user: dict) -> dict:
    org_id = current_user["organization_id"]
    admin_id = current_user["user_id"]

    learner = fetch_one(
        """
        SELECT user_id
        FROM users
        WHERE organization_id = ? AND role = 'learner' AND is_active = TRUE
        ORDER BY user_id
        LIMIT 1
        """,
        (org_id,),
    )
    module = fetch_one(
        """
        SELECT module_id
        FROM modules
        WHERE organization_id = ? AND status = 'published'
        ORDER BY module_id
        LIMIT 1
        """,
        (org_id,),
    )
    if not learner or not module:
        return {
            "name": "Assignment lifecycle (create → update → deactivate)",
            "passed": False,
            "detail": "Missing active learner or published module in organization.",
            "root_cause": "Workflow prerequisites were not present.",
            "recommended_action": "Create at least one active learner and one published module before running this QA test.",
            "category": "Workflow",
        }

    assignment_id = None
    try:
        due_date = (date.today() + timedelta(days=14)).isoformat()
        assignment_id = execute(
            """
            INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, is_active)
            VALUES (?, ?, ?, ?, ?, TRUE)
            """,
            (org_id, int(module["module_id"]), int(learner["user_id"]), admin_id, due_date),
        )
        created = fetch_one(
            "SELECT due_date, is_active FROM assignments WHERE assignment_id = ?",
            (assignment_id,),
        )
        updated_due_date = (date.today() + timedelta(days=21)).isoformat()
        execute(
            "UPDATE assignments SET due_date = ? WHERE assignment_id = ?",
            (updated_due_date, assignment_id),
        )
        updated = fetch_one(
            "SELECT due_date FROM assignments WHERE assignment_id = ?",
            (assignment_id,),
        )
        execute(
            "UPDATE assignments SET is_active = FALSE WHERE assignment_id = ?",
            (assignment_id,),
        )
        deactivated = fetch_one(
            "SELECT is_active FROM assignments WHERE assignment_id = ?",
            (assignment_id,),
        )

        passed = bool(created) and created["is_active"] and str(updated["due_date"]) == updated_due_date and not bool(deactivated["is_active"])
        return {
            "name": "Assignment lifecycle (create → update → deactivate)",
            "passed": passed,
            "detail": f"assignment_id={assignment_id}, initial_due={created['due_date'] if created else None}, updated_due={updated_due_date}",
            "root_cause": "Lifecycle operations completed." if passed else "One or more lifecycle assertions failed.",
            "recommended_action": "No action needed." if passed else "Verify INSERT/UPDATE permissions and assignment table constraints.",
            "category": "Workflow",
        }
    except Exception as exc:
        return {
            "name": "Assignment lifecycle (create → update → deactivate)",
            "passed": False,
            "detail": f"Exception: {type(exc).__name__}: {exc}",
            "root_cause": "Assignment lifecycle workflow raised an exception.",
            "recommended_action": "Inspect app/server logs for stack trace and validate assignments table schema.",
            "category": "Workflow",
        }
    finally:
        if assignment_id:
            execute("DELETE FROM assignments WHERE assignment_id = ?", (assignment_id,))


def _run_admin_qa_suite(current_user: dict) -> list[dict]:
    org_id = current_user["organization_id"]
    db_info = get_database_debug_info()
    connectivity_ok = bool(db_info.get("host") and db_info.get("database") and db_info.get("postgres_configured"))

    tests: list[dict] = [
        {"name": "Database URL is configured", "passed": db_info.get("database_url_set", False), "detail": f"backend={db_info.get('backend')}", "category": "Connectivity"},
        {"name": "Database connection metadata available", "passed": connectivity_ok, "detail": f"host={db_info.get('host')} db={db_info.get('database')}", "category": "Connectivity"},
        _qa_assert_query_has_rows("organizations table reachable", "SELECT organization_id FROM organizations LIMIT 1"),
        _qa_assert_query_has_rows("users table reachable", "SELECT user_id FROM users LIMIT 1"),
        _qa_assert_query_has_rows("modules table reachable", "SELECT module_id FROM modules LIMIT 1"),
        _qa_assert_query_has_rows("module_questions table reachable", "SELECT question_id FROM module_questions LIMIT 1"),
        _qa_assert_query_has_rows("investigation_actions table reachable", "SELECT action_id FROM investigation_actions LIMIT 1"),
        _qa_assert_query_has_rows("attempts table reachable", "SELECT attempt_id FROM attempts LIMIT 1"),
        _qa_assert_query_has_rows("action_logs table reachable", "SELECT log_id FROM action_logs LIMIT 1"),
        _qa_assert_query_has_rows("assignments table reachable", "SELECT assignment_id FROM assignments LIMIT 1"),
        _qa_assert_query_has_rows("learner_profiles table reachable", "SELECT id FROM learner_profiles LIMIT 1"),
        _qa_assert_query_has_rows("module_assignments table reachable", "SELECT id FROM module_assignments LIMIT 1"),
        _qa_assert_query_has_rows("module_progress table reachable", "SELECT id FROM module_progress LIMIT 1"),
        _qa_assert_query_has_rows("module_generation_runs table reachable", "SELECT run_id FROM module_generation_runs LIMIT 1"),
        _qa_assert_query_has_rows("module_generation_questions table reachable", "SELECT generated_question_id FROM module_generation_questions LIMIT 1"),
        _qa_assert_scalar(
            "Active learners exist",
            "SELECT COUNT(*) AS count FROM users WHERE role='learner' AND is_active=TRUE AND organization_id = ?",
            (org_id,),
            minimum=1,
            category="Readiness",
        ),
        _qa_assert_scalar(
            "Published modules exist",
            "SELECT COUNT(*) AS count FROM modules WHERE status='published' AND organization_id = ?",
            (org_id,),
            minimum=1,
            category="Readiness",
        ),
        _qa_assert_scalar(
            "Active assignments exist",
            "SELECT COUNT(*) AS count FROM assignments WHERE is_active=TRUE AND organization_id = ?",
            (org_id,),
            minimum=1,
            category="Readiness",
        ),
        {
            "name": "No orphan assignments (learner)",
            "passed": int(fetch_one("""
                SELECT COUNT(*) AS count
                FROM assignments a
                LEFT JOIN users u ON u.user_id = a.learner_id
                WHERE a.organization_id = ? AND a.is_active = TRUE AND u.user_id IS NULL
            """, (org_id,))["count"]) == 0,
            "detail": "Assignments all map to learners",
            "root_cause": "No orphan learner references detected.",
            "recommended_action": "No action needed.",
            "category": "Data Integrity",
        },
        {
            "name": "No orphan assignments (module)",
            "passed": int(fetch_one("""
                SELECT COUNT(*) AS count
                FROM assignments a
                LEFT JOIN modules m ON m.module_id = a.module_id
                WHERE a.organization_id = ? AND a.is_active = TRUE AND m.module_id IS NULL
            """, (org_id,))["count"]) == 0,
            "detail": "Assignments all map to modules",
            "root_cause": "No orphan module references detected.",
            "recommended_action": "No action needed.",
            "category": "Data Integrity",
        },
        {
            "name": "Assignment status query executes",
            "passed": not _assignments_with_status(org_id).empty,
            "detail": "_assignments_with_status returned records",
            "root_cause": "Status query returned at least one record.",
            "recommended_action": "No action needed.",
            "category": "Workflow",
        },
        _qa_assignment_lifecycle_test(current_user),
    ]

    return tests


def render_admin_quality_hub(current_user: dict) -> None:
    st.subheader("QA Test Center")
    st.caption("Admin-only automated regression checks for connectivity, assignments, and data integrity.")

    tab_overview, tab_results, tab_catalog = st.tabs(["Overview", "Run Tests", "Test Catalog"])

    with tab_overview:
        st.info("Use this suite before/after each update to catch regressions quickly.")
        st.markdown(
            """
            **What this validates automatically**
            - Database connectivity and table availability
            - Assignment workflow (create/update/deactivate)
            - Key data integrity checks (no orphan assignments)
            - Readiness checks for learners/modules/assignments
            """
        )

    with tab_results:
        if st.button("Run full QA suite", type="primary", use_container_width=True):
            st.session_state["qa_suite_results"] = _run_admin_qa_suite(current_user)
            st.session_state["qa_last_run"] = date.today().isoformat()

        results = st.session_state.get("qa_suite_results", [])
        if not results:
            st.caption("No QA run yet in this session. Click **Run full QA suite**.")
        else:
            results_df = pd.DataFrame(results)
            for col in ["root_cause", "recommended_action"]:
                if col not in results_df.columns:
                    results_df[col] = ""
            results_df["root_cause"] = results_df["root_cause"].fillna("")
            results_df["recommended_action"] = results_df["recommended_action"].fillna("")
            results_df["status"] = results_df["passed"].map({True: "PASS", False: "FAIL"})
            pass_count = int(results_df["passed"].sum())
            fail_count = int((~results_df["passed"]).sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Total tests", len(results_df))
            c2.metric("Passed", pass_count)
            c3.metric("Failed", fail_count)
            st.dataframe(
                results_df[["status", "category", "name", "detail"]],
                hide_index=True,
                use_container_width=True,
            )

            failed = results_df[results_df["passed"] == False]
            if not failed.empty:
                st.error("Some checks failed. Review details before shipping updates.")
                st.table(failed[["name", "detail"]])
                with st.expander("Failure logs (root cause + recommended action)", expanded=True):
                    st.dataframe(
                        failed[["category", "name", "detail", "root_cause", "recommended_action"]],
                        hide_index=True,
                        use_container_width=True,
                    )
            else:
                st.success("All QA checks passed for this run.")

    with tab_catalog:
        catalog = [
            "1. Database URL is configured",
            "2. Database metadata is available",
            "3. organizations table reachable",
            "4. users table reachable",
            "5. modules table reachable",
            "6. module_questions table reachable",
            "7. investigation_actions table reachable",
            "8. attempts table reachable",
            "9. action_logs table reachable",
            "10. assignments table reachable",
            "11. learner_profiles table reachable",
            "12. module_assignments table reachable",
            "13. module_progress table reachable",
            "14. module_generation_runs table reachable",
            "15. module_generation_questions table reachable",
            "16. Active learners exist",
            "17. Published modules exist",
            "18. Active assignments exist",
            "19. No orphan assignments (learner)",
            "20. No orphan assignments (module)",
            "21. Assignment status query executes",
            "22. Assignment lifecycle (create → update → deactivate)",
        ]
        st.markdown("\n".join([f"- {item}" for item in catalog]))


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

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


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
