from __future__ import annotations

from datetime import date

import pandas as pd
import psycopg2
import streamlit as st

from db import execute, fetch_all, fetch_one, fetch_table_rows, list_public_tables
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
                execute("UPDATE assignments SET is_active = 0 WHERE assignment_id = ? AND organization_id = ?", (selected_assignment_id, org_id))
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
                a.total_score,
                a.understanding_score,
                a.investigation_score,
                a.solution_score,
                a.communication_score,
                a.ai_feedback
            FROM attempts a
            JOIN users u ON u.user_id = a.user_id
            JOIN modules m ON m.module_id = a.module_id
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


def render_module_builder(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    st.subheader("Module Builder")

    st.markdown("#### Create module")
    with st.form("create_module"):
        title = st.text_input("Title")
        category = st.text_input("Category", value="General")
        difficulty = st.selectbox("Difficulty", ["Beginner", "Intermediate", "Advanced"])
        description = st.text_area("Description")
        learning_objectives = st.text_area("Learning objectives (one per line)")
        content_sections = st.text_area("Ordered content sections (one per line)")
        completion_requirements = st.text_area("Completion requirements")
        quiz_required = st.checkbox("Quiz required")
        submit = st.form_submit_button("Send to database: Create module", type="primary")
        if submit and title:
            execute(
                """
                INSERT INTO modules (
                    title, category, difficulty, description, estimated_time,
                    organization_id, status, learning_objectives, content_sections,
                    completion_requirements, quiz_required, created_by, updated_at
                ) VALUES (?, ?, ?, ?, '20 min', ?, 'draft', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    title,
                    category,
                    difficulty,
                    description,
                    org_id,
                    _parse_lines(learning_objectives),
                    _parse_lines(content_sections),
                    completion_requirements,
                    True if quiz_required else False,
                    current_user["user_id"],
                ),
            )
            st.success("Module created as draft.")
            st.rerun()

    modules_df = to_df(fetch_all("SELECT * FROM modules WHERE organization_id = ? ORDER BY updated_at DESC", (org_id,)))
    if modules_df.empty:
        st.info("No modules yet.")
        return

    st.markdown("#### Manage modules")
    st.dataframe(modules_df[["module_id", "title", "status", "difficulty", "updated_at"]], hide_index=True, use_container_width=True)
    module_map = {f"#{int(r['module_id'])} • {r['title']} ({r['status']})": int(r["module_id"]) for _, r in modules_df.iterrows()}
    selected_label = st.selectbox("Select module", list(module_map.keys()))
    module_id = module_map[selected_label]
    module = fetch_one("SELECT * FROM modules WHERE module_id = ? AND organization_id = ?", (module_id, org_id))

    with st.form("edit_module"):
        edit_title = st.text_input("Title", value=module["title"])
        edit_description = st.text_area("Description", value=module["description"] or "")
        edit_objectives = st.text_area("Learning objectives", value=module["learning_objectives"] or "")
        edit_sections = st.text_area("Ordered content sections", value=module["content_sections"] or "")
        edit_requirements = st.text_area("Completion requirements", value=module["completion_requirements"] or "")
        edit_quiz_required = st.checkbox("Quiz required", value=bool(module["quiz_required"]))
        save = st.form_submit_button("Send to database: Save edits")
        if save:
            execute(
                """
                UPDATE modules
                SET title = ?, description = ?, learning_objectives = ?, content_sections = ?,
                    completion_requirements = ?, quiz_required = ?, updated_at = CURRENT_TIMESTAMP
                WHERE module_id = ? AND organization_id = ?
                """,
                (
                    edit_title,
                    edit_description,
                    _parse_lines(edit_objectives),
                    _parse_lines(edit_sections),
                    edit_requirements,
                    1 if edit_quiz_required else 0,
                    module_id,
                    org_id,
                ),
            )
            st.success("Module updated.")
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
