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
from utils import metric_row, to_df

admin_logger = get_logger(module="admin_views")


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
            u.is_active AS learner_active,
            m.title AS module_title,
            CASE
                WHEN x.last_attempt_at IS NOT NULL THEN 'Completed'
                WHEN a.due_date IS NOT NULL AND DATE(a.due_date) < DATE('now') THEN 'Overdue'
                WHEN x.attempt_count > 0 THEN 'In Progress'
                ELSE 'Not Started'
            END AS status,
            x.attempt_count,
            x.last_attempt_at
        FROM assignments a
        JOIN users u ON u.user_id = a.learner_id
        JOIN modules m ON m.module_id = a.module_id
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
               AND DATETIME(t.created_at) >= DATETIME(a2.assigned_at)
            WHERE a2.organization_id = ?
              AND a2.is_active = 1
            GROUP BY a2.assignment_id
        ) x ON x.assignment_id = a.assignment_id
        WHERE a.organization_id = ? AND a.is_active = 1
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
        SELECT
            u.user_id,
            u.name,
            u.team,
            u.is_active,
            COUNT(DISTINCT a.assignment_id) AS assigned_modules,
            COUNT(DISTINCT CASE WHEN x.last_attempt_at IS NOT NULL THEN a.module_id END) AS completed_modules,
            MAX(x.last_attempt_at) AS last_activity
        FROM users u
        LEFT JOIN assignments a ON a.learner_id = u.user_id AND a.is_active = TRUE
        LEFT JOIN (
            SELECT user_id, module_id, MAX(created_at) AS last_attempt_at
            FROM attempts
            WHERE organization_id = ?
            GROUP BY user_id, module_id
        ) x ON x.user_id = u.user_id AND x.module_id = a.module_id
        WHERE u.role = 'learner' AND u.organization_id = ?
        GROUP BY u.user_id, u.name, u.team, u.is_active
        ORDER BY u.name
        """,
        (org_id, org_id),
    )
    df = to_df(rows)

    if df.empty:
        st.info("No learners in this organization.")
        return

    q = st.text_input("Search learners")
    status_filter = st.selectbox("Status", ["All", "Active", "Inactive"])
    filtered = df.copy()
    if q:
        filtered = filtered[filtered["name"].str.contains(q, case=False) | filtered["team"].str.contains(q, case=False)]
    if status_filter != "All":
        target = 1 if status_filter == "Active" else 0
        filtered = filtered[filtered["is_active"] == target]

    filtered["status"] = filtered["is_active"].map({1: "Active", 0: "Inactive"})
    st.dataframe(
        filtered[["name", "team", "status", "assigned_modules", "completed_modules", "last_activity"]],
        hide_index=True,
        use_container_width=True,
    )

    learner_options = {f"{r['name']} ({'Active' if r['is_active'] else 'Inactive'})": int(r["user_id"]) for _, r in df.iterrows()}
    selected_label = st.selectbox("Select learner", list(learner_options.keys()))
    learner_id = learner_options[selected_label]
    learner = fetch_one("SELECT user_id, name, is_active FROM users WHERE user_id = ? AND organization_id = ?", (learner_id, org_id))

    c1, c2 = st.columns(2)
    with c1:
        if learner["is_active"]:
            if st.button("Send to database: Deactivate learner", type="secondary"):
                try:
                    execute("UPDATE users SET is_active = 0 WHERE user_id = ? AND organization_id = ?", (learner_id, org_id))
                    view_logger.info("Button click.", action="deactivate_learner", learner_id=learner_id)
                    st.success(f"{learner['name']} deactivated.")
                    st.rerun()
                except Exception:
                    view_logger.exception("Failed deactivating learner.", learner_id=learner_id)
                    st.error("Could not deactivate learner.")
        else:
            if st.button("Send to database: Activate learner", type="primary"):
                try:
                    execute("UPDATE users SET is_active = 1 WHERE user_id = ? AND organization_id = ?", (learner_id, org_id))
                    view_logger.info("Button click.", action="activate_learner", learner_id=learner_id)
                    st.success(f"{learner['name']} activated.")
                    st.rerun()
                except Exception:
                    view_logger.exception("Failed activating learner.", learner_id=learner_id)
                    st.error("Could not activate learner.")


def render_assignment_management(current_user: dict) -> None:
    org_id = current_user["organization_id"]
    view_logger = admin_logger.bind(user_id=current_user.get("user_id"), session_id=st.session_state.get("session_id"))
    st.subheader("Assignment Management")

    learners = fetch_all("SELECT user_id, name, is_active FROM users WHERE role='learner' AND organization_id=? ORDER BY name", (org_id,))
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
        learner_map = {f"{l['name']} ({'Active' if l['is_active'] else 'Inactive'})": int(l["user_id"]) for l in learners}

        selected_module = st.selectbox("Module", list(module_map.keys()))
        selected_learners = st.multiselect("Learners", list(learner_map.keys()))
        enable_due_date = st.checkbox("Set due date", value=False)
        due_date = st.date_input("Due date", value=date.today(), disabled=not enable_due_date)

        if st.button("Send to database: Assign training", type="primary"):
            module_id = module_map[selected_module]
            due_date_value = due_date.isoformat() if enable_due_date else None
            if not selected_learners:
                st.warning("Select at least one learner before assigning.")
                return
            try:
                for learner_label in selected_learners:
                    learner_id = learner_map[learner_label]
                    execute(
                        """
                        INSERT INTO assignments (organization_id, module_id, learner_id, assigned_by, due_date, is_active)
                        VALUES (?, ?, ?, ?, ?, 1)
                        """,
                        (org_id, module_id, learner_id, current_user["user_id"], due_date_value),
                    )
                view_logger.info("Form submitted.", form="assign_training", scenario_id=module_id, learners=len(selected_learners))
                st.success(f"Assigned module to {len(selected_learners)} learner(s).")
                st.rerun()
            except Exception:
                view_logger.exception("Failed assigning training.", scenario_id=module_id)
                st.error("Could not assign training.")

    assignments_df = _assignments_with_status(org_id)
    if assignments_df.empty:
        st.info("No assignments yet.")
        return

    st.markdown("#### Current assignments")
    st.dataframe(
        assignments_df[["assignment_id", "learner_name", "module_title", "due_date", "status", "last_attempt_at"]],
        hide_index=True,
        use_container_width=True,
    )

    assignment_map = {
        f"#{r['assignment_id']} • {r['learner_name']} • {r['module_title']} ({r['status']})": int(r["assignment_id"])
        for _, r in assignments_df.iterrows()
    }
    selected_assignment_label = st.selectbox("Select assignment", list(assignment_map.keys()))
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
                    1 if quiz_required else 0,
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
