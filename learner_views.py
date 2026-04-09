from __future__ import annotations

from typing import Dict

import altair as alt
import streamlit as st

from db import fetch_all, fetch_one, insert_attempt, log_actions
from evaluation import evaluate_submission
from utils import metric_row, parse_json_list, to_df


def _assigned_modules(user: Dict):
    return fetch_all(
        """
        SELECT
            a.assignment_id,
            a.module_id,
            a.due_date,
            a.assigned_at,
            m.title,
            m.category,
            m.difficulty,
            m.estimated_time,
            m.description,
            COALESCE(x.best_score, 0) AS best_score,
            CASE
                WHEN x.last_attempt_at IS NOT NULL THEN 'Completed'
                WHEN a.due_date IS NOT NULL AND DATE(a.due_date) < DATE('now') THEN 'Overdue'
                WHEN x.attempt_count > 0 THEN 'In Progress'
                ELSE 'Not Started'
            END AS status,
            x.attempt_count,
            x.last_attempt_at
        FROM assignments a
        JOIN modules m ON m.module_id = a.module_id
        LEFT JOIN (
            SELECT
                a2.assignment_id,
                COUNT(t.attempt_id) AS attempt_count,
                MAX(t.created_at) AS last_attempt_at,
                MAX(t.total_score) AS best_score
            FROM assignments a2
            LEFT JOIN attempts t
                ON t.user_id = a2.learner_id
               AND t.module_id = a2.module_id
               AND t.organization_id = a2.organization_id
               AND DATETIME(t.created_at) >= DATETIME(a2.assigned_at)
            WHERE a2.learner_id = ?
              AND a2.organization_id = ?
              AND a2.is_active = 1
            GROUP BY a2.assignment_id
        ) x ON x.assignment_id = a.assignment_id
        WHERE a.learner_id = ?
          AND a.organization_id = ?
          AND a.is_active = 1
          AND m.status = 'published'
        ORDER BY a.assigned_at DESC
        """,
        (user["user_id"], user["organization_id"], user["user_id"], user["organization_id"]),
    )


def _learner_stats(user: Dict) -> Dict:
    attempts = fetch_all(
        """
        SELECT a.*, m.title FROM attempts a
        JOIN modules m ON a.module_id = m.module_id
        WHERE a.user_id = ?
        ORDER BY a.created_at DESC
        """,
        (user["user_id"],),
    )
    assigned_modules = _assigned_modules(user)
    completed_module_ids = {a["module_id"] for a in attempts}
    avg_score = round(sum(a["total_score"] for a in attempts) / len(attempts), 1) if attempts else 0
    return {
        "attempts": attempts,
        "assigned_count": len(assigned_modules),
        "completed_count": len(completed_module_ids),
        "avg_score": avg_score,
        "assigned_modules": assigned_modules,
    }


def render_learner_home(user: Dict) -> None:
    st.subheader("Welcome back")
    st.caption("Complete assigned troubleshooting scenarios, submit your work, and review your coaching feedback.")

    stats = _learner_stats(user)
    recent_feedback = stats["attempts"][0]["ai_feedback"] if stats["attempts"] else "No feedback yet. Complete your first module to begin."

    metric_row(
        {
            "Assigned modules": stats["assigned_count"],
            "Completed": stats["completed_count"],
            "Average score": f"{stats['avg_score']}%",
        }
    )

    completion_ratio = (stats["completed_count"] / stats["assigned_count"]) if stats["assigned_count"] else 0
    st.progress(completion_ratio, text=f"Assigned progress: {int(completion_ratio * 100)}%")

    with st.container(border=True):
        st.markdown("#### Recent feedback")
        st.write(recent_feedback)


def render_module_library(user: Dict) -> None:
    st.subheader("Assigned Modules")
    assignments = _assigned_modules(user)

    if not assignments:
        st.info("No modules are assigned yet. Your admin can assign training from the admin dashboard.")
        return

    for i in range(0, len(assignments), 2):
        cols = st.columns(2)
        for col, module in zip(cols, assignments[i : i + 2]):
            with col:
                with st.container(border=True):
                    st.markdown(f"### {module['title']}")
                    st.caption(
                        f"{module['category']} • {module['difficulty']} • {module['estimated_time']} • Status: {module['status']}"
                    )
                    if module["due_date"]:
                        st.caption(f"Due: {module['due_date']}")
                    st.write(module["description"])
                    if module["status"] == "Completed":
                        st.success(f"Completed • Best score: {module['best_score']}%")
                        if st.button(
                            "View score",
                            key=f"view_{module['assignment_id']}_{module['module_id']}",
                            type="secondary",
                        ):
                            attempt = fetch_one(
                                """
                                SELECT attempt_id
                                FROM attempts
                                WHERE user_id = ?
                                  AND module_id = ?
                                  AND organization_id = ?
                                  AND DATETIME(created_at) >= DATETIME(?)
                                ORDER BY created_at DESC
                                LIMIT 1
                                """,
                                (user["user_id"], module["module_id"], user["organization_id"], module["assigned_at"]),
                            )
                            if attempt:
                                st.session_state.latest_attempt_id = int(attempt["attempt_id"])
                                st.session_state.page = "Results"
                                st.rerun()
                    else:
                        if st.button("Start module", key=f"start_{module['assignment_id']}_{module['module_id']}", type="primary"):
                            st.session_state.active_module_id = module["module_id"]
                            st.session_state.page = "Scenario"
                            st.rerun()

    completed_modules = [module for module in assignments if module["status"] == "Completed"]
    st.markdown("### Completed modules")
    if not completed_modules:
        st.caption("No completed modules yet.")
    else:
        st.dataframe(
            [
                {
                    "Module": module["title"],
                    "Completed at": module["last_attempt_at"],
                    "Score": f"{module['best_score']}%",
                }
                for module in completed_modules
            ],
            use_container_width=True,
            hide_index=True,
        )


def render_scenario_page(user: Dict) -> None:
    module_id = st.session_state.get("active_module_id")
    if not module_id:
        st.info("Select a module from Assigned Modules to begin.")
        return

    assignment = fetch_one(
        """
        SELECT assignment_id, assigned_at
        FROM assignments
        WHERE learner_id = ? AND module_id = ? AND organization_id = ? AND is_active = 1
        """,
        (user["user_id"], module_id, user["organization_id"]),
    )
    if not assignment:
        st.warning("This module is not currently assigned to you.")
        return

    existing_attempt = fetch_one(
        """
        SELECT attempt_id, total_score
        FROM attempts
        WHERE user_id = ?
          AND module_id = ?
          AND organization_id = ?
          AND DATETIME(created_at) >= DATETIME(?)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user["user_id"], module_id, user["organization_id"], assignment["assigned_at"]),
    )
    if existing_attempt:
        st.success(f"You've already completed this module. Score: {existing_attempt['total_score']}%")
        st.info("This assignment allows one graded submission. If reassigned by your admin, you can attempt it again.")
        if st.button("View completed results", type="secondary"):
            st.session_state.latest_attempt_id = int(existing_attempt["attempt_id"])
            st.session_state.page = "Results"
            st.rerun()
        return

    module = fetch_one(
        "SELECT * FROM modules WHERE module_id = ? AND organization_id = ?",
        (module_id, user["organization_id"]),
    )
    actions = fetch_all("SELECT * FROM investigation_actions WHERE module_id = ?", (module_id,))

    st.subheader(module["title"])
    st.caption(f"Difficulty: {module['difficulty']} • Estimated time: {module['estimated_time']}")

    with st.container(border=True):
        st.markdown("**Ticket / Issue Description**")
        st.write(module["scenario_ticket"])
        st.markdown("**Context Notes**")
        st.write(module["scenario_context"])

    st.markdown("### Investigation Panel")
    used_actions_key = f"used_actions_{module_id}"
    revealed_key = f"revealed_{module_id}"
    st.session_state.setdefault(used_actions_key, [])
    st.session_state.setdefault(revealed_key, {})

    cols = st.columns(3)
    for idx, action in enumerate(actions):
        with cols[idx % 3]:
            if st.button(action["action_name"], key=f"action_{module_id}_{action['action_id']}"):
                if action["action_name"] not in st.session_state[used_actions_key]:
                    st.session_state[used_actions_key].append(action["action_name"])
                st.session_state[revealed_key][action["action_name"]] = action["revealed_information"]

    if st.session_state[revealed_key]:
        for name, details in st.session_state[revealed_key].items():
            with st.expander(name, expanded=True):
                st.write(details)

    notes = st.text_area("Personal notes", key=f"notes_{module_id}", height=100)
    diagnosis = st.text_area("Diagnosis", key=f"diagnosis_{module_id}", height=100)
    next_steps = st.text_area("Next steps", key=f"next_steps_{module_id}", height=120)
    customer_response = st.text_area("Customer response", key=f"customer_{module_id}", height=120)
    escalation_choice = st.selectbox("Escalation decision", ["No escalation", "Escalate to Engineering", "Escalate to Security", "Escalate to Product"])

    if st.button("Submit module", type="primary"):
        answers = {
            "diagnosis_answer": diagnosis,
            "next_steps_answer": next_steps,
            "customer_response": customer_response,
            "escalation_choice": escalation_choice,
            "notes": notes,
        }
        evaluation = evaluate_submission(dict(module), answers, st.session_state[used_actions_key])
        payload = {**answers, **evaluation}
        attempt_id = insert_attempt(user["user_id"], module_id, payload, user["organization_id"])
        log_actions(attempt_id, st.session_state[used_actions_key])

        st.session_state.latest_attempt_id = attempt_id
        st.session_state.page = "Results"
        st.toast("🎉 Thank you — you've completed this module!")
        st.rerun()


def render_results_page(user: Dict) -> None:
    attempt_id = st.session_state.get("latest_attempt_id")
    if not attempt_id:
        st.info("Submit a scenario to view results.")
        return

    attempt = fetch_one(
        """
        SELECT a.*, m.title, m.lesson_takeaway, m.expected_customer_response
        FROM attempts a
        JOIN modules m ON a.module_id = m.module_id
        WHERE attempt_id = ? AND a.organization_id = ?
        """,
        (attempt_id, user["organization_id"]),
    )
    if not attempt:
        st.warning("Result not found.")
        return

    st.subheader(f"Results • {attempt['title']}")
    metric_row(
        {
            "Total score": f"{attempt['total_score']}%",
            "Understanding": f"{attempt['understanding_score']}%",
            "Investigation": f"{attempt['investigation_score']}%",
            "Solution quality": f"{attempt['solution_score']}%",
            "Communication": f"{attempt['communication_score']}%",
        }
    )

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.markdown("#### What you did well")
            for item in parse_json_list(attempt["strengths"]):
                st.write(f"- {item}")
    with col2:
        with st.container(border=True):
            st.markdown("#### What you missed")
            for item in parse_json_list(attempt["missed_points"]):
                st.write(f"- {item}")

    with st.container(border=True):
        st.markdown("#### Best-practice reasoning")
        st.write(attempt["best_practice_reasoning"])
        st.markdown("#### Recommended response")
        st.write(attempt["recommended_response"])
        st.markdown("#### Lesson takeaway")
        st.write(attempt["takeaway_summary"] or attempt["lesson_takeaway"])

    c1, c2 = st.columns(2)
    with c1:
        st.success("✅ Thank you! You've completed this module.")
    if c2.button("Back to assignments"):
        st.session_state.page = "Assigned Modules"
        st.rerun()


def render_progress_page(user: Dict) -> None:
    attempts = fetch_all(
        """
        SELECT a.*, m.title FROM attempts a
        JOIN modules m ON a.module_id = m.module_id
        WHERE a.user_id = ?
        ORDER BY a.created_at
        """,
        (user["user_id"],),
    )

    st.subheader("My Progress")
    if not attempts:
        st.info("No attempts yet. Start an assigned module to begin tracking progress.")
        return

    df = to_df(attempts)
    metric_row(
        {
            "Modules completed": df["module_id"].nunique(),
            "Average score": f"{round(df['total_score'].mean(), 1)}%",
            "Recent score": f"{df['total_score'].iloc[-1]}%",
        }
    )

    trend = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(x="created_at:T", y="total_score:Q", tooltip=["title", "total_score"])
        .properties(height=260)
    )
    st.altair_chart(trend, use_container_width=True)

    strengths = []
    misses = []
    for _, row in df.tail(5).iterrows():
        strengths.extend(parse_json_list(row["strengths"]))
        misses.extend(parse_json_list(row["missed_points"]))

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.markdown("#### Emerging strengths")
            for item in list(dict.fromkeys(strengths))[:5]:
                st.write(f"- {item}")
    with col2:
        with st.container(border=True):
            st.markdown("#### Weakest areas")
            for item in list(dict.fromkeys(misses))[:5]:
                st.write(f"- {item}")

    st.markdown("#### Recent attempts")
    st.dataframe(
        df[["created_at", "title", "total_score", "understanding_score", "investigation_score", "solution_score", "communication_score"]]
        .sort_values("created_at", ascending=False)
        .head(10),
        use_container_width=True,
        hide_index=True,
    )
