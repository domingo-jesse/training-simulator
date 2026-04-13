from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Dict

import altair as alt
import streamlit as st

from db import execute, fetch_all, fetch_one, insert_attempt, log_actions
from evaluation import evaluate_submission
from logger import get_logger
from utils import metric_row, parse_json_list, to_df

learner_logger = get_logger(module="learner_views")
WIZARD_STEPS = [
    "Scenario Overview",
    "Investigation / Notes",
    "Assessment Questions",
    "Final Response / Decision",
    "Review and Submit",
    "Results / Feedback",
]


def _compact_text(value: str) -> str:
    return " ".join((value or "").split())


def _build_scenario_overview(module: Dict) -> str:
    parts = [
        _compact_text(module.get("description", "")),
        _compact_text(module.get("scenario_ticket", "")),
        _compact_text(module.get("scenario_context", "")),
    ]
    return " ".join(part for part in parts if part)


def _estimated_minutes(value: str | None, fallback: int = 20) -> int:
    match = re.search(r"(\d+)", value or "")
    if not match:
        return fallback
    return max(1, int(match.group(1)))


def _question_options(options_text: str | None) -> list[str]:
    return [line.strip() for line in (options_text or "").splitlines() if line.strip()]


def _format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "N/A"
    safe_seconds = max(0, int(seconds))
    minutes = safe_seconds // 60
    remainder = safe_seconds % 60
    return f"{minutes}m {remainder:02d}s"


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
                WHEN a.due_date IS NOT NULL AND a.due_date::date < CURRENT_DATE THEN 'Overdue'
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
               AND t.created_at >= a2.assigned_at
            WHERE a2.learner_id = ?
              AND a2.organization_id = ?
              AND a2.is_active = TRUE
            GROUP BY a2.assignment_id
        ) x ON x.assignment_id = a.assignment_id
        WHERE a.learner_id = ?
          AND a.organization_id = ?
          AND a.is_active = TRUE
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
    view_logger = learner_logger.bind(user_id=user.get("user_id"), session_id=st.session_state.get("session_id"))
    in_workspace_mode = st.session_state.get("learner_page") == "Module Workspace"
    st.subheader("Module Workspace" if in_workspace_mode else "Assigned Modules")
    try:
        assignments = _assigned_modules(user)
    except Exception:
        view_logger.exception("Failed loading assigned modules.")
        st.error("We couldn't load your assignments right now. Please try again.")
        return

    if not assignments:
        st.info("No modules are assigned yet. Your admin can assign training from the admin dashboard.")
        return

    @st.dialog("Ready to start this module?")
    def _start_module_warning_dialog(module: Dict) -> None:
        st.warning("You have one graded attempt for this assignment. Make sure you're ready before starting.")
        st.write(f"Module: **{module['title']}**")
        c1, c2 = st.columns(2)
        if c1.button("Cancel", key=f"cancel_start_{module['assignment_id']}_{module['module_id']}"):
            st.session_state.pending_start_module = None
            st.rerun()
        if c2.button("Yes, start now", key=f"confirm_start_{module['assignment_id']}_{module['module_id']}", type="primary"):
            view_logger.info("Start module confirmed.", action="start_module_confirmed", scenario_id=module["module_id"])
            st.session_state.active_assignment_id = int(module["assignment_id"])
            st.session_state.learner_page = "Module Workspace"
            st.query_params["page"] = "module_workspace"
            st.query_params["assignment_id"] = str(module["assignment_id"])
            st.session_state.pending_start_module = None
            st.rerun()

    pending_start = st.session_state.get("pending_start_module")
    if pending_start:
        _start_module_warning_dialog(pending_start)

    active_assignment_id = st.session_state.get("active_assignment_id")
    if active_assignment_id:
        if in_workspace_mode:
            st.caption("You're in your personal module workspace for the selected assignment.")
            c1, c2 = st.columns([3, 1])
            with c2:
                if st.button("Exit workspace", use_container_width=True):
                    st.session_state.active_assignment_id = None
                    st.session_state.learner_page = "Assigned Modules"
                    st.query_params["page"] = "assigned-modules"
                    st.query_params.pop("assignment_id", None)
                    st.rerun()
            render_scenario_page(user)
            return
        st.info("You already have an active module workspace. Open **Module Workspace** from the sidebar to continue.")
    elif in_workspace_mode:
        st.info("No active module workspace yet. Start a module from **Assigned Modules**.")
        if st.button("Go to assigned modules", type="primary"):
            st.session_state.page = "Assigned Modules"
            st.rerun()
        return

    assigned_modules = [module for module in assignments if module["status"] != "Completed"]
    completed_modules = [module for module in assignments if module["status"] == "Completed"]
    tab_assigned, tab_completed = st.tabs(["Assigned", "Completed"])

    with tab_assigned:
        if not assigned_modules:
            st.caption("No active assigned modules right now.")
        for i in range(0, len(assigned_modules), 2):
            cols = st.columns(2)
            for col, module in zip(cols, assigned_modules[i : i + 2]):
                with col:
                    with st.container(border=True):
                        st.markdown(f"### {module['title']}")
                        st.caption(
                            f"{module['category']} • {module['difficulty']} • {module['estimated_time']} • Status: {module['status']}"
                        )
                        if module["due_date"]:
                            st.caption(f"Due: {module['due_date']}")
                        st.write(_compact_text(module["description"]))
                        if st.button("Start module", key=f"start_{module['assignment_id']}_{module['module_id']}", type="primary"):
                            view_logger.info("Button click.", action="start_module_clicked", scenario_id=module["module_id"])
                            st.session_state.pending_start_module = dict(module)
                            st.rerun()

    with tab_completed:
        if not completed_modules:
            st.caption("No completed modules yet.")
        for i in range(0, len(completed_modules), 2):
            cols = st.columns(2)
            for col, module in zip(cols, completed_modules[i : i + 2]):
                with col:
                    with st.container(border=True):
                        st.markdown(f"### {module['title']}")
                        st.caption(
                            f"{module['category']} • {module['difficulty']} • Completed at: {module['last_attempt_at']}"
                        )
                        st.success(f"Completed • Best score: {module['best_score']}%")
                        if st.button(
                            "View completed module",
                            key=f"view_{module['assignment_id']}_{module['module_id']}",
                            type="secondary",
                        ):
                            view_logger.info("Button click.", action="view_score", scenario_id=module["module_id"])
                            attempt = fetch_one(
                                """
                                SELECT attempt_id
                                FROM attempts
                                WHERE user_id = ?
                                  AND module_id = ?
                                  AND organization_id = ?
                                  AND created_at >= ?
                                ORDER BY created_at DESC
                                LIMIT 1
                                """,
                                (user["user_id"], module["module_id"], user["organization_id"], module["assigned_at"]),
                            )
                            if attempt:
                                st.session_state.latest_attempt_id = int(attempt["attempt_id"])
                                st.session_state.page = "Results"
                                st.rerun()



def _load_or_create_workspace_state(*, assignment_id: int, module_id: int, user: Dict) -> dict:
    state = fetch_one(
        """
        SELECT *
        FROM assignment_workspace_state
        WHERE assignment_id = ?
          AND organization_id = ?
          AND module_id = ?
          AND user_id = ?
        """,
        (assignment_id, user["organization_id"], module_id, user["user_id"]),
    )
    if state:
        return dict(state)

    started_at = datetime.now(timezone.utc).isoformat()
    execute(
        """
        INSERT INTO assignment_workspace_state (
            assignment_id,
            organization_id,
            module_id,
            user_id,
            current_step,
            progress_status,
            learner_notes,
            diagnosis_response,
            next_steps_response,
            customer_response,
            escalation_choice,
            question_responses,
            revealed_actions,
            used_actions,
            submitted_state,
            started_at,
            created_at,
            updated_at,
            last_saved_at
        ) VALUES (?, ?, ?, ?, 1, 'not_started', '', '', '', '', 'No escalation', '{}', '{}', '[]', 0, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (assignment_id, user["organization_id"], module_id, user["user_id"], started_at),
    )
    return fetch_one(
        """
        SELECT *
        FROM assignment_workspace_state
        WHERE assignment_id = ?
          AND organization_id = ?
          AND module_id = ?
          AND user_id = ?
        """,
        (assignment_id, user["organization_id"], module_id, user["user_id"]),
    ) or {}


def _persist_workspace_state(*, assignment_id: int, module_id: int, user: Dict) -> None:
    execute(
        """
        UPDATE assignment_workspace_state
        SET
            current_step = ?,
            progress_status = ?,
            learner_notes = ?,
            diagnosis_response = ?,
            next_steps_response = ?,
            customer_response = ?,
            escalation_choice = ?,
            question_responses = ?,
            revealed_actions = ?,
            used_actions = ?,
            submitted_state = ?,
            updated_at = CURRENT_TIMESTAMP,
            last_saved_at = CURRENT_TIMESTAMP
        WHERE assignment_id = ?
          AND organization_id = ?
          AND module_id = ?
          AND user_id = ?
        """,
        (
            int(st.session_state.get(f"wizard_step_{assignment_id}", 1)),
            "submitted" if st.session_state.get(f"submitted_{assignment_id}") else "in_progress",
            st.session_state.get(f"notes_{assignment_id}", ""),
            st.session_state.get(f"diagnosis_{assignment_id}", ""),
            st.session_state.get(f"next_steps_{assignment_id}", ""),
            st.session_state.get(f"customer_{assignment_id}", ""),
            st.session_state.get(f"escalation_{assignment_id}", "No escalation"),
            json.dumps(st.session_state.get(f"question_answers_{assignment_id}", {})),
            json.dumps(st.session_state.get(f"revealed_{assignment_id}", {})),
            json.dumps(st.session_state.get(f"used_actions_{assignment_id}", [])),
            int(bool(st.session_state.get(f"submitted_{assignment_id}"))),
            assignment_id,
            user["organization_id"],
            module_id,
            user["user_id"],
        ),
    )


def render_scenario_page(user: Dict) -> None:
    view_logger = learner_logger.bind(user_id=user.get("user_id"), session_id=st.session_state.get("session_id"))
    assignment_id = st.session_state.get("active_assignment_id")
    if not assignment_id:
        st.info("Select a module from Assigned Modules to begin.")
        return

    assignment = fetch_one(
        """
        SELECT assignment_id, assigned_at, module_id
        FROM assignments
        WHERE learner_id = ? AND assignment_id = ? AND organization_id = ? AND is_active = TRUE
        """,
        (user["user_id"], assignment_id, user["organization_id"]),
    )
    if not assignment:
        st.error("Assignment not found or you do not have access to it.")
        return

    module_id = int(assignment["module_id"])
    st.query_params["assignment_id"] = str(assignment_id)

    existing_attempt = fetch_one(
        """
        SELECT attempt_id, total_score
        FROM attempts
        WHERE user_id = ?
          AND module_id = ?
          AND organization_id = ?
          AND created_at >= ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user["user_id"], module_id, user["organization_id"], assignment["assigned_at"]),
    )
    if existing_attempt:
        st.success(f"You've already completed this module. Score: {existing_attempt['total_score']}%")
        st.info("This assignment allows one graded submission. If reassigned by your admin, you can attempt it again.")
        if st.button("View completed results", type="secondary"):
            st.session_state.active_assignment_id = None
            st.session_state.latest_attempt_id = int(existing_attempt["attempt_id"])
            st.session_state.page = "Results"
            st.rerun()
        return

    module = fetch_one(
        "SELECT * FROM modules WHERE module_id = ? AND organization_id = ?",
        (module_id, user["organization_id"]),
    )
    actions = fetch_all("SELECT * FROM investigation_actions WHERE module_id = ?", (module_id,))
    assessment_questions = fetch_all(
        "SELECT * FROM module_questions WHERE module_id = ? ORDER BY question_order",
        (module_id,),
    )

    persisted = _load_or_create_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
    step_key = f"wizard_step_{assignment_id}"
    st.session_state.setdefault(step_key, int(persisted.get("current_step") or 1))
    st.session_state.setdefault(f"used_actions_{assignment_id}", json.loads(persisted.get("used_actions") or "[]"))
    st.session_state.setdefault(f"revealed_{assignment_id}", json.loads(persisted.get("revealed_actions") or "{}"))
    st.session_state.setdefault(f"started_at_{assignment_id}", persisted.get("started_at") or datetime.now(timezone.utc).isoformat())
    st.session_state.setdefault(f"notes_{assignment_id}", persisted.get("learner_notes") or "")
    st.session_state.setdefault(f"diagnosis_{assignment_id}", persisted.get("diagnosis_response") or "")
    st.session_state.setdefault(f"next_steps_{assignment_id}", persisted.get("next_steps_response") or "")
    st.session_state.setdefault(f"customer_{assignment_id}", persisted.get("customer_response") or "")
    st.session_state.setdefault(f"escalation_{assignment_id}", persisted.get("escalation_choice") or "No escalation")
    st.session_state.setdefault(f"question_answers_{assignment_id}", json.loads(persisted.get("question_responses") or "{}"))

    st.subheader(module["title"])
    st.caption(f"Difficulty: {module['difficulty']} • Estimated time: {module['estimated_time']}")

    current_step = int(st.session_state.get(step_key, 1))
    st.progress(current_step / len(WIZARD_STEPS), text=f"Step {current_step} of {len(WIZARD_STEPS)} • {WIZARD_STEPS[current_step - 1]}")

    used_actions_key = f"used_actions_{assignment_id}"
    revealed_key = f"revealed_{assignment_id}"
    started_at_key = f"started_at_{assignment_id}"

    started_at_iso = st.session_state.get(started_at_key)
    try:
        started_at_dt = datetime.fromisoformat(started_at_iso) if started_at_iso else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        started_at_dt = datetime.now(timezone.utc)
        st.session_state[started_at_key] = started_at_dt.isoformat()

    duration_minutes = _estimated_minutes(module.get("estimated_time"), fallback=20)
    deadline = started_at_dt.timestamp() + (duration_minutes * 60)
    now_ts = datetime.now(timezone.utc).timestamp()
    remaining_seconds = int(deadline - now_ts)
    timer_key = f"timer_submitted_{assignment_id}"
    st.session_state.setdefault(timer_key, False)

    @st.fragment(run_every="1s")
    def _render_countdown(seconds_left: int) -> None:
        minutes = max(0, seconds_left) // 60
        seconds = max(0, seconds_left) % 60
        if seconds_left > 0:
            st.info(f"⏳ Time remaining: {minutes:02d}:{seconds:02d}")
        else:
            st.error("⏰ Time is up. Your current work will be submitted automatically.")

    _render_countdown(remaining_seconds)

    question_answers: dict[str, str] = st.session_state.get(f"question_answers_{assignment_id}", {})
    diagnosis = st.session_state.get(f"diagnosis_{assignment_id}", "")
    next_steps = st.session_state.get(f"next_steps_{assignment_id}", "")
    customer_response = st.session_state.get(f"customer_{assignment_id}", "")

    if current_step == 1:
        with st.container(border=True):
            st.markdown("### Scenario Overview")
            st.write(_build_scenario_overview(module))
    elif current_step == 2:
        st.markdown("### Investigation / Notes")
        cols = st.columns(3)
        for idx, action in enumerate(actions):
            with cols[idx % 3]:
                if st.button(action["action_name"], key=f"action_{assignment_id}_{action['action_id']}"):
                    if action["action_name"] not in st.session_state[used_actions_key]:
                        st.session_state[used_actions_key].append(action["action_name"])
                    st.session_state[revealed_key][action["action_name"]] = action["revealed_information"]
        if st.session_state[revealed_key]:
            for name, details in st.session_state[revealed_key].items():
                with st.expander(name, expanded=True):
                    st.write(details)
        st.text_area("Personal notes", key=f"notes_{assignment_id}", height=140)
    elif current_step == 3:
        st.markdown("### Assessment Questions")
        for question in assessment_questions:
            qid = str(question["question_id"])
            question_key = f"assessment_q_{assignment_id}_{qid}"
            if question_key not in st.session_state and question_answers.get(qid):
                st.session_state[question_key] = question_answers.get(qid)
            if question.get("question_type") == "multiple_choice":
                options = _question_options(question.get("options_text"))
                answer = st.radio(
                    f"Q{question['question_order']}. {question['question_text']}",
                    options=options if options else ["No options configured"],
                    key=question_key,
                    index=None,
                )
            else:
                answer = st.text_area(
                    f"Q{question['question_order']}. {question['question_text']}",
                    key=question_key,
                    height=100,
                )
            question_answers[qid] = answer or ""
        st.session_state[f"question_answers_{assignment_id}"] = question_answers
    elif current_step == 4:
        st.markdown("### Final Response / Decision")
        st.text_area("Diagnosis", key=f"diagnosis_{assignment_id}", height=100)
        st.text_area("Next steps", key=f"next_steps_{assignment_id}", height=120)
        st.text_area("Customer response", key=f"customer_{assignment_id}", height=120)
        st.selectbox(
            "Escalation decision",
            ["No escalation", "Escalate to Engineering", "Escalate to Security", "Escalate to Product"],
            key=f"escalation_{assignment_id}",
        )
    else:
        st.markdown("### Review and Submit")
        with st.container(border=True):
            st.write(f"Actions used: {len(st.session_state[used_actions_key])}")
            answered = sum(1 for value in question_answers.values() if str(value).strip())
            st.write(f"Answered questions: {answered}/{len(assessment_questions)}")
            st.write(f"Diagnosis provided: {'Yes' if st.session_state.get(f'diagnosis_{assignment_id}', '').strip() else 'No'}")

    def _submit_module_attempt(*, timed_out: bool) -> None:
        scenario_logger = view_logger.bind(scenario_id=module_id)
        scenario_logger.info("Form submitted.", form="submit_module", timed_out=timed_out)
        submitted_at = datetime.now(timezone.utc)
        started_at_iso_inner = st.session_state.get(started_at_key)
        started_at = None
        try:
            started_at = datetime.fromisoformat(started_at_iso_inner) if started_at_iso_inner else None
        except (TypeError, ValueError):
            started_at = None
        elapsed_seconds = int((submitted_at - started_at).total_seconds()) if started_at else None
        time_limit_seconds = duration_minutes * 60
        time_remaining_seconds = max(0, time_limit_seconds - (elapsed_seconds or 0)) if elapsed_seconds is not None else None
        time_out_note = "\n\nTime limit reached before completion." if timed_out else ""
        answers = {
            "diagnosis_answer": st.session_state.get(f"diagnosis_{assignment_id}", ""),
            "next_steps_answer": st.session_state.get(f"next_steps_{assignment_id}", ""),
            "customer_response": st.session_state.get(f"customer_{assignment_id}", ""),
            "escalation_choice": st.session_state.get(f"escalation_{assignment_id}", "No escalation"),
            "notes": f"{st.session_state.get(f'notes_{assignment_id}', '')}{time_out_note}",
            "started_at": started_at.isoformat() if started_at else None,
            "submitted_at": submitted_at.isoformat(),
            "elapsed_seconds": elapsed_seconds,
            "time_limit_seconds": time_limit_seconds,
            "time_remaining_seconds": time_remaining_seconds,
            "attempt_state": "time_expired" if timed_out else "graded",
            "graded_by_type": "system",
            "graded_by_user_id": None,
            "graded_at": submitted_at.isoformat(),
            "actions_used": list(st.session_state[used_actions_key]),
            "actions_used_count": len(st.session_state[used_actions_key]),
            "timed_out": timed_out,
            "question_responses": json.dumps(st.session_state.get(f"question_answers_{assignment_id}", {})),
        }
        try:
            evaluation = evaluate_submission(dict(module), answers, st.session_state[used_actions_key])
            payload = {**answers, **evaluation}
            attempt_id = insert_attempt(user["user_id"], module_id, payload, user["organization_id"])
            log_actions(attempt_id, st.session_state[used_actions_key])
            scenario_logger.info("Scenario submission saved.", attempt_id=attempt_id, timed_out=timed_out)

            st.session_state.latest_attempt_id = attempt_id
            st.session_state.active_assignment_id = None
            st.session_state.page = "Results"
            st.session_state.pop(started_at_key, None)
            st.session_state[timer_key] = True
            st.session_state[f"submitted_{assignment_id}"] = True
            execute(
                """
                UPDATE assignment_workspace_state
                SET progress_status = 'submitted',
                    submitted_state = 1,
                    submitted_at = CURRENT_TIMESTAMP,
                    current_step = 6,
                    updated_at = CURRENT_TIMESTAMP,
                    last_saved_at = CURRENT_TIMESTAMP
                WHERE assignment_id = ? AND organization_id = ? AND module_id = ? AND user_id = ?
                """,
                (assignment_id, user["organization_id"], module_id, user["user_id"]),
            )
            if timed_out:
                st.toast("⏰ Time ran out — we submitted your current work for grading.")
            else:
                st.toast("🎉 Thank you — you've completed this module!")
            st.rerun()
        except Exception:
            scenario_logger.exception("Failed to submit module.")
            st.error("We couldn't submit this module. Please try again.")

    if remaining_seconds <= 0 and not st.session_state.get(timer_key):
        _submit_module_attempt(timed_out=True)

    validation_error = None
    if current_step == 3 and assessment_questions:
        unanswered = [q for q in assessment_questions if not question_answers.get(str(q["question_id"]), "").strip()]
        if unanswered:
            validation_error = "Please answer all assessment questions before continuing."
    if current_step == 4:
        if not st.session_state.get(f"diagnosis_{assignment_id}", "").strip() or not st.session_state.get(f"next_steps_{assignment_id}", "").strip() or not st.session_state.get(f"customer_{assignment_id}", "").strip():
            validation_error = "Diagnosis, next steps, and customer response are required before continuing."
    if validation_error:
        st.warning(validation_error)

    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Back", disabled=current_step <= 1):
            st.session_state[step_key] = max(1, current_step - 1)
            _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
            st.rerun()
    with c2:
        if st.button("Next", disabled=current_step >= 5 or bool(validation_error)):
            st.session_state[step_key] = min(5, current_step + 1)
            _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
            st.rerun()
    with c3:
        if st.button("Send to database: Submit module", type="primary", disabled=st.session_state.get(timer_key, False) or current_step < 5):
            _submit_module_attempt(timed_out=False)


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
    if int(attempt.get("timed_out") or 0) == 1 or attempt.get("attempt_state") == "time_expired":
        st.warning("This assessment was auto-submitted because the time limit expired.")
    st.caption(
        "Time given: "
        f"{_format_duration(attempt.get('time_limit_seconds'))} • "
        f"Time taken: {_format_duration(attempt.get('elapsed_seconds'))} • "
        f"Time left: {_format_duration(attempt.get('time_remaining_seconds'))}"
    )
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
        st.session_state.active_assignment_id = None
        st.query_params.pop("assignment_id", None)
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
