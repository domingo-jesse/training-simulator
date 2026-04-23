from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict
from urllib import error, request

import streamlit as st

from db import execute, fetch_all, fetch_one, insert_attempt, log_actions
from ai_grading import grade_submission_with_ai
from logger import get_logger
from utils import ensure_dataframe_schema, has_dataframe_columns, metric_row, parse_json_list, render_page_header, safe_int, to_df

learner_logger = get_logger(module="learner_views")
WIZARD_STEPS = [
    "Scenario Overview",
    "Investigation / Notes",
    "Assessment Questions",
    "Final Response / Decision",
    "Review and Submit",
]

SUBMITTED_STATE_IN_PROGRESS = 0
SUBMITTED_STATE_SUBMITTED = 1
SUBMITTED_STATE_APPROVED = 2
APPROVED_GRADING_STATUS = "approved"


def _clamp_wizard_step(step_value: int) -> int:
    return max(1, min(step_value, len(WIZARD_STEPS)))


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
    text = (options_text or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(choice).strip() for choice in parsed if str(choice).strip()]
        if isinstance(parsed, dict):
            choices = parsed.get("choices")
            if isinstance(choices, list):
                return [str(choice).strip() for choice in choices if str(choice).strip()]
    except Exception:
        pass
    return [line.strip() for line in text.splitlines() if line.strip()]


def _is_ai_conversation_question(question: Dict) -> bool:
    return str(question.get("question_type") or "").strip().lower() == "ai_conversation"


def _question_answer_complete(question: Dict, answer_value: object) -> bool:
    if not _is_ai_conversation_question(question):
        return bool(str(answer_value or "").strip())
    if not isinstance(answer_value, dict):
        return False
    return bool(answer_value.get("complete")) and bool(answer_value.get("transcript"))


def _generate_ai_conversation_message(*, question: Dict, transcript: list[dict], is_wrap_up: bool = False) -> str:
    persona = str(question.get("ai_role_or_persona") or "Training coach").strip()
    prompt = str(question.get("ai_conversation_prompt") or "").strip()
    evaluation_focus = str(question.get("evaluation_focus") or "").strip()
    wrap_up_instruction = str(question.get("wrap_up_message_optional") or "").strip()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        if not transcript:
            return f"Hello, I'm your {persona}. {prompt}"
        if is_wrap_up:
            return f"Thanks for completing this exercise. Focus area reviewed: {evaluation_focus or 'scenario judgment'}."
        return "Thanks, continue with your next step in this scenario."

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    system_prompt = (
        f"You are role-playing as {persona}. Keep responses concise (1-3 sentences), realistic, and in-role. "
        f"Scenario: {prompt}. Evaluation focus: {evaluation_focus}. "
        "Do not go off-topic. Do not ask multiple questions at once."
    )
    if is_wrap_up:
        system_prompt += f" This is the final wrap-up. {wrap_up_instruction or 'Provide a short closing message.'}"
    body = {"model": model, "messages": [{"role": "system", "content": system_prompt}, *transcript], "temperature": 0.2}
    http_request = request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=30) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        return str(parsed["choices"][0]["message"]["content"]).strip()
    except (error.URLError, TimeoutError, KeyError, TypeError, json.JSONDecodeError, ValueError):
        if is_wrap_up:
            return f"Thanks for completing this conversation. We assessed: {evaluation_focus or 'scenario skills'}."
        return "Understood. Please continue with your next response."


def _format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "N/A"
    safe_seconds = max(0, int(seconds))
    minutes = safe_seconds // 60
    remainder = safe_seconds % 60
    return f"{minutes}m {remainder:02d}s"


def _state_is_submitted(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "t", "yes", "y", "submitted", "complete"}


def _submitted_state_value(value: object) -> int:
    if isinstance(value, bool):
        return SUBMITTED_STATE_SUBMITTED if value else SUBMITTED_STATE_IN_PROGRESS
    if isinstance(value, (int, float)):
        int_value = int(value)
        if int_value in {SUBMITTED_STATE_IN_PROGRESS, SUBMITTED_STATE_SUBMITTED, SUBMITTED_STATE_APPROVED}:
            return int_value
        return SUBMITTED_STATE_SUBMITTED if int_value > 0 else SUBMITTED_STATE_IN_PROGRESS
    normalized = str(value or "").strip().lower()
    if normalized in {"approved", "2"}:
        return SUBMITTED_STATE_APPROVED
    if normalized in {"1", "true", "t", "yes", "y", "submitted", "complete"}:
        return SUBMITTED_STATE_SUBMITTED
    return SUBMITTED_STATE_IN_PROGRESS


def is_result_approved(result_row: Dict | None) -> bool:
    if not result_row:
        return False
    grading_status = str(result_row.get("grading_status") or "").strip().lower()
    attempt_status = str(result_row.get("result_status") or "").strip().lower()
    return grading_status == APPROVED_GRADING_STATUS or attempt_status == APPROVED_GRADING_STATUS


def get_submission_status(result_row: Dict | None) -> Dict[str, str]:
    if not result_row:
        return {
            "label": "Not started",
            "headline": "No Submission Yet",
            "message": "No submission is available for this assignment yet.",
        }

    if is_result_approved(result_row):
        return {
            "label": "Approved",
            "headline": "Results Approved",
            "message": "Your scores and review details are available below.",
        }

    if result_row.get("attempt_id"):
        return {
            "label": "Pending Review",
            "headline": "Results Not Yet Approved",
            "message": "Submitted • Pending Review. Scores and detailed feedback will appear after admin approval.",
        }

    return {
        "label": "Not started",
        "headline": "No Submission Yet",
        "message": "No submission is available for this assignment yet.",
    }


def get_learner_visible_result(result_row: Dict | None) -> Dict | None:
    if not result_row or not is_result_approved(result_row):
        return None

    strengths = parse_json_list(result_row.get("learner_strengths") or result_row.get("strengths"))
    weaknesses = parse_json_list(result_row.get("learner_weaknesses") or result_row.get("missed_points"))
    return {
        "attempt_id": result_row.get("attempt_id"),
        "title": result_row.get("title"),
        "timed_out": result_row.get("timed_out"),
        "attempt_state": result_row.get("attempt_state"),
        "time_limit_seconds": result_row.get("time_limit_seconds"),
        "elapsed_seconds": result_row.get("elapsed_seconds"),
        "time_remaining_seconds": result_row.get("time_remaining_seconds"),
        "total_score": result_row.get("approved_percentage"),
        "understanding_score": result_row.get("understanding_score"),
        "investigation_score": result_row.get("investigation_score"),
        "solution_score": result_row.get("solution_score"),
        "communication_score": result_row.get("communication_score"),
        "strengths": strengths,
        "missed_points": weaknesses,
        "best_practice_reasoning": result_row.get("best_practice_reasoning"),
        "recommended_response": result_row.get("recommended_response"),
        "takeaway_summary": result_row.get("lesson_takeaway"),
        "lesson_takeaway": result_row.get("lesson_takeaway"),
        "learner_visible_feedback": result_row.get("learner_visible_feedback"),
    }


def _assigned_modules(user: Dict):
    return _assigned_modules_cached(int(user["user_id"]), int(user["organization_id"]))


@st.cache_data(ttl=30, show_spinner=False)
def _assigned_modules_cached(user_id: int, organization_id: int):
    rows = fetch_all(
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
            x.approved_best_score AS best_score,
            x.attempt_count,
            x.last_attempt_at,
            x.last_result_status,
            x.last_grading_status,
            x.last_total_score,
            COALESCE(ws.submitted_state, 0) AS submitted_state,
            COALESCE(ws.progress_status, 'not_started') AS progress_status,
            ws.current_step
        FROM assignments a
        JOIN modules m ON m.module_id = a.module_id
        LEFT JOIN (
            SELECT
                a2.assignment_id,
                COUNT(t.attempt_id) AS attempt_count,
                MAX(t.created_at) AS last_attempt_at,
                MAX(
                    CASE
                        WHEN COALESCE(t.result_status, 'pending_review') = 'approved' THEN t.total_score
                        ELSE NULL
                    END
                ) AS approved_best_score,
                (
                    SELECT COALESCE(t2.result_status, 'pending_review')
                    FROM attempts t2
                    WHERE t2.user_id = a2.learner_id
                      AND t2.module_id = a2.module_id
                      AND t2.organization_id = a2.organization_id
                      AND t2.created_at >= a2.assigned_at
                    ORDER BY t2.created_at DESC
                    LIMIT 1
                ) AS last_result_status
                ,
                (
                    SELECT COALESCE(ss2.grading_status, t2.result_status, 'pending_review')
                    FROM attempts t2
                    LEFT JOIN submission_scores ss2 ON ss2.attempt_id = t2.attempt_id
                    WHERE t2.user_id = a2.learner_id
                      AND t2.module_id = a2.module_id
                      AND t2.organization_id = a2.organization_id
                      AND t2.created_at >= a2.assigned_at
                    ORDER BY t2.created_at DESC
                    LIMIT 1
                ) AS last_grading_status
                ,
                (
                    SELECT t3.total_score
                    FROM attempts t3
                    WHERE t3.user_id = a2.learner_id
                      AND t3.module_id = a2.module_id
                      AND t3.organization_id = a2.organization_id
                      AND t3.created_at >= a2.assigned_at
                    ORDER BY t3.created_at DESC
                    LIMIT 1
                ) AS last_total_score
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
        LEFT JOIN assignment_workspace_state ws
          ON ws.assignment_id = a.assignment_id
         AND ws.organization_id = a.organization_id
         AND ws.module_id = a.module_id
         AND ws.user_id = a.learner_id
        WHERE a.learner_id = ?
          AND a.organization_id = ?
          AND a.is_active = TRUE
          AND COALESCE(m.status, 'existing') <> 'archived'
        ORDER BY a.assigned_at DESC
        """,
        (user_id, organization_id, user_id, organization_id),
    )
    return [_with_assignment_status(dict(row)) for row in rows]


def _learner_module_status(module: Dict) -> str:
    attempt_count = safe_int(module.get("attempt_count"), 0)
    last_grading_status = str(module.get("last_grading_status") or "").strip().lower()
    last_result_status = str(module.get("last_result_status") or "").strip().lower()
    if last_grading_status == APPROVED_GRADING_STATUS or last_result_status == APPROVED_GRADING_STATUS:
        return "Completed"

    if attempt_count > 0:
        if last_result_status in {"submitted", "ai_grading", "ai_graded_pending_review", "pending_review", "grading_failed", "returned"} or not last_result_status:
            return "Pending results"
        return "Submitted"

    if _state_is_submitted(module.get("submitted_state")):
        return "Submitted"

    progress_status = str(module.get("progress_status") or "").strip().lower()
    if progress_status == "in_progress" or safe_int(module.get("current_step"), 1) > 1:
        return "In progress"

    return "Not started"


def _with_assignment_status(module: Dict) -> Dict:
    module["status"] = _learner_module_status(module)
    return module


def _learner_stats(user: Dict) -> Dict:
    return _learner_stats_cached(int(user["user_id"]), int(user["organization_id"]))


@st.cache_data(ttl=30, show_spinner=False)
def _learner_stats_cached(user_id: int, organization_id: int) -> Dict:
    attempts = fetch_all(
        """
        SELECT
            a.attempt_id,
            a.module_id,
            a.created_at,
            m.title,
            COALESCE(ss.percentage, a.total_score) AS approved_score,
            COALESCE(ss.learner_visible_feedback, ss.overall_ai_feedback, a.ai_feedback) AS learner_feedback
        FROM attempts a
        JOIN modules m ON a.module_id = m.module_id
        LEFT JOIN submission_scores ss ON ss.attempt_id = a.attempt_id
        WHERE a.user_id = ?
          AND a.organization_id = ?
          AND COALESCE(ss.grading_status, a.result_status, 'pending_review') = 'approved'
        ORDER BY a.created_at DESC
        """,
        (user_id, organization_id),
    )
    assigned_modules = _assigned_modules_cached(user_id, organization_id)
    completed_count = sum(1 for module in assigned_modules if module.get("status") == "Completed")
    avg_score = round(sum(float(a["approved_score"]) for a in attempts if a.get("approved_score") is not None) / len(attempts), 1) if attempts else 0
    return {
        "attempts": attempts,
        "assigned_count": len(assigned_modules),
        "completed_count": completed_count,
        "avg_score": avg_score,
        "assigned_modules": assigned_modules,
    }


@st.cache_data(ttl=120, show_spinner=False)
def _module_workspace_bundle(module_id: int, organization_id: int) -> tuple[dict | None, list[dict], list[dict]]:
    """Cache relatively static module metadata used by the learner workspace."""
    module = fetch_one(
        "SELECT * FROM modules WHERE module_id = ? AND organization_id = ?",
        (module_id, organization_id),
    )
    actions = fetch_all("SELECT * FROM investigation_actions WHERE module_id = ?", (module_id,))
    questions = fetch_all(
        "SELECT * FROM module_questions WHERE module_id = ? ORDER BY question_order",
        (module_id,),
    )
    return module, actions, questions


@st.cache_data(ttl=30, show_spinner=False)
def _assignment_for_workspace_cached(assignment_id: int, user_id: int, organization_id: int) -> tuple[dict | None, str | None]:
    assignment = fetch_one(
        """
        SELECT assignment_id, assigned_at, module_id
        FROM assignments
        WHERE assignment_id = ?
          AND learner_id = ?
          AND organization_id = ?
          AND is_active = TRUE
        """,
        (assignment_id, user_id, organization_id),
    )
    if assignment:
        return dict(assignment), None

    exists_for_org = fetch_one(
        """
        SELECT assignment_id
        FROM assignments
        WHERE assignment_id = ?
          AND organization_id = ?
          AND is_active = TRUE
        """,
        (assignment_id, organization_id),
    )
    if exists_for_org:
        return None, "unauthorized"
    return None, "not_found"


@st.cache_data(ttl=15, show_spinner=False)
def _workspace_state_cached(assignment_id: int, module_id: int, user_id: int, organization_id: int) -> dict | None:
    state = fetch_one(
        """
        SELECT *
        FROM assignment_workspace_state
        WHERE assignment_id = ?
          AND organization_id = ?
          AND module_id = ?
          AND user_id = ?
        """,
        (assignment_id, organization_id, module_id, user_id),
    )
    return dict(state) if state else None


@st.cache_data(ttl=20, show_spinner=False)
def _existing_attempt_cached(user_id: int, module_id: int, organization_id: int, assigned_at: str):
    return fetch_one(
        """
        SELECT attempt_id, total_score, COALESCE(result_status, 'pending_review') AS result_status
        FROM attempts
        WHERE user_id = ?
          AND module_id = ?
          AND organization_id = ?
          AND created_at >= ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id, module_id, organization_id, assigned_at),
    )


def render_learner_home(user: Dict) -> None:
    render_page_header("Welcome back", "Complete assigned simulations and monitor your performance over time.")

    stats = _learner_stats(user)
    recent_feedback = (
        stats["attempts"][0].get("learner_feedback")
        if stats["attempts"] and stats["attempts"][0].get("learner_feedback")
        else "No approved feedback yet. Complete your first module and wait for admin approval to begin."
    )

    def navigate_learner(page_key: str, page_slug: str) -> None:
        st.session_state["learner_page"] = page_key
        st.query_params["page"] = page_slug
        st.rerun()

    summary_cards = [
        ("Assigned modules", stats["assigned_count"], "assigned_modules", "assigned-modules"),
        ("Completed", stats["completed_count"], "results", "progress-results"),
        ("Average score", f"{stats['avg_score']}%", "results", "progress-results"),
    ]
    card_columns = st.columns(len(summary_cards))
    for col, (label, value, target_page, target_slug) in zip(card_columns, summary_cards):
        with col:
            card_label = f"**{value}**  \n{label}"
            if st.button(card_label, key=f"home_summary_{target_page}_{label}", use_container_width=True):
                navigate_learner(target_page, target_slug)

    completion_ratio = (stats["completed_count"] / stats["assigned_count"]) if stats["assigned_count"] else 0
    st.progress(completion_ratio, text=f"Assigned progress: {int(completion_ratio * 100)}%")

    with st.container(border=True):
        st.markdown("#### Recent feedback")
        st.write(recent_feedback)


def render_module_library(user: Dict) -> None:
    view_logger = learner_logger.bind(user_id=user.get("user_id"), session_id=st.session_state.get("session_id"))
    in_workspace_mode = st.session_state.get("learner_page") == "module_workspace"
    render_page_header("Module Workspace" if in_workspace_mode else "Assigned Modules", "Focused execution flow with clean progress tracking.")
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
            st.session_state.learner_page = "module_workspace"
            st.query_params["page"] = "module-workspace"
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
                    st.session_state.learner_page = "assigned_modules"
                    st.query_params["page"] = "assigned-modules"
                    st.query_params.pop("assignment_id", None)
                    st.rerun()
            render_scenario_page(user)
            return
        st.info("You already have an active module workspace. Open **Module Workspace** from the sidebar to continue.")
    elif in_workspace_mode:
        st.info("No active module workspace yet. Start a module from **Assigned Modules**.")
        if st.button("Go to assigned modules", type="primary"):
            st.session_state.learner_page = "assigned_modules"
            st.query_params["page"] = "assigned-modules"
            st.rerun()
        return

    assigned_modules = [module for module in assignments if module["status"] != "Completed"]
    completed_modules = [module for module in assignments if module["status"] == "Completed"]
    tab_assigned, tab_completed = st.tabs(["Assigned", "Completed"])

    def _open_results(module: Dict, *, action: str) -> None:
        view_logger.info("Button click.", action=action, scenario_id=module["module_id"])
        st.session_state.active_assignment_id = None
        st.session_state.learner_page = "results"
        st.session_state["learner_selected_result_assignment"] = int(module["assignment_id"])
        st.query_params["page"] = "progress-results"
        st.rerun()

    with tab_assigned:
        if not assigned_modules:
            st.caption("No active assigned modules right now.")
        for i in range(0, len(assigned_modules), 2):
            cols = st.columns(2)
            for col, module in zip(cols, assigned_modules[i : i + 2]):
                with col:
                    with st.container(border=True):
                        status = module["status"]
                        st.markdown(f"### {module['title']}")
                        st.caption(
                            f"{module['category']} • {module['difficulty']} • {module['estimated_time']} • Status: {status}"
                        )
                        if module["due_date"]:
                            st.caption(f"Due: {module['due_date']}")
                        st.write(_compact_text(module["description"]))

                        if status == "Pending results":
                            st.warning("Pending results • Submitted and awaiting review.")
                            if st.button(
                                "View status",
                                key=f"status_{module['assignment_id']}_{module['module_id']}",
                                type="secondary",
                            ):
                                _open_results(module, action="view_pending_status")
                        elif status == "Submitted":
                            st.info("Submitted • Results pending approval.")
                            if st.button(
                                "View submission",
                                key=f"submission_{module['assignment_id']}_{module['module_id']}",
                                type="secondary",
                            ):
                                _open_results(module, action="view_submission_status")
                        elif status == "In progress":
                            st.info("In progress • Continue your module.")
                            if st.button(
                                "Continue module",
                                key=f"continue_{module['assignment_id']}_{module['module_id']}",
                                type="primary",
                            ):
                                view_logger.info("Button click.", action="continue_module_clicked", scenario_id=module["module_id"])
                                st.session_state.pending_start_module = dict(module)
                                st.rerun()
                        else:
                            st.info("Not started • Ready when you are.")
                            if st.button(
                                "Start module",
                                key=f"start_{module['assignment_id']}_{module['module_id']}",
                                type="primary",
                            ):
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
                        best_score = module.get("best_score")
                        score_display = f"{best_score}%" if best_score is not None else "Approved"
                        st.success(f"Completed • Best score: {score_display}")
                        if st.button(
                            "View completed module",
                            key=f"view_{module['assignment_id']}_{module['module_id']}",
                            type="secondary",
                        ):
                            _open_results(module, action="view_score")



def _load_or_create_workspace_state(*, assignment_id: int, module_id: int, user: Dict, time_limit_minutes: int) -> dict:
    state = _workspace_state_cached(assignment_id, module_id, int(user["user_id"]), int(user["organization_id"]))
    if state:
        return dict(state)

    started_at = datetime.now(timezone.utc)
    end_time = datetime.fromtimestamp(started_at.timestamp() + (time_limit_minutes * 60), timezone.utc)
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
            time_limit_minutes,
            end_time,
            auto_submitted_state,
            created_at,
            updated_at,
            last_saved_at
        ) VALUES (?, ?, ?, ?, 1, 'not_started', '', '', '', '', 'No escalation', '{}', '{}', '[]', 0, ?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            assignment_id,
            user["organization_id"],
            module_id,
            user["user_id"],
            started_at.isoformat(),
            time_limit_minutes,
            end_time.isoformat(),
        ),
    )
    _workspace_state_cached.clear()
    return _workspace_state_cached(assignment_id, module_id, int(user["user_id"]), int(user["organization_id"])) or {}


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
            _clamp_wizard_step(int(st.session_state.get(f"wizard_step_{assignment_id}", 1))),
            "submitted" if st.session_state.get(f"submitted_{assignment_id}") else "in_progress",
            st.session_state.get(f"notes_{assignment_id}", ""),
            st.session_state.get(f"diagnosis_{assignment_id}", ""),
            st.session_state.get(f"next_steps_{assignment_id}", ""),
            st.session_state.get(f"customer_{assignment_id}", ""),
            st.session_state.get(f"escalation_{assignment_id}", "No escalation"),
            json.dumps(st.session_state.get(f"question_answers_{assignment_id}", {})),
            json.dumps(st.session_state.get(f"revealed_{assignment_id}", {})),
            json.dumps(st.session_state.get(f"used_actions_{assignment_id}", [])),
            SUBMITTED_STATE_SUBMITTED if st.session_state.get(f"submitted_{assignment_id}") else SUBMITTED_STATE_IN_PROGRESS,
            assignment_id,
            user["organization_id"],
            module_id,
            user["user_id"],
        ),
    )
    _workspace_state_cached.clear()


def _fetch_assignment_for_workspace(*, assignment_id: int, user: Dict) -> tuple[dict | None, str | None]:
    return _assignment_for_workspace_cached(int(assignment_id), int(user["user_id"]), int(user["organization_id"]))


def _render_assignment_access_error(*, reason: str) -> None:
    if reason == "unauthorized":
        st.error("Unauthorized: this assignment workspace is not available for your account.")
    else:
        st.error("Assignment not found. It may be inactive or unavailable.")
    st.session_state.active_assignment_id = None
    st.query_params.pop("assignment_id", None)


def render_scenario_page(user: Dict) -> None:
    view_logger = learner_logger.bind(user_id=user.get("user_id"), session_id=st.session_state.get("session_id"))
    assignment_id = st.session_state.get("active_assignment_id")
    if not assignment_id:
        st.info("Select a module from Assigned Modules to begin.")
        return

    assignment, access_error = _fetch_assignment_for_workspace(assignment_id=assignment_id, user=user)
    if not assignment:
        _render_assignment_access_error(reason=access_error or "not_found")
        return

    module_id = int(assignment["module_id"])
    st.query_params["assignment_id"] = str(assignment_id)

    existing_attempt = _existing_attempt_cached(
        int(user["user_id"]),
        module_id,
        int(user["organization_id"]),
        str(assignment["assigned_at"]),
    )
    if existing_attempt:
        if str(existing_attempt.get("result_status") or "").strip().lower() == "approved":
            st.success(f"You've already completed this module. Score: {existing_attempt['total_score']}%")
        else:
            st.success("You've already submitted this module.")
            st.info("Your submission has been graded and is awaiting instructor approval.")
        st.info("This assignment allows one graded submission. If reassigned by your admin, you can attempt it again.")
        if st.button("View results" if str(existing_attempt.get("result_status") or "").strip().lower() == "approved" else "View status", type="secondary"):
            st.session_state.active_assignment_id = None
            st.session_state.latest_attempt_id = int(existing_attempt["attempt_id"])
            st.session_state.learner_page = "results"
            st.session_state["learner_selected_result_assignment"] = int(assignment_id)
            st.query_params["page"] = "progress-results"
            st.rerun()
        return

    # Cache static module payload to avoid repeated DB reads during interaction reruns.
    module, actions, assessment_questions = _module_workspace_bundle(module_id, int(user["organization_id"]))
    if not module:
        st.error("This module is no longer available.")
        return

    duration_minutes = _estimated_minutes(module.get("estimated_time"), fallback=20)
    persisted = _load_or_create_workspace_state(
        assignment_id=assignment_id,
        module_id=module_id,
        user=user,
        time_limit_minutes=duration_minutes,
    )
    step_key = f"wizard_step_{assignment_id}"
    st.session_state.setdefault(step_key, _clamp_wizard_step(safe_int(persisted.get("current_step"), 1)))
    st.session_state.setdefault(f"used_actions_{assignment_id}", json.loads(persisted.get("used_actions") or "[]"))
    st.session_state.setdefault(f"revealed_{assignment_id}", json.loads(persisted.get("revealed_actions") or "{}"))
    st.session_state.setdefault(f"started_at_{assignment_id}", persisted.get("started_at") or datetime.now(timezone.utc).isoformat())
    st.session_state.setdefault(f"notes_{assignment_id}", persisted.get("learner_notes") or "")
    st.session_state.setdefault(f"diagnosis_{assignment_id}", persisted.get("diagnosis_response") or "")
    st.session_state.setdefault(f"next_steps_{assignment_id}", persisted.get("next_steps_response") or "")
    st.session_state.setdefault(f"customer_{assignment_id}", persisted.get("customer_response") or "")
    st.session_state.setdefault(f"escalation_{assignment_id}", persisted.get("escalation_choice") or "No escalation")
    st.session_state.setdefault(f"review_diagnosis_{assignment_id}", st.session_state.get(f"diagnosis_{assignment_id}", ""))
    st.session_state.setdefault(f"review_next_steps_{assignment_id}", st.session_state.get(f"next_steps_{assignment_id}", ""))
    st.session_state.setdefault(f"review_customer_{assignment_id}", st.session_state.get(f"customer_{assignment_id}", ""))
    st.session_state.setdefault(f"review_escalation_{assignment_id}", st.session_state.get(f"escalation_{assignment_id}", "No escalation"))
    st.session_state.setdefault(f"review_actions_{assignment_id}", list(st.session_state.get(f"used_actions_{assignment_id}", [])))
    st.session_state.setdefault(f"question_answers_{assignment_id}", json.loads(persisted.get("question_responses") or "{}"))
    persisted_submitted_state = _submitted_state_value(persisted.get("submitted_state"))
    st.session_state.setdefault(
        f"submitted_{assignment_id}",
        persisted_submitted_state >= SUBMITTED_STATE_SUBMITTED,
    )

    render_page_header(module["title"], f"Difficulty: {module['difficulty']} • Estimated time: {module['estimated_time']}")

    current_step = _clamp_wizard_step(int(st.session_state.get(step_key, 1)))
    total_steps = len(WIZARD_STEPS)
    st.progress(current_step / total_steps, text=f"Step {current_step} of {total_steps} • {WIZARD_STEPS[current_step - 1]}")

    used_actions_key = f"used_actions_{assignment_id}"
    revealed_key = f"revealed_{assignment_id}"
    started_at_key = f"started_at_{assignment_id}"

    started_at_iso = persisted.get("started_at") or st.session_state.get(started_at_key)
    try:
        started_at_dt = datetime.fromisoformat(started_at_iso) if started_at_iso else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        started_at_dt = datetime.now(timezone.utc)
        st.session_state[started_at_key] = started_at_dt.isoformat()

    persisted_limit_minutes = safe_int(persisted.get("time_limit_minutes"), duration_minutes)
    end_time_iso = persisted.get("end_time")
    try:
        end_time_dt = datetime.fromisoformat(end_time_iso) if end_time_iso else datetime.fromtimestamp(
            started_at_dt.timestamp() + (persisted_limit_minutes * 60), timezone.utc
        )
    except (TypeError, ValueError):
        end_time_dt = datetime.fromtimestamp(started_at_dt.timestamp() + (persisted_limit_minutes * 60), timezone.utc)
    if not end_time_iso:
        execute(
            """
            UPDATE assignment_workspace_state
            SET time_limit_minutes = COALESCE(time_limit_minutes, ?),
                end_time = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE assignment_id = ? AND organization_id = ? AND module_id = ? AND user_id = ?
            """,
            (
                persisted_limit_minutes,
                end_time_dt.isoformat(),
                assignment_id,
                user["organization_id"],
                module_id,
                user["user_id"],
            ),
        )
    deadline = end_time_dt.timestamp()
    now_ts = datetime.now(timezone.utc).timestamp()
    remaining_seconds = int(deadline - now_ts)
    timer_key = f"timer_submitted_{assignment_id}"
    st.session_state.setdefault(timer_key, False)
    already_submitted = _state_is_submitted(persisted.get("submitted_state"))
    already_auto_submitted = _state_is_submitted(persisted.get("auto_submitted_state"))

    @st.fragment(run_every="1s" if not already_submitted else None)
    def _render_countdown(deadline_epoch: float, is_submitted: bool) -> None:
        seconds_left = int(deadline_epoch - datetime.now(timezone.utc).timestamp())
        minutes = max(0, seconds_left) // 60
        seconds = max(0, seconds_left) % 60
        if is_submitted:
            st.success("✅ This assignment is already submitted.")
        elif seconds_left > 60:
            st.info(f"⏳ Time remaining: {minutes:02d}:{seconds:02d}")
        elif seconds_left > 0:
            st.warning(f"⚠️ 1 minute remaining — submit soon ({minutes:02d}:{seconds:02d}).")
        else:
            st.error("⏰ Time is up. Submitting your assignment...")

    with st.container(border=True):
        _render_countdown(deadline, already_submitted)

    if already_submitted:
        st.info("This assignment has already been submitted. Open Results or return to Assigned Modules.")
        return

    def _submit_module_attempt(*, timed_out: bool) -> None:
        scenario_logger = view_logger.bind(scenario_id=module_id)
        scenario_logger.info("User submitted module response form.", form="submit_module", timed_out=timed_out)
        submission_lock = fetch_one(
            """
            UPDATE assignment_workspace_state
            SET submitted_state = ?,
                progress_status = 'submitted',
                submitted_at = COALESCE(submitted_at, CURRENT_TIMESTAMP),
                auto_submitted_state = CASE WHEN ? THEN ? ELSE auto_submitted_state END,
                updated_at = CURRENT_TIMESTAMP,
                last_saved_at = CURRENT_TIMESTAMP
            WHERE assignment_id = ?
              AND organization_id = ?
              AND module_id = ?
              AND user_id = ?
              AND submitted_state = 0
            RETURNING assignment_id
            """,
            (
                SUBMITTED_STATE_SUBMITTED,
                timed_out,
                SUBMITTED_STATE_SUBMITTED,
                assignment_id,
                user["organization_id"],
                module_id,
                user["user_id"],
            ),
        )
        if not submission_lock:
            scenario_logger.info("Skipping duplicate submission attempt.")
            st.session_state[timer_key] = True
            st.session_state[f"submitted_{assignment_id}"] = True
            return

        submitted_at = datetime.now(timezone.utc)
        started_at_iso_inner = st.session_state.get(started_at_key)
        started_at = None
        try:
            started_at = datetime.fromisoformat(started_at_iso_inner) if started_at_iso_inner else None
        except (TypeError, ValueError):
            started_at = None
        elapsed_seconds = int((submitted_at - started_at).total_seconds()) if started_at else None
        time_limit_seconds = persisted_limit_minutes * 60
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
            "timed_out": 1 if timed_out else 0,
            "question_responses": json.dumps(st.session_state.get(f"question_answers_{assignment_id}", {})),
        }
        try:
            payload = {
                **answers,
                "result_status": "submitted",
                "coaching_feedback": "",
                "strengths": [],
                "missed_points": [],
                "best_practice_reasoning": "",
                "recommended_response": "",
                "takeaway_summary": "",
            }
            attempt_id = insert_attempt(user["user_id"], module_id, payload, user["organization_id"])
            log_actions(attempt_id, st.session_state[used_actions_key])
            try:
                grade_submission_with_ai(int(attempt_id))
            except Exception:
                scenario_logger.exception("Post-submit AI grading failed.", attempt_id=attempt_id)
            scenario_logger.info("Assignment submission recorded.", attempt_id=attempt_id, timed_out=timed_out)

            st.session_state.latest_attempt_id = attempt_id
            st.session_state.active_assignment_id = None
            st.session_state.learner_page = "results"
            st.session_state["learner_selected_result_assignment"] = int(assignment_id)
            st.query_params["page"] = "progress-results"
            st.session_state.pop(started_at_key, None)
            st.session_state[timer_key] = True
            st.session_state[f"submitted_{assignment_id}"] = True
            execute(
                """
                UPDATE assignment_workspace_state
                SET current_step = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    last_saved_at = CURRENT_TIMESTAMP
                WHERE assignment_id = ? AND organization_id = ? AND module_id = ? AND user_id = ?
                """,
                (total_steps, assignment_id, user["organization_id"], module_id, user["user_id"]),
            )
            if timed_out:
                st.toast("⏰ Time ran out — we submitted your current work for grading.")
            else:
                st.toast("🎉 Thank you — you've completed this module!")
            st.cache_data.clear()
            st.rerun()
        except Exception:
            scenario_logger.exception("Failed to submit module.")
            st.error("We couldn't submit this module. Please try again.")

    if (already_submitted or already_auto_submitted) and remaining_seconds <= 0:
        st.info("This assignment has already expired and been submitted.")
        return

    if remaining_seconds <= 0 and not st.session_state.get(timer_key) and not already_submitted:
        _submit_module_attempt(timed_out=True)

    @st.fragment
    def _render_assessment_workspace() -> None:
        # Keep interactive learner widgets in a fragment so shell/sidebar do not flicker on each interaction.
        current_step_local = _clamp_wizard_step(int(st.session_state.get(step_key, 1)))
        question_answers_local: dict[str, str] = st.session_state.get(f"question_answers_{assignment_id}", {})

        with st.container(border=True):
            if current_step_local == 1:
                st.markdown("### Scenario Overview")
                st.write(_build_scenario_overview(module))
                c1, c2 = st.columns(2)
                with c1:
                    st.button("Back", disabled=True)
                with c2:
                    if st.button("Next", type="primary"):
                        st.session_state[step_key] = 2
                        _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                        st.toast("Progress saved.")
                        st.rerun()
            elif current_step_local == 2:
                st.markdown("### Investigation / Notes")
                cols = st.columns(3)
                for idx, action in enumerate(actions):
                    with cols[idx % 3]:
                        if st.button(action["action_name"], key=f"action_{assignment_id}_{action['action_id']}"):
                            if action["action_name"] not in st.session_state[used_actions_key]:
                                st.session_state[used_actions_key].append(action["action_name"])
                            st.session_state[revealed_key][action["action_name"]] = action["revealed_information"]
                            _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                if st.session_state[revealed_key]:
                    for name, details in st.session_state[revealed_key].items():
                        with st.expander(name, expanded=True):
                            st.write(details)
                with st.form(key=f"wizard_step2_form_{assignment_id}", clear_on_submit=False):
                    st.text_area("Personal notes", key=f"notes_{assignment_id}", height=140)
                    c1, c2 = st.columns(2)
                    with c1:
                        back_clicked = st.form_submit_button("Back")
                    with c2:
                        next_clicked = st.form_submit_button("Next", type="primary")

                if back_clicked:
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                    st.session_state[step_key] = max(1, current_step_local - 1)
                    st.rerun()
                elif next_clicked:
                    st.session_state[step_key] = _clamp_wizard_step(current_step_local + 1)
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                    st.toast("Notes saved.")
                    st.rerun()
            elif current_step_local == 3:
                st.markdown("### Assessment Questions")
                for question in assessment_questions:
                    qid = str(question["question_id"])
                    question_key = f"assessment_q_{assignment_id}_{qid}"
                    if _is_ai_conversation_question(question):
                        conversation_key = f"ai_conversation_{assignment_id}_{qid}"
                        max_responses = int(question.get("max_learner_responses") or 3)
                        if max_responses not in {3, 4}:
                            max_responses = 3
                        payload = question_answers_local.get(qid)
                        if not isinstance(payload, dict):
                            payload = {"transcript": [], "learner_responses": 0, "complete": False, "max_learner_responses": max_responses}
                        payload["max_learner_responses"] = max_responses
                        transcript = payload.get("transcript")
                        if not isinstance(transcript, list):
                            transcript = []
                        if not transcript:
                            opening = _generate_ai_conversation_message(question=question, transcript=[])
                            transcript = [{"role": "assistant", "content": opening, "timestamp": datetime.now(timezone.utc).isoformat()}]
                            payload["transcript"] = transcript
                            question_answers_local[qid] = payload
                            st.session_state[f"question_answers_{assignment_id}"] = question_answers_local
                            _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                        with st.container(border=True):
                            st.markdown(f"**Q{question['question_order']}. {question['question_text']}**")
                            remaining = max(0, max_responses - int(payload.get("learner_responses") or 0))
                            st.caption(f"Responses remaining: {remaining} of {max_responses}")
                            for turn in transcript:
                                role = "AI" if turn.get("role") == "assistant" else "You"
                                st.markdown(f"**{role}:** {turn.get('content') or ''}")
                            input_key = f"{conversation_key}_input"
                            learner_reply = st.text_input(
                                "Your reply",
                                key=input_key,
                                disabled=bool(payload.get("complete")),
                            )
                            if st.button("Send reply", key=f"{conversation_key}_send", disabled=bool(payload.get("complete"))):
                                if learner_reply.strip():
                                    transcript.append({"role": "user", "content": learner_reply.strip(), "timestamp": datetime.now(timezone.utc).isoformat()})
                                    learner_count = int(payload.get("learner_responses") or 0) + 1
                                    payload["learner_responses"] = learner_count
                                    if learner_count >= max_responses:
                                        closing = _generate_ai_conversation_message(question=question, transcript=transcript, is_wrap_up=True)
                                        transcript.append({"role": "assistant", "content": closing, "timestamp": datetime.now(timezone.utc).isoformat()})
                                        payload["complete"] = True
                                    else:
                                        ai_reply = _generate_ai_conversation_message(question=question, transcript=transcript)
                                        transcript.append({"role": "assistant", "content": ai_reply, "timestamp": datetime.now(timezone.utc).isoformat()})
                                    payload["transcript"] = transcript
                                    question_answers_local[qid] = payload
                                    st.session_state[f"question_answers_{assignment_id}"] = question_answers_local
                                    st.session_state[input_key] = ""
                                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                                    st.rerun()
                            if payload.get("complete"):
                                st.success("Conversation complete. This question is locked.")
                        question_answers_local[qid] = payload
                    else:
                        if question_key not in st.session_state and question_answers_local.get(qid):
                            st.session_state[question_key] = question_answers_local.get(qid)
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
                        question_answers_local[qid] = answer or ""

                unanswered = [q for q in assessment_questions if not _question_answer_complete(q, question_answers_local.get(str(q["question_id"])))]
                step_validation_error = "Please complete all assessment questions before continuing." if unanswered else None
                if step_validation_error:
                    st.warning(step_validation_error)
                c1, c2 = st.columns(2)
                with c1:
                    back_clicked = st.button("Back", key=f"wizard_step3_back_{assignment_id}")
                with c2:
                    next_clicked = st.button("Next", key=f"wizard_step3_next_{assignment_id}", type="primary", disabled=bool(step_validation_error))

                if back_clicked:
                    st.session_state[f"question_answers_{assignment_id}"] = question_answers_local
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                    st.session_state[step_key] = max(1, current_step_local - 1)
                    st.rerun()
                elif next_clicked:
                    st.session_state[f"question_answers_{assignment_id}"] = question_answers_local
                    st.session_state[step_key] = _clamp_wizard_step(current_step_local + 1)
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                    st.toast("Progress saved.")
                    st.rerun()
            elif current_step_local == 4:
                st.markdown("### Final Response / Decision")
                with st.form(key=f"wizard_step4_form_{assignment_id}", clear_on_submit=False):
                    diagnosis_value = st.text_area("Diagnosis", key=f"diagnosis_{assignment_id}", height=100)
                    next_steps_value = st.text_area("Next steps", key=f"next_steps_{assignment_id}", height=120)
                    customer_response_value = st.text_area("Customer response", key=f"customer_{assignment_id}", height=120)
                    st.selectbox(
                        "Escalation decision",
                        ["No escalation", "Escalate to Engineering", "Escalate to Security", "Escalate to Product"],
                        key=f"escalation_{assignment_id}",
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        back_clicked = st.form_submit_button("Back")
                    with c2:
                        next_clicked = st.form_submit_button("Next", type="primary")

                if back_clicked:
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                    st.session_state[step_key] = max(1, current_step_local - 1)
                    st.rerun()
                elif next_clicked:
                    step_validation_error = None
                    if (
                        not (diagnosis_value or "").strip()
                        or not (next_steps_value or "").strip()
                        or not (customer_response_value or "").strip()
                    ):
                        step_validation_error = "Diagnosis, next steps, and customer response are required before continuing."
                    if step_validation_error:
                        st.warning(step_validation_error)
                    else:
                        st.session_state[step_key] = _clamp_wizard_step(current_step_local + 1)
                        _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)
                        st.toast("Progress saved.")
                        st.rerun()
            else:
                st.markdown("### Review and Submit")
                st.caption("Review your answers below. You can make final edits before submitting.")

                answered = sum(
                    1
                    for question in assessment_questions
                    if _question_answer_complete(question, question_answers_local.get(str(question["question_id"])))
                )
                summary_col1, summary_col2, summary_col3 = st.columns(3)
                with summary_col1:
                    st.metric("Actions selected", len(st.session_state[used_actions_key]))
                with summary_col2:
                    st.metric("Assessment questions", f"{answered}/{len(assessment_questions)}")
                with summary_col3:
                    has_diagnosis = bool(st.session_state.get(f"diagnosis_{assignment_id}", "").strip())
                    st.metric("Diagnosis provided", "Yes" if has_diagnosis else "No")

                action_name_to_details = {
                    action.get("action_name"): action.get("revealed_information", "")
                    for action in actions
                    if action.get("action_name")
                }
                action_names = list(action_name_to_details.keys())
                with st.form(key=f"wizard_step5_review_form_{assignment_id}", clear_on_submit=False):
                    st.markdown("#### Review and Edit")
                    diagnosis_value = st.text_area("Diagnosis", key=f"review_diagnosis_{assignment_id}", height=100)
                    next_steps_value = st.text_area("Next steps", key=f"review_next_steps_{assignment_id}", height=120)
                    customer_response_value = st.text_area(
                        "Customer/client response",
                        key=f"review_customer_{assignment_id}",
                        height=120,
                    )
                    escalation_value = st.selectbox(
                        "Escalation decision",
                        ["No escalation", "Escalate to Engineering", "Escalate to Security", "Escalate to Product"],
                        key=f"review_escalation_{assignment_id}",
                    )
                    selected_actions = st.multiselect(
                        "Selected actions",
                        options=action_names,
                        default=st.session_state.get(used_actions_key, []),
                        key=f"review_actions_{assignment_id}",
                        help="Adjust which investigation actions should be included with your submission.",
                    )

                    edited_question_answers: dict[str, str] = {}
                    if assessment_questions:
                        st.markdown("#### Assessment question responses")
                        for question in assessment_questions:
                            qid = str(question["question_id"])
                            review_question_key = f"review_assessment_q_{assignment_id}_{qid}"
                            if review_question_key not in st.session_state:
                                st.session_state[review_question_key] = question_answers_local.get(qid, "")
                            if question.get("question_type") == "multiple_choice":
                                options = _question_options(question.get("options_text"))
                                answer = st.radio(
                                    f"Q{question['question_order']}. {question['question_text']}",
                                    options=options if options else ["No options configured"],
                                    key=review_question_key,
                                    index=None,
                                )
                            elif _is_ai_conversation_question(question):
                                payload = question_answers_local.get(qid)
                                st.markdown(f"Q{question['question_order']}. {question['question_text']}")
                                if isinstance(payload, dict) and isinstance(payload.get("transcript"), list):
                                    for turn in payload.get("transcript") or []:
                                        role = "AI" if turn.get("role") == "assistant" else "You"
                                        st.markdown(f"**{role}:** {turn.get('content') or ''}")
                                answer = payload
                            else:
                                answer = st.text_area(
                                    f"Q{question['question_order']}. {question['question_text']}",
                                    key=review_question_key,
                                    height=90,
                                )
                            edited_question_answers[qid] = answer or ""

                    c1, c2, c3 = st.columns(3)
                    with c1:
                        back_clicked = st.form_submit_button("Back")
                    with c2:
                        save_clicked = st.form_submit_button("Save edits")
                    with c3:
                        submit_clicked = st.form_submit_button(
                            "Submit Response",
                            type="primary",
                            disabled=st.session_state.get(timer_key, False),
                        )

                if back_clicked or save_clicked or submit_clicked:
                    st.session_state[f"diagnosis_{assignment_id}"] = diagnosis_value or ""
                    st.session_state[f"next_steps_{assignment_id}"] = next_steps_value or ""
                    st.session_state[f"customer_{assignment_id}"] = customer_response_value or ""
                    st.session_state[f"escalation_{assignment_id}"] = escalation_value or "No escalation"
                    st.session_state[used_actions_key] = list(selected_actions or [])
                    for action_name in st.session_state[used_actions_key]:
                        if action_name not in st.session_state[revealed_key] and action_name in action_name_to_details:
                            st.session_state[revealed_key][action_name] = action_name_to_details[action_name]
                    if assessment_questions:
                        st.session_state[f"question_answers_{assignment_id}"] = edited_question_answers
                    _persist_workspace_state(assignment_id=assignment_id, module_id=module_id, user=user)

                if back_clicked:
                    st.session_state[step_key] = max(1, current_step_local - 1)
                    st.rerun()

                if save_clicked:
                    st.toast("Review updates saved.")
                    st.rerun()

                if submit_clicked:
                    review_validation_error = None
                    unanswered = [
                        q
                        for q in assessment_questions
                        if not _question_answer_complete(
                            q,
                            st.session_state.get(f"question_answers_{assignment_id}", {}).get(str(q["question_id"])),
                        )
                    ]
                    if unanswered:
                        review_validation_error = "Please answer all assessment questions before submitting."
                    elif (
                        not st.session_state.get(f"diagnosis_{assignment_id}", "").strip()
                        or not st.session_state.get(f"next_steps_{assignment_id}", "").strip()
                        or not st.session_state.get(f"customer_{assignment_id}", "").strip()
                    ):
                        review_validation_error = "Diagnosis, next steps, and customer response are required before submitting."

                    if review_validation_error:
                        st.warning(review_validation_error)
                    else:
                        _submit_module_attempt(timed_out=False)

    _render_assessment_workspace()


def _render_result_detail(attempt: Dict) -> None:
    st.markdown(f"### Result details • {attempt['title']}")
    if safe_int(attempt.get("timed_out")) == 1 or attempt.get("attempt_state") == "time_expired":
        st.warning("This assessment was auto-submitted because the time limit expired.")
    st.caption(
        "Time given: "
        f"{_format_duration(attempt.get('time_limit_seconds'))} • "
        f"Time taken: {_format_duration(attempt.get('elapsed_seconds'))} • "
        f"Time left: {_format_duration(attempt.get('time_remaining_seconds'))}"
    )
    metric_row(
        {
            "Total score": f"{attempt.get('total_score') or 0}%",
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
            strengths = attempt.get("strengths") or []
            if strengths:
                for item in strengths:
                    st.write(f"- {item}")
            else:
                st.caption("No specific strengths were captured for this result.")
    with col2:
        with st.container(border=True):
            st.markdown("#### What you missed")
            misses = attempt.get("missed_points") or []
            if misses:
                for item in misses:
                    st.write(f"- {item}")
            else:
                st.caption("No missed areas were captured for this result.")

    with st.container(border=True):
        st.markdown("#### Best-practice reasoning")
        st.write(attempt.get("best_practice_reasoning") or "No best-practice reasoning has been shared yet.")
        st.markdown("#### Recommended response")
        st.write(attempt.get("recommended_response") or "No recommended response has been shared yet.")
        st.markdown("#### Lesson takeaway")
        st.write(attempt.get("takeaway_summary") or attempt.get("lesson_takeaway") or "No lesson takeaway is available yet.")

    c1, _ = st.columns(2)
    with c1:
        st.success("✅ Thank you! You've completed this module.")


def _render_pending_result_state() -> None:
    with st.container(border=True):
        st.markdown("### Submitted")
        st.info("Pending Review")
        st.caption("Results Not Yet Approved • Scores and detailed feedback will appear after admin approval.")


def render_progress_results_page(user: Dict) -> None:
    approved_attempts = fetch_all(
        """
        SELECT
            a.attempt_id,
            a.module_id,
            m.title,
            COALESCE(ss.percentage, a.total_score) AS total_score,
            COALESCE(ss.learner_strengths, a.strengths) AS strengths,
            COALESCE(ss.learner_weaknesses, ss.learner_missed_points, a.missed_points) AS missed_points
        FROM attempts a
        LEFT JOIN submission_scores ss ON ss.attempt_id = a.attempt_id
        JOIN modules m ON a.module_id = m.module_id
        WHERE a.user_id = ?
          AND a.organization_id = ?
          AND COALESCE(ss.grading_status, a.result_status, 'pending_review') = 'approved'
        ORDER BY a.created_at
        """,
        (user["user_id"], user["organization_id"]),
    )

    assignments = _assigned_modules(user)
    if not assignments:
        render_page_header("Progress & Results", "Track overall progress and review detailed module outcomes in one place.")
        st.info("No assignments are available yet.")
        return

    attempts_by_assignment = {
        int(row["assignment_id"]): row
        for row in fetch_all(
            """
            SELECT DISTINCT ON (a.assignment_id)
                a.assignment_id,
                t.attempt_id,
                t.result_status,
                t.timed_out,
                t.attempt_state,
                t.time_limit_seconds,
                t.elapsed_seconds,
                t.time_remaining_seconds,
                m.title,
                COALESCE(ss.grading_status, t.result_status, 'pending_review') AS grading_status,
                COALESCE(ss.percentage, t.total_score) AS approved_percentage,
                COALESCE(ss.learner_visible_feedback, ss.overall_ai_feedback, t.ai_feedback) AS learner_visible_feedback,
                COALESCE(ss.learner_strengths, t.strengths) AS learner_strengths,
                COALESCE(ss.learner_weaknesses, ss.learner_missed_points, t.missed_points) AS learner_weaknesses,
                COALESCE(ss.best_practice_reasoning, t.best_practice_reasoning) AS best_practice_reasoning,
                COALESCE(ss.recommended_response, t.recommended_response, m.expected_customer_response) AS recommended_response,
                COALESCE(ss.lesson_takeaway, t.takeaway_summary, m.lesson_takeaway) AS lesson_takeaway,
                COALESCE(ss.understanding_score, t.understanding_score) AS understanding_score,
                COALESCE(ss.investigation_score, t.investigation_score) AS investigation_score,
                COALESCE(ss.solution_score, t.solution_score) AS solution_score,
                COALESCE(ss.communication_score, t.communication_score) AS communication_score
            FROM assignments a
            LEFT JOIN attempts t
                ON t.user_id = a.learner_id
               AND t.module_id = a.module_id
               AND t.organization_id = a.organization_id
               AND t.created_at >= a.assigned_at
            LEFT JOIN submission_scores ss ON ss.attempt_id = t.attempt_id
            LEFT JOIN modules m ON m.module_id = a.module_id
            WHERE a.learner_id = ?
              AND a.organization_id = ?
              AND a.is_active = TRUE
            ORDER BY a.assignment_id, t.created_at DESC NULLS LAST
            """,
            (user["user_id"], user["organization_id"]),
        )
        if row.get("attempt_id")
    }
    submitted_count = sum(
        1
        for module in assignments
        if module.get("status") in {"Submitted", "Pending results", "Completed"}
    )
    pending_results_count = sum(1 for module in assignments if module.get("status") == "Pending results")

    render_page_header("Progress & Results", "Track overall progress and review detailed module outcomes in one place.")

    df = to_df(
        approved_attempts,
        columns=["module_id", "total_score", "strengths", "missed_points"],
    )
    metric_row(
        {
            "Completed": int(df["module_id"].nunique()) if has_dataframe_columns(df, ["module_id"]) else 0,
            "Submitted": submitted_count,
            "Pending results": pending_results_count,
            "Average score": f"{round(df['total_score'].mean(), 1)}%" if has_dataframe_columns(df, ["total_score"]) else "0%",
            "Recent score": f"{df['total_score'].iloc[-1]}%" if has_dataframe_columns(df, ["total_score"]) else "N/A",
        }
    )

    strengths = []
    misses = []
    if not df.empty:
        df = ensure_dataframe_schema(df, ["strengths", "missed_points"])
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

    st.markdown("#### View module result")
    assignments_sorted = sorted(
        assignments,
        key=lambda a: (0 if a["status"] == "Completed" else 1, -(int(a["assignment_id"]))),
    )
    assignment_ids = [int(a["assignment_id"]) for a in assignments_sorted]
    label_map = {
        int(a["assignment_id"]): f"{a['title']} — {a['status']}"
        for a in assignments_sorted
    }
    default_assignment_id = next((assignment_id for assignment_id in assignment_ids if assignment_id in attempts_by_assignment), assignment_ids[0])
    if "learner_selected_result_assignment" not in st.session_state:
        st.session_state["learner_selected_result_assignment"] = default_assignment_id
    if st.session_state["learner_selected_result_assignment"] not in assignment_ids:
        st.session_state["learner_selected_result_assignment"] = default_assignment_id

    selected_assignment_id = st.selectbox(
        "Select assignment",
        options=assignment_ids,
        format_func=lambda assignment_id: label_map[assignment_id],
        key="learner_selected_result_assignment",
    )

    selected_attempt = attempts_by_assignment.get(int(selected_assignment_id))
    if not selected_attempt:
        status = get_submission_status(selected_attempt)
        st.info(status["message"])
        return

    learner_result = get_learner_visible_result(selected_attempt)
    if learner_result is None:
        _render_pending_result_state()
        return

    _render_result_detail(learner_result)
