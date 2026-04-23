from __future__ import annotations

import json
import re
from typing import Any

from db import execute, fetch_all, fetch_one
from logger import get_logger

grading_service_logger = get_logger(module="grading_service")

SCORING_APPROACHES = {"keyword", "llm", "manual_review"}


def _normalize_scoring_type(value: object, fallback: str = "keyword") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in SCORING_APPROACHES:
        return normalized
    if normalized in {"rubric_llm", "llm_rubric"}:
        return "llm"
    if normalized == "hybrid":
        return "keyword"
    return fallback if fallback in SCORING_APPROACHES else "keyword"


def _keywords(text: str) -> list[str]:
    tokens = [t.strip().lower() for t in re.split(r"[^a-zA-Z0-9]+", text or "") if len(t.strip()) > 3]
    seen: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.append(token)
    return seen[:30]


def _parse_rubric_criteria(criteria_json: str | None, rubric_text: str | None) -> list[dict[str, Any]]:
    criteria: list[dict[str, Any]] = []
    if criteria_json:
        try:
            parsed = json.loads(criteria_json)
            if isinstance(parsed, list):
                for row in parsed:
                    if isinstance(row, dict):
                        criteria.append(
                            {
                                "label": str(row.get("label") or "").strip(),
                                "guidance": str(row.get("guidance") or "").strip(),
                                "max_points": float(row.get("max_points") or 0),
                            }
                        )
        except Exception:
            criteria = []
    if criteria:
        return [row for row in criteria if row.get("label")]

    lines = [line.strip() for line in (rubric_text or "").splitlines() if line.strip()]
    return [{"label": line, "guidance": "", "max_points": 1.0} for line in lines]


def _keyword_grade(learner_answer: str, expected_answer: str, rubric: str, max_points: float) -> dict[str, Any]:
    rubric_text = " ".join(part for part in [expected_answer, rubric] if part).strip()
    rubric_keywords = _keywords(rubric_text)
    answer_lower = (learner_answer or "").lower()

    if not rubric_keywords:
        coverage_ratio = 1.0 if learner_answer.strip() else 0.0
        missing: list[str] = []
    else:
        covered = [k for k in rubric_keywords if k in answer_lower]
        coverage_ratio = len(covered) / len(rubric_keywords)
        missing = [k for k in rubric_keywords if k not in answer_lower][:8]
    awarded = round(max(0.0, min(max_points, max_points * coverage_ratio)), 2)
    return {
        "awarded_points": awarded,
        "max_points": max_points,
        "reasoning": f"Keyword coverage {round(coverage_ratio * 100, 1)}%.",
        "feedback": "Expand details tied to rubric expectations." if missing else "You covered most expected concepts.",
        "missing_elements": missing,
        "breakdown": [],
    }


def _rubric_grade(learner_answer: str, rubric: str, criteria_json: str | None, max_points: float) -> dict[str, Any]:
    criteria = _parse_rubric_criteria(criteria_json, rubric)
    if not criteria:
        return _keyword_grade(learner_answer, "", rubric, max_points)
    answer_lower = (learner_answer or "").lower()
    weighted_total = sum(max(float(item.get("max_points") or 0), 0.1) for item in criteria)
    scored_total = 0.0
    breakdown: list[dict[str, Any]] = []
    missing: list[str] = []
    for item in criteria:
        label = str(item.get("label") or "")
        guidance = str(item.get("guidance") or "")
        criterion_max = max(float(item.get("max_points") or 0), 0.1)
        label_keywords = _keywords(f"{label} {guidance}") or _keywords(label)
        hit_count = sum(1 for token in label_keywords if token in answer_lower) if label_keywords else 0
        coverage = (hit_count / len(label_keywords)) if label_keywords else (1.0 if learner_answer.strip() else 0.0)
        criterion_score = round(criterion_max * coverage, 2)
        scored_total += criterion_score
        if coverage < 0.6:
            missing.append(label)
        breakdown.append(
            {
                "criterion": label,
                "score": criterion_score,
                "max_points": criterion_max,
                "coverage": round(coverage, 3),
                "guidance": guidance,
            }
        )
    normalized = round((scored_total / weighted_total) * max_points, 2) if weighted_total > 0 else 0.0
    return {
        "awarded_points": max(0.0, min(max_points, normalized)),
        "max_points": max_points,
        "reasoning": "Rubric criteria scored with structured breakdown.",
        "feedback": "Review missing rubric criteria for a stronger response." if missing else "Good coverage across rubric criteria.",
        "missing_elements": missing[:8],
        "breakdown": breakdown,
    }


def grade_submission(attempt_id: int) -> dict[str, Any]:
    attempt = fetch_one(
        """
        SELECT a.*, m.organization_id AS module_org_id, m.expected_customer_response, m.lesson_takeaway,
               COALESCE(m.scoring_style, 'keyword') AS module_scoring_style,
               COALESCE(m.llm_scoring_enabled, FALSE) AS llm_scoring_enabled,
               COALESCE(m.llm_grader_instructions, '') AS llm_grader_instructions,
               COALESCE(m.learner_feedback_visibility, 'admin_approved_only') AS learner_feedback_visibility,
               COALESCE(m.scoring_config_json, '') AS module_scoring_config_json
        FROM attempts a
        JOIN modules m ON m.module_id = a.module_id
        WHERE a.attempt_id = ?
        """,
        (attempt_id,),
    )
    if not attempt:
        raise ValueError(f"Attempt {attempt_id} was not found.")

    module_questions = fetch_all(
        """
        SELECT question_id, question_text, COALESCE(expected_answer, '') AS expected_answer,
               COALESCE(rubric, rationale, '') AS rubric,
               COALESCE(max_points, 10) AS max_points,
               COALESCE(question_type, 'open_text') AS question_type,
               COALESCE(scoring_style, '') AS scoring_style,
               COALESCE(llm_grading_instructions, '') AS llm_grading_instructions,
               COALESCE(rubric_criteria_json, '') AS rubric_criteria_json
        FROM module_questions
        WHERE module_id = ?
        ORDER BY question_order
        """,
        (attempt["module_id"],),
    )

    question_responses_raw = attempt.get("question_responses")
    try:
        question_responses = json.loads(question_responses_raw) if isinstance(question_responses_raw, str) else (question_responses_raw or {})
    except Exception:
        question_responses = {}

    module_scoring_style = _normalize_scoring_type(attempt.get("module_scoring_style"), fallback="keyword")
    llm_enabled = bool(attempt.get("llm_scoring_enabled"))
    default_style = module_scoring_style if llm_enabled else "keyword"

    execute("UPDATE attempts SET result_status = 'ai_grading' WHERE attempt_id = ?", (attempt_id,))
    execute(
        """
        INSERT INTO submission_scores (attempt_id, grading_status, scoring_method)
        VALUES (?, 'ai_grading', ?)
        ON CONFLICT(attempt_id) DO UPDATE SET grading_status = excluded.grading_status, scoring_method = excluded.scoring_method
        """,
        (attempt_id, default_style),
    )

    try:
        question_scores: list[dict[str, Any]] = []
        total_score = 0.0
        max_total_score = 0.0
        for question in module_questions:
            question_id = int(question["question_id"])
            raw_answer = question_responses.get(str(question_id), "")
            if isinstance(raw_answer, dict):
                transcript = raw_answer.get("transcript")
                learner_answer = json.dumps(transcript) if isinstance(transcript, list) else json.dumps(raw_answer)
            else:
                learner_answer = str(raw_answer or "")
            max_points = float(question.get("max_points") or 0)
            q_style = _normalize_scoring_type(question.get("scoring_style"), fallback=default_style)
            if str(question.get("question_type") or "").strip() == "ai_conversation" and q_style == "keyword":
                q_style = "manual_review"
            if q_style == "manual_review":
                graded = {
                    "awarded_points": 0.0,
                    "max_points": max_points,
                    "reasoning": "Marked for manual review.",
                    "feedback": "Awaiting admin review.",
                    "missing_elements": [],
                    "breakdown": [],
                }
            elif q_style == "llm":
                graded = _rubric_grade(
                    learner_answer,
                    str(question.get("rubric") or ""),
                    str(question.get("rubric_criteria_json") or ""),
                    max_points,
                )
            else:
                graded = _keyword_grade(learner_answer, str(question.get("expected_answer") or ""), str(question.get("rubric") or ""), max_points)

            total_score += float(graded["awarded_points"])
            max_total_score += max_points
            execute(
                """
                INSERT INTO submission_question_scores (
                    attempt_id, question_id, ai_score, final_score, feedback,
                    learner_answer, ai_awarded_points, ai_max_points,
                    ai_feedback, ai_reasoning, missing_key_concepts, final_awarded_points,
                    visible_to_learner, is_admin_override, scoring_method, score_breakdown_json, conversation_transcript
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, FALSE, ?, ?, ?)
                ON CONFLICT(attempt_id, question_id) DO UPDATE SET
                    ai_score = excluded.ai_score,
                    final_score = excluded.final_score,
                    feedback = excluded.feedback,
                    learner_answer = excluded.learner_answer,
                    ai_awarded_points = excluded.ai_awarded_points,
                    ai_max_points = excluded.ai_max_points,
                    ai_feedback = excluded.ai_feedback,
                    ai_reasoning = excluded.ai_reasoning,
                    missing_key_concepts = excluded.missing_key_concepts,
                    final_awarded_points = excluded.final_awarded_points,
                    scoring_method = excluded.scoring_method,
                    score_breakdown_json = excluded.score_breakdown_json,
                    conversation_transcript = excluded.conversation_transcript,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    attempt_id,
                    question_id,
                    graded["awarded_points"],
                    graded["awarded_points"],
                    graded["feedback"],
                    learner_answer,
                    graded["awarded_points"],
                    graded["max_points"],
                    graded["feedback"],
                    graded["reasoning"],
                    json.dumps(graded["missing_elements"]),
                    graded["awarded_points"],
                    q_style,
                    json.dumps(graded["breakdown"]),
                    learner_answer if str(question.get("question_type") or "").strip() == "ai_conversation" else "",
                ),
            )
            question_scores.append({"question_id": question_id, "scoring_method": q_style, **graded})

        percentage = round((total_score / max_total_score) * 100, 1) if max_total_score else None
        overall_feedback = "Strong performance across rubric criteria." if (percentage or 0) >= 80 else "Partial understanding shown; review missed concepts and add more detail."
        any_manual_review = any(row.get("scoring_method") == "manual_review" for row in question_scores)
        grading_status = "pending_review" if any_manual_review else "ai_graded_pending_review"

        execute(
            """
            INSERT INTO submission_scores (
                attempt_id, ai_total_score, final_total_score, max_total_score, percentage,
                grading_status, overall_ai_feedback, learner_visible_feedback,
                best_practice_reasoning, recommended_response, lesson_takeaway,
                learner_strengths, learner_weaknesses, learner_missed_points,
                scoring_version, scoring_method, scoring_config_json, scoring_breakdown_json, ai_reasoning_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'llm_v1', ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                ai_total_score = excluded.ai_total_score,
                final_total_score = COALESCE(submission_scores.admin_total_score, excluded.ai_total_score),
                max_total_score = excluded.max_total_score,
                percentage = excluded.percentage,
                grading_status = excluded.grading_status,
                overall_ai_feedback = excluded.overall_ai_feedback,
                learner_visible_feedback = COALESCE(submission_scores.learner_visible_feedback, excluded.learner_visible_feedback),
                best_practice_reasoning = COALESCE(submission_scores.best_practice_reasoning, excluded.best_practice_reasoning),
                recommended_response = COALESCE(submission_scores.recommended_response, excluded.recommended_response),
                lesson_takeaway = COALESCE(submission_scores.lesson_takeaway, excluded.lesson_takeaway),
                learner_strengths = COALESCE(submission_scores.learner_strengths, excluded.learner_strengths),
                learner_weaknesses = COALESCE(submission_scores.learner_weaknesses, excluded.learner_weaknesses),
                learner_missed_points = COALESCE(submission_scores.learner_missed_points, excluded.learner_missed_points),
                scoring_method = excluded.scoring_method,
                scoring_config_json = excluded.scoring_config_json,
                scoring_breakdown_json = excluded.scoring_breakdown_json,
                ai_reasoning_json = excluded.ai_reasoning_json,
                scored_at = CURRENT_TIMESTAMP
            """,
            (
                attempt_id,
                total_score,
                total_score,
                max_total_score,
                percentage,
                grading_status,
                overall_feedback,
                overall_feedback,
                attempt.get("best_practice_reasoning") or "",
                attempt.get("recommended_response") or attempt.get("expected_customer_response") or "",
                attempt.get("takeaway_summary") or attempt.get("lesson_takeaway") or "",
                attempt.get("strengths") or "[]",
                attempt.get("missed_points") or "[]",
                attempt.get("missed_points") or "[]",
                default_style,
                attempt.get("module_scoring_config_json") or "{}",
                json.dumps({"questions": question_scores}),
                json.dumps({"module_instructions": attempt.get("llm_grader_instructions") or ""}),
            ),
        )

        execute(
            "UPDATE attempts SET total_score = ?, ai_feedback = ?, result_status = ? WHERE attempt_id = ?",
            (percentage, overall_feedback, grading_status, attempt_id),
        )
        return {
            "total_score": total_score,
            "max_total_score": max_total_score,
            "percentage": percentage,
            "overall_feedback": overall_feedback,
            "question_scores": question_scores,
            "grading_status": grading_status,
        }
    except Exception as exc:
        grading_service_logger.exception("Structured grading failed.", attempt_id=attempt_id)
        execute("UPDATE attempts SET result_status = 'grading_failed' WHERE attempt_id = ?", (attempt_id,))
        execute(
            """
            INSERT INTO submission_scores (attempt_id, grading_status, grading_error)
            VALUES (?, 'grading_failed', ?)
            ON CONFLICT(attempt_id) DO UPDATE SET grading_status = excluded.grading_status, grading_error = excluded.grading_error
            """,
            (attempt_id, str(exc)),
        )
        raise
