from __future__ import annotations

import json
import re
from typing import Any

from db import execute, fetch_all, fetch_one
from logger import get_logger

grading_logger = get_logger(module="ai_grading")


def _keywords(text: str) -> list[str]:
    tokens = [t.strip().lower() for t in re.split(r"[^a-zA-Z0-9]+", text or "") if len(t.strip()) > 3]
    seen: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.append(token)
    return seen[:20]


def _grade_answer(*, learner_answer: str, expected_answer: str, rubric: str, max_points: float) -> dict[str, Any]:
    rubric_text = " ".join(part for part in [expected_answer, rubric] if part).strip()
    rubric_keywords = _keywords(rubric_text)
    answer_lower = (learner_answer or "").lower()

    if not rubric_keywords:
        coverage_ratio = 1.0 if learner_answer.strip() else 0.0
        missing: list[str] = []
    else:
        covered = [k for k in rubric_keywords if k in answer_lower]
        coverage_ratio = len(covered) / len(rubric_keywords)
        missing = [k for k in rubric_keywords if k not in answer_lower][:5]

    awarded_points = round(max(0.0, min(float(max_points), float(max_points) * coverage_ratio)), 2)
    reasoning = (
        f"Scored against expected answer/rubric using keyword coverage. "
        f"Coverage={round(coverage_ratio * 100, 1)}% based on rubric concepts."
    )
    feedback = (
        "Good progress. " if coverage_ratio >= 0.7 else "Needs improvement. "
    ) + (
        "Expand details tied to rubric expectations." if missing else "You covered most expected concepts."
    )

    return {
        "awarded_points": awarded_points,
        "max_points": float(max_points),
        "reasoning": reasoning,
        "feedback": feedback,
        "missing_elements": missing,
    }


def grade_submission_with_ai(attempt_id: int) -> dict[str, Any]:
    attempt = fetch_one(
        """
        SELECT a.*, m.organization_id AS module_org_id
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
               COALESCE(max_points, 10) AS max_points
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

    execute(
        "UPDATE attempts SET result_status = 'ai_grading' WHERE attempt_id = ?",
        (attempt_id,),
    )
    execute(
        """
        INSERT INTO submission_scores (attempt_id, grading_status)
        VALUES (?, 'ai_grading')
        ON CONFLICT(attempt_id) DO UPDATE SET grading_status = excluded.grading_status
        """,
        (attempt_id,),
    )

    try:
        question_scores: list[dict[str, Any]] = []
        total_score = 0.0
        max_total_score = 0.0

        for question in module_questions:
            question_id = int(question["question_id"])
            learner_answer = str(question_responses.get(str(question_id), "") or "")
            max_points = float(question.get("max_points") or 0)
            graded = _grade_answer(
                learner_answer=learner_answer,
                expected_answer=str(question.get("expected_answer") or ""),
                rubric=str(question.get("rubric") or ""),
                max_points=max_points,
            )
            total_score += float(graded["awarded_points"])
            max_total_score += max_points

            execute(
                """
                INSERT INTO submission_question_scores (
                    attempt_id, question_id, ai_score, final_score, feedback,
                    learner_answer, ai_awarded_points, ai_max_points,
                    ai_feedback, ai_reasoning, missing_key_concepts, final_awarded_points,
                    visible_to_learner, is_admin_override
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, FALSE)
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
                ),
            )
            question_scores.append({"question_id": question_id, **graded})

        percentage = round((total_score / max_total_score) * 100, 1) if max_total_score else None
        overall_feedback = (
            "Strong performance across rubric criteria."
            if (percentage or 0) >= 80
            else "Partial understanding shown; review missed concepts and add more detail."
        )

        execute(
            """
            INSERT INTO submission_scores (
                attempt_id, ai_total_score, final_total_score, max_total_score, percentage,
                grading_status, overall_ai_feedback, learner_visible_feedback
            )
            VALUES (?, ?, ?, ?, ?, 'ai_graded_pending_review', ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                ai_total_score = excluded.ai_total_score,
                final_total_score = COALESCE(submission_scores.admin_total_score, excluded.ai_total_score),
                max_total_score = excluded.max_total_score,
                percentage = excluded.percentage,
                grading_status = excluded.grading_status,
                overall_ai_feedback = excluded.overall_ai_feedback,
                learner_visible_feedback = COALESCE(submission_scores.learner_visible_feedback, excluded.learner_visible_feedback),
                scored_at = CURRENT_TIMESTAMP
            """,
            (attempt_id, total_score, total_score, max_total_score, percentage, overall_feedback, overall_feedback),
        )

        execute(
            """
            UPDATE attempts
            SET total_score = ?,
                ai_feedback = ?,
                result_status = 'ai_graded_pending_review'
            WHERE attempt_id = ?
            """,
            (percentage, overall_feedback, attempt_id),
        )

        return {
            "total_score": total_score,
            "max_total_score": max_total_score,
            "percentage": percentage,
            "overall_feedback": overall_feedback,
            "question_scores": question_scores,
        }
    except Exception:
        grading_logger.exception("AI grading failed.", attempt_id=attempt_id)
        execute(
            "UPDATE attempts SET result_status = 'grading_failed' WHERE attempt_id = ?",
            (attempt_id,),
        )
        execute(
            """
            INSERT INTO submission_scores (attempt_id, grading_status)
            VALUES (?, 'grading_failed')
            ON CONFLICT(attempt_id) DO UPDATE SET grading_status = excluded.grading_status
            """,
            (attempt_id,),
        )
        raise
