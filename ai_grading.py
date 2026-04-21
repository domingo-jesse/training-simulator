from __future__ import annotations

from typing import Any

from grading_service import grade_submission


def grade_submission_with_ai(attempt_id: int) -> dict[str, Any]:
    return grade_submission(attempt_id)
