from __future__ import annotations

from typing import Dict, List

from logger import get_logger

eval_logger = get_logger(module="evaluation")


def _keyword_score(answer: str, keywords: List[str]) -> int:
    answer_lower = (answer or "").lower()
    hits = sum(1 for keyword in keywords if keyword in answer_lower)
    return min(100, 45 + hits * 14)


def evaluate_submission(module: Dict, answers: Dict, actions_used: List[str]) -> Dict:
    """
    Placeholder scoring layer.

    TODO (future): Replace heuristic scoring with an LLM evaluator call that receives
    module context, learner answers, and action trace, then returns calibrated rubrics.
    """
    scenario_logger = eval_logger.bind(scenario_id=module.get("module_id") or module.get("title"))
    scenario_logger.info("OpenAI request start.", request_type="submission_evaluation")
    try:
        diagnosis = answers.get("diagnosis_answer", "")
        next_steps = answers.get("next_steps_answer", "")
        customer = answers.get("customer_response", "")

        understanding = _keyword_score(diagnosis, ["root", "cause", "config", "credential", "workflow", "mapping"])
        investigation = min(100, 40 + len(actions_used) * 10)
        solution_quality = _keyword_score(next_steps, ["validate", "rollback", "fix", "monitor", "requeue", "test"])
        communication = _keyword_score(customer, ["impact", "eta", "update", "thanks", "prevent", "timeline"])
    except Exception:
        scenario_logger.exception("OpenAI error while evaluating submission.")
        raise

    total_score = round((understanding + investigation + solution_quality + communication) / 4, 1)

    strengths = []
    missed = []
    category_rationales = {}

    if understanding >= 75:
        strengths.append("Good identification of the likely root cause and context.")
        category_rationales["understanding"] = "Diagnosis clearly tied observed evidence to a plausible root cause."
    else:
        missed.append("Diagnosis was not specific enough to the scenario mechanics.")
        category_rationales["understanding"] = "Diagnosis did not map strongly enough to concrete scenario mechanics."

    if investigation >= 70:
        strengths.append("Used investigation actions effectively before finalizing a response.")
        category_rationales["investigation"] = "You gathered enough evidence through investigation actions before finalizing."
    else:
        missed.append("Could improve evidence gathering by checking more investigation panels.")
        category_rationales["investigation"] = "Evidence collection was limited; more investigation actions were needed."

    if solution_quality >= 75:
        strengths.append("Proposed actionable next steps with operational follow-through.")
        category_rationales["solution_quality"] = "Next steps were concrete, validated, and operationally actionable."
    else:
        missed.append("Next steps should include concrete remediation and validation checks.")
        category_rationales["solution_quality"] = "Proposed remediation lacked enough implementation and validation detail."

    if communication >= 75:
        strengths.append("Customer communication is clear and confidence-building.")
        category_rationales["communication"] = "Response communicated impact and expectations in a confidence-building way."
    else:
        missed.append("Customer response should include impact, timing, and prevention language.")
        category_rationales["communication"] = "Response needed clearer impact, timeline, and prevention communication."

    result = {
        "scoring_version": "heuristic_v1",
        "total_score": total_score,
        "category_scores": {
            "understanding": understanding,
            "investigation": investigation,
            "solution_quality": solution_quality,
            "communication": communication,
        },
        "category_rationales": category_rationales,
        "scoring_engine": {
            "provider": "internal_heuristic",
            "model_name": "heuristic_rules_v1",
            "prompt_template_id": "heuristic_template_v1",
            "temperature": 0.0,
            "config": {"keyword_weight": 14, "base_score": 45},
        },
        "strengths": strengths,
        "missed_points": missed,
        "coaching_feedback": "Focus on connecting observed clues to a direct root-cause statement and prevention plan.",
        "best_practice_reasoning": module.get("expected_reasoning_path", "Follow a hypothesis-driven investigation and validate changes."),
        "recommended_response": module.get("expected_customer_response", "Acknowledge impact, explain next actions, and commit to follow-up."),
        "takeaway_summary": module.get("lesson_takeaway", "Use structured troubleshooting and communicate proactively."),
    }
    scenario_logger.info("OpenAI request completed.", request_type="submission_evaluation", total_score=total_score)
    return result
