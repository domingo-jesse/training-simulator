from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from logger import get_logger

module_gen_logger = get_logger(module="module_generation")


@dataclass
class ModuleGenerationInput:
    title: str
    category: str
    difficulty: str
    description: str
    role_focus: str
    test_focus: str
    learning_objectives: list[str]
    scenario_constraints: str
    completion_requirements: str
    question_count: int


@dataclass
class ModuleDraftGenerationInput:
    prompt: str
    question_count: int = 3


def _fallback_preview(payload: ModuleGenerationInput) -> dict[str, Any]:
    question_count = min(10, max(0, payload.question_count))
    objectives = payload.learning_objectives or ["Demonstrate role-specific troubleshooting judgement"]
    questions = []
    for idx in range(question_count):
        objective = objectives[idx % len(objectives)]
        questions.append(
            {
                "question": f"How would you apply '{objective}' when handling {payload.role_focus or 'this role'} constraints?",
                "rationale": "Evaluates the learner's ability to operationalize objectives in a realistic scenario.",
            }
        )

    return {
        "title": payload.title or f"{payload.role_focus or 'Team'} Simulation",
        "description": payload.description
        or "AI-assisted preview built from admin goals, objectives, and scenario constraints.",
        "scenario_overview": (
            f"Learner acts as {payload.role_focus or 'a frontline operator'} and must satisfy: "
            f"{payload.test_focus or 'core operational goals'}. "
            f"Context constraints: {payload.scenario_constraints or 'Use available signals and stakeholder communication.'}"
        ),
        "questions": questions,
    }


def _openai_headers(api_key: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }


def _fallback_module_draft(payload: ModuleDraftGenerationInput) -> dict[str, Any]:
    seed_prompt = payload.prompt.strip() or "general training workflow"
    question_count = min(10, max(0, payload.question_count))
    questions = []
    for idx in range(question_count):
        questions.append(
            {
                "question_text": f"How would you handle step {idx + 1} for: {seed_prompt}?",
                "question_type": "open_text",
                "scoring_type": "llm",
                "llm_grading_criteria": (
                    "Evaluate sequencing, risk controls, communication quality, escalation judgment, and policy alignment."
                ),
                "keyword_expected_terms": [],
                "learner_visible_feedback_mode": "admin_approved_only",
                "answer_guidance": "Explain your approach with practical sequencing and risk controls.",
                "ai_conversation_prompt": "",
                "ai_role_or_persona": "",
                "evaluation_focus": "",
                "max_learner_responses": 3,
                "optional_wrap_up_instruction": "",
            }
        )

    return {
        "title": "AI Draft Module",
        "description": f"Draft module generated from: {seed_prompt}",
        "scenario": (
            "You are the primary responder handling the situation described by the author. "
            "Apply policy, communication standards, and safe escalation decisions."
        ),
        "category": "",
        "role_focus": "Frontline team member",
        "test_focus": "Policy adherence, communication quality, and escalation judgment",
        "learning_objectives": [
            "Apply policy correctly under pressure",
            "Use clear, empathetic communication with stakeholders",
            "Escalate appropriately with concise handoff context",
        ],
        "content_sections": [
            "Scenario context and constraints",
            "Decision checkpoints and escalation paths",
            "Communication best practices",
        ],
        "difficulty": "",
        "time_limit_minutes": 20,
        "questions": questions,
        "overall_rubric": "Evaluate clarity, policy alignment, empathy, and decision quality across all responses.",
    }


def generate_module_draft(payload: ModuleDraftGenerationInput) -> tuple[dict[str, Any], str | None]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _fallback_module_draft(payload), "OPENAI_API_KEY is not configured, so a local fallback module draft was generated."

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    question_count = min(10, max(0, payload.question_count))
    prompt = (
        "Generate a complete module draft from the user's idea. "
        "Return strict JSON only with this exact schema and key names: "
        "{"
        '"title": string, "description": string, "scenario": string, '
        '"category": string, "role_focus": string, "test_focus": string, '
        '"learning_objectives": string[], "content_sections": string[], '
        '"difficulty": string, "time_limit_minutes": number, '
        '"questions": [{"question_text": string, "question_type": "open_text" | "multiple_choice" | "ai_conversation", '
        '"scoring_type": "manual" | "keyword" | "llm", '
        '"keyword_expected_terms": string[], "llm_grading_criteria": string, '
        '"learner_visible_feedback_mode": "admin_approved_only" | "immediate", '
        '"answer_guidance": string, '
        '"choices": string[], '
        '"ai_conversation_prompt": string, "ai_role_or_persona": string, "evaluation_focus": string, '
        '"max_learner_responses": number, "optional_wrap_up_instruction": string}], '
        '"overall_rubric": string'
        "}. "
        f"Generate exactly {question_count} questions. "
        "If question_count is 0, return an empty questions array but still provide all other sections. "
        "Write naturally and clearly, avoid placeholder phrasing, avoid repetitive generic training language, "
        "and make content realistic, specific, and professionally written. "
        "Scenarios must include enough detail to be usable for training. "
        "Each question must be grounded in the scenario and practical. "
        "Answer guidance and ideal answers must be thoughtful and useful for real grading. "
        "Rubrics must include concrete evaluation criteria.\n\n"
        f"User prompt: {payload.prompt}"
    )

    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Return only valid JSON matching the requested schema. "
                    "Write with natural, professional language and concrete detail."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
    }

    http_request = request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers=_openai_headers(api_key),
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=60) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        content = parsed["choices"][0]["message"]["content"]
        draft = json.loads(content)

        raw_questions = draft.get("questions") or []
        if not isinstance(raw_questions, list):
            raise ValueError("questions must be a list")

        safe_questions = []
        for item in raw_questions[:question_count]:
            if not isinstance(item, dict):
                continue
            question_text = str(item.get("question_text", "")).strip()
            if not question_text:
                continue
            question_type = str(item.get("question_type", "open_text")).strip().lower()
            if question_type not in {"open_text", "multiple_choice", "ai_conversation"}:
                question_type = "open_text"
            scoring_type = str(item.get("scoring_type", "manual")).strip().lower()
            if scoring_type not in {"manual", "keyword", "llm"}:
                scoring_type = "manual"
            if question_type == "ai_conversation" and scoring_type == "keyword":
                scoring_type = "manual"
            if question_type == "multiple_choice" and scoring_type == "keyword":
                scoring_type = "manual"
            safe_questions.append(
                {
                    "question_text": question_text,
                    "question_type": question_type,
                    "scoring_type": scoring_type,
                    "keyword_expected_terms": item.get("keyword_expected_terms") if isinstance(item.get("keyword_expected_terms"), list) else [],
                    "llm_grading_criteria": str(item.get("llm_grading_criteria", "")).strip(),
                    "learner_visible_feedback_mode": str(item.get("learner_visible_feedback_mode") or "admin_approved_only").strip(),
                    "answer_guidance": str(item.get("answer_guidance", "")).strip(),
                    "choices": item.get("choices") if isinstance(item.get("choices"), list) else [],
                    "ai_conversation_prompt": str(item.get("ai_conversation_prompt", "")).strip(),
                    "ai_role_or_persona": str(item.get("ai_role_or_persona", "")).strip(),
                    "evaluation_focus": str(item.get("evaluation_focus", "")).strip(),
                    "max_learner_responses": 4 if int(item.get("max_learner_responses") or 3) == 4 else 3,
                    "optional_wrap_up_instruction": str(item.get("optional_wrap_up_instruction", "")).strip(),
                }
            )

        missing_questions = max(0, question_count - len(safe_questions))
        if question_count > 0 and missing_questions == question_count:
            raise ValueError("OpenAI response did not include usable questions")
        if missing_questions:
            for missing_idx in range(missing_questions):
                safe_questions.append(
                    {
                        "question_text": f"Question {len(safe_questions) + 1}: Add scenario-specific prompt.",
                        "question_type": "open_text",
                        "scoring_type": "manual",
                        "keyword_expected_terms": [],
                        "llm_grading_criteria": "",
                        "learner_visible_feedback_mode": "admin_approved_only",
                        "answer_guidance": "Add practical guidance with decision points and tradeoffs.",
                        "choices": [],
                        "ai_conversation_prompt": "",
                        "ai_role_or_persona": "",
                        "evaluation_focus": "",
                        "max_learner_responses": 3,
                        "optional_wrap_up_instruction": "",
                    }
                )

        output = {
            "title": str(draft.get("title") or "AI Draft Module").strip(),
            "description": str(draft.get("description") or "").strip(),
            "scenario": str(draft.get("scenario") or "").strip(),
            "category": str(draft.get("category") or "").strip(),
            "role_focus": str(draft.get("role_focus") or "").strip(),
            "test_focus": str(draft.get("test_focus") or "").strip(),
            "learning_objectives": [
                str(item).strip()
                for item in (draft.get("learning_objectives") if isinstance(draft.get("learning_objectives"), list) else [])
                if str(item).strip()
            ],
            "content_sections": [
                str(item).strip()
                for item in (draft.get("content_sections") if isinstance(draft.get("content_sections"), list) else [])
                if str(item).strip()
            ],
            "difficulty": str(draft.get("difficulty") or "").strip(),
            "time_limit_minutes": int(draft.get("time_limit_minutes") or 20),
            "questions": safe_questions,
            "overall_rubric": str(draft.get("overall_rubric") or "").strip(),
        }
        return output, None
    except (error.URLError, TimeoutError, KeyError, TypeError, json.JSONDecodeError, ValueError):
        module_gen_logger.exception("Failed to generate module draft with OpenAI.")
        return _fallback_module_draft(payload), "OpenAI call failed; a local fallback module draft was generated."


def generate_module_preview(payload: ModuleGenerationInput) -> tuple[dict[str, Any], str | None]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _fallback_preview(payload), "OPENAI_API_KEY is not configured, so a local fallback preview was generated."

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    prompt = (
        "You are helping an LMS admin generate a training module preview. "
        "Return strict JSON with keys: title, description, scenario_overview, questions. "
        "questions must be an array of objects with keys question and rationale. "
        "Generate exactly the requested number of questions, with practical scenario-based prompts.\n\n"
        f"Input payload: {json.dumps(payload.__dict__)}"
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
    }

    http_request = request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers=_openai_headers(api_key),
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=45) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        content = parsed["choices"][0]["message"]["content"]
        preview = json.loads(content)

        questions = preview.get("questions") or []
        if not isinstance(questions, list):
            raise ValueError("questions must be a list")

        safe_questions = []
        requested_count = min(10, max(0, payload.question_count))
        for item in questions[:requested_count]:
            if not isinstance(item, dict):
                continue
            q = str(item.get("question", "")).strip()
            if not q:
                continue
            safe_questions.append(
                {
                    "question": q,
                    "rationale": str(item.get("rationale", "")).strip(),
                }
            )

        if requested_count > 0 and not safe_questions:
            raise ValueError("OpenAI response did not include usable questions")

        output = {
            "title": str(preview.get("title") or payload.title or "AI Module").strip(),
            "description": str(preview.get("description") or payload.description or "").strip(),
            "scenario_overview": str(preview.get("scenario_overview") or "").strip(),
            "questions": safe_questions,
        }
        return output, None
    except (error.URLError, TimeoutError, KeyError, json.JSONDecodeError, ValueError):
        module_gen_logger.exception("Failed to generate module preview with OpenAI.")
        return _fallback_preview(payload), "OpenAI call failed; a local fallback preview was generated."
