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
    question_count: int = 5


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
    question_count = min(10, max(1, payload.question_count))
    questions = []
    for idx in range(question_count):
        questions.append(
            {
                "question_text": f"How would you handle step {idx + 1} for: {seed_prompt}?",
                "question_type": "open_text",
                "answer_guidance": "Provide a concise, practical response with rationale and escalation criteria.",
                "rubric": "4 = complete and accurate; 3 = mostly complete; 2 = partial; 1 = minimal.",
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
    question_count = min(10, max(1, payload.question_count))
    prompt = (
        "Generate a complete module draft from the user's idea. "
        "Return strict JSON only with this exact schema and key names: "
        "{"
        '"title": string, "description": string, "scenario": string, '
        '"category": string, "difficulty": string, "time_limit_minutes": number, '
        '"questions": [{"question_text": string, "question_type": "open_text" | "multiple_choice", '
        '"answer_guidance": string, "rubric": string}], '
        '"overall_rubric": string'
        "}. "
        f"Generate {question_count} questions. Keep all text production-ready and practical.\n\n"
        f"User prompt: {payload.prompt}"
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return only valid JSON matching the requested schema."},
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
        if not isinstance(raw_questions, list) or not raw_questions:
            raise ValueError("questions must be a non-empty list")

        safe_questions = []
        for item in raw_questions[:question_count]:
            if not isinstance(item, dict):
                continue
            question_text = str(item.get("question_text", "")).strip()
            if not question_text:
                continue
            question_type = str(item.get("question_type", "open_text")).strip().lower()
            if question_type not in {"open_text", "multiple_choice"}:
                question_type = "open_text"
            safe_questions.append(
                {
                    "question_text": question_text,
                    "question_type": question_type,
                    "answer_guidance": str(item.get("answer_guidance", "")).strip(),
                    "rubric": str(item.get("rubric", "")).strip(),
                }
            )

        if not safe_questions:
            raise ValueError("OpenAI response did not include usable questions")

        output = {
            "title": str(draft.get("title") or "AI Draft Module").strip(),
            "description": str(draft.get("description") or "").strip(),
            "scenario": str(draft.get("scenario") or "").strip(),
            "category": str(draft.get("category") or "").strip(),
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
