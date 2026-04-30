"""BOTC tattoo concept mixer.

Generate five tattoo concepts from one or two Blood on the Clocktower roles
or from a free-form BOTC theme.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


TATTOO_STYLES = [
    "Fine-line blackwork",
    "Neo-traditional",
    "Dotwork / geometric",
    "Ignorant-style sketch",
    "Dark illustrative",
]


@dataclass(frozen=True)
class TattooConcept:
    style: str
    title: str
    image_prompt: str
    description: str


def _theme_from_inputs(role_one: str | None, role_two: str | None, theme: str | None) -> str:
    role_one = (role_one or "").strip()
    role_two = (role_two or "").strip()
    theme = (theme or "").strip()

    if theme:
        return theme
    if role_one and role_two:
        return f"fusion of BOTC roles '{role_one}' and '{role_two}'"
    if role_one:
        return f"BOTC role '{role_one}'"
    if role_two:
        return f"BOTC role '{role_two}'"
    return "mysterious BOTC town vs evil energy"


def build_tattoo_concepts(
    role_one: str | None = None,
    role_two: str | None = None,
    theme: str | None = None,
    styles: Iterable[str] = TATTOO_STYLES,
) -> list[TattooConcept]:
    """Build five style-specific BOTC tattoo concepts.

    Returns concept descriptions and model-ready prompts that can be sent to an
    image model.
    """

    core_theme = _theme_from_inputs(role_one, role_two, theme)
    concepts: list[TattooConcept] = []

    for idx, style in enumerate(styles, start=1):
        title = f"{style} Concept {idx}"
        image_prompt = (
            f"Tattoo flash design, {style.lower()} style, inspired by {core_theme}. "
            "Include symbolic clock motif, hidden storytelling details, high-contrast composition, "
            "clean stencil-ready silhouette, no text, white background, professional tattoo concept sheet."
        )
        description = (
            f"A {style.lower()} interpretation of {core_theme}, built around a broken clockface and layered "
            "symbols that suggest bluffing, deduction, and night/day tension from Blood on the Clocktower."
        )
        concepts.append(
            TattooConcept(
                style=style,
                title=title,
                image_prompt=image_prompt,
                description=description,
            )
        )

    return concepts


def render_markdown(concepts: list[TattooConcept]) -> str:
    """Render concepts to markdown for chat/UI display."""
    lines = ["## BOTC Tattoo Mixer Concepts"]
    for concept in concepts:
        lines.extend(
            [
                f"### {concept.title}",
                f"**Style:** {concept.style}",
                f"**AI Prompt:** {concept.image_prompt}",
                f"**Description:** {concept.description}",
            ]
        )
    return "\n\n".join(lines)


if __name__ == "__main__":
    sample = build_tattoo_concepts(role_one="Ravenkeeper", role_two="Poisoner")
    print(render_markdown(sample))
