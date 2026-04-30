"""Streamlit BOTC tattoo mixer app.

Generate five tattoo concepts from one/two Blood on the Clocktower roles
or from a custom theme, each in a different tattoo style.
"""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

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


def _theme_from_inputs(role_one: str, role_two: str, theme: str) -> str:
    role_one = role_one.strip()
    role_two = role_two.strip()
    theme = theme.strip()

    if theme:
        return theme
    if role_one and role_two:
        return f"fusion of BOTC roles '{role_one}' and '{role_two}'"
    if role_one:
        return f"BOTC role '{role_one}'"
    if role_two:
        return f"BOTC role '{role_two}'"
    return "mysterious BOTC town vs evil energy"


def build_tattoo_concepts(role_one: str, role_two: str, theme: str) -> list[TattooConcept]:
    core_theme = _theme_from_inputs(role_one, role_two, theme)
    concepts: list[TattooConcept] = []

    for idx, style in enumerate(TATTOO_STYLES, start=1):
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


def render_app() -> None:
    st.set_page_config(page_title="BOTC Tattoo Mixer", page_icon="🕰️", layout="wide")
    st.title("🕰️ BOTC Tattoo Mixer")
    st.caption("Enter up to two BOTC roles or a BOTC theme. Get 5 AI-ready tattoo ideas in different styles.")

    col1, col2, col3 = st.columns(3)
    with col1:
        role_one = st.text_input("Role 1", placeholder="e.g., Ravenkeeper")
    with col2:
        role_two = st.text_input("Role 2", placeholder="e.g., Poisoner")
    with col3:
        theme = st.text_input("Theme (optional)", placeholder="e.g., clocktower at midnight, bluff vs truth")

    if st.button("Generate 5 Tattoo Concepts", type="primary"):
        concepts = build_tattoo_concepts(role_one=role_one, role_two=role_two, theme=theme)
        st.success("Generated 5 BOTC tattoo concepts.")

        for concept in concepts:
            with st.container(border=True):
                st.subheader(concept.title)
                st.write(f"**Style:** {concept.style}")
                st.write(f"**Description:** {concept.description}")
                st.code(concept.image_prompt, language="text")


if __name__ == "__main__":
    render_app()
