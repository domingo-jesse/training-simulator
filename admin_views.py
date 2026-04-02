from __future__ import annotations

import pandas as pd
import streamlit as st

from db import fetch_all
from utils import metric_row, parse_json_list, to_df


def _attempts_df() -> pd.DataFrame:
    rows = fetch_all(
        """
        SELECT a.*, u.name AS learner_name, u.team, m.title AS module_title, m.difficulty
        FROM attempts a
        JOIN users u ON a.user_id = u.user_id
        JOIN modules m ON a.module_id = m.module_id
        ORDER BY a.created_at DESC
        """
    )
    return to_df(rows)


def render_admin_overview() -> None:
    st.subheader("Admin Overview")
    attempts = _attempts_df()
    learners = to_df(fetch_all("SELECT * FROM users WHERE role='learner'"))
    modules = to_df(fetch_all("SELECT * FROM modules"))

    if attempts.empty:
        st.info("No attempts yet.")
        return

    avg_by_learner = attempts.groupby("learner_name", as_index=False)["total_score"].mean().sort_values("total_score", ascending=False)
    avg_by_module = attempts.groupby("module_title", as_index=False)["total_score"].mean().sort_values("total_score")

    most_missed = []
    for raw in attempts["missed_points"].tolist():
        most_missed.extend(parse_json_list(raw))
    most_missed_area = pd.Series(most_missed).value_counts().index[0] if most_missed else "N/A"

    completion_rate = round((attempts["module_id"].nunique() / len(modules)) * 100, 1) if len(modules) else 0

    metric_row(
        {
            "Total learners": len(learners),
            "Total attempts": len(attempts),
            "Average score": f"{round(attempts['total_score'].mean(), 1)}%",
            "Completion rate": f"{completion_rate}%",
            "Top learner": avg_by_learner.iloc[0]["learner_name"],
            "Most difficult module": avg_by_module.iloc[0]["module_title"],
            "Most missed skill area": most_missed_area[:35] + ("..." if len(most_missed_area) > 35 else ""),
        }
    )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Score distribution")
        st.bar_chart(attempts["total_score"], height=250)
    with c2:
        st.markdown("#### Module completion")
        st.bar_chart(attempts.groupby("module_title")["attempt_id"].count(), height=250)

    st.markdown("#### Common missed issue areas")
    if most_missed:
        st.dataframe(pd.Series(most_missed).value_counts().rename_axis("Area").reset_index(name="Count"), hide_index=True, use_container_width=True)


def render_learner_performance() -> None:
    st.subheader("Learner Performance")
    attempts = _attempts_df()
    if attempts.empty:
        st.info("No submissions to review.")
        return

    search = st.text_input("Search learner")
    if search:
        attempts = attempts[attempts["learner_name"].str.contains(search, case=False)]

    grouped = (
        attempts.groupby("learner_name", as_index=False)
        .agg(
            modules_completed=("module_id", "nunique"),
            average_score=("total_score", "mean"),
            last_activity=("created_at", "max"),
            recent_score=("total_score", "last"),
            first_score=("total_score", "first"),
        )
        .sort_values("average_score", ascending=False)
    )
    grouped["improvement"] = (grouped["recent_score"] - grouped["first_score"]).round(1)
    grouped["rank"] = grouped["average_score"].rank(ascending=False, method="dense").astype(int)
    grouped["percentile"] = (100 - grouped["average_score"].rank(pct=True).mul(100)).round(1)

    st.dataframe(grouped, hide_index=True, use_container_width=True)

    learner = st.selectbox("Open learner details", grouped["learner_name"].tolist())
    subset = attempts[attempts["learner_name"] == learner]
    st.markdown(f"#### {learner} details")
    st.dataframe(subset[["created_at", "module_title", "total_score", "understanding_score", "investigation_score", "solution_score", "communication_score"]], hide_index=True, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Strongest categories**")
        cat_means = subset[["understanding_score", "investigation_score", "solution_score", "communication_score"]].mean().sort_values(ascending=False)
        st.write(cat_means.head(2).to_dict())
    with col2:
        st.markdown("**Common weaknesses**")
        misses = []
        for raw in subset["missed_points"].tolist():
            misses.extend(parse_json_list(raw))
        st.write(pd.Series(misses).value_counts().head(5).to_dict() if misses else {})

    with st.expander("Written answers"):
        st.dataframe(subset[["module_title", "diagnosis_answer", "next_steps_answer", "customer_response", "ai_feedback"]], hide_index=True, use_container_width=True)


def render_submission_review() -> None:
    st.subheader("Submission Review")
    attempts = _attempts_df()
    if attempts.empty:
        st.info("No attempts found.")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        learner_filter = st.selectbox("Learner", ["All"] + sorted(attempts["learner_name"].unique().tolist()))
    with col2:
        module_filter = st.selectbox("Module", ["All"] + sorted(attempts["module_title"].unique().tolist()))
    with col3:
        diff_filter = st.selectbox("Difficulty", ["All"] + sorted(attempts["difficulty"].unique().tolist()))
    with col4:
        score_range = st.slider("Score range", 0, 100, (0, 100))

    filtered = attempts.copy()
    if learner_filter != "All":
        filtered = filtered[filtered["learner_name"] == learner_filter]
    if module_filter != "All":
        filtered = filtered[filtered["module_title"] == module_filter]
    if diff_filter != "All":
        filtered = filtered[filtered["difficulty"] == diff_filter]
    filtered = filtered[(filtered["total_score"] >= score_range[0]) & (filtered["total_score"] <= score_range[1])]

    st.dataframe(
        filtered[
            [
                "created_at",
                "learner_name",
                "module_title",
                "diagnosis_answer",
                "next_steps_answer",
                "customer_response",
                "total_score",
                "understanding_score",
                "investigation_score",
                "solution_score",
                "communication_score",
                "ai_feedback",
                "recommended_response",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Export filtered submissions to CSV",
        filtered.to_csv(index=False),
        file_name="submission_review.csv",
        mime="text/csv",
    )


def render_rankings() -> None:
    st.subheader("Rankings")
    attempts = _attempts_df()
    if attempts.empty:
        st.info("No ranking data yet.")
        return

    grouped = attempts.groupby("learner_name", as_index=False).agg(
        avg_score=("total_score", "mean"),
        highest_recent=("total_score", "max"),
        best_comm=("communication_score", "mean"),
        attempts=("attempt_id", "count"),
    )
    grouped["most_improved"] = grouped["highest_recent"] - grouped["avg_score"]
    grouped["fastest_completion"] = (100 / grouped["attempts"]).round(1)

    ranking_type = st.radio(
        "Leaderboard",
        ["Total average score", "Highest recent score", "Most improved", "Fastest completion", "Best communication score"],
        horizontal=True,
    )

    sort_map = {
        "Total average score": "avg_score",
        "Highest recent score": "highest_recent",
        "Most improved": "most_improved",
        "Fastest completion": "fastest_completion",
        "Best communication score": "best_comm",
    }

    ranked = grouped.sort_values(sort_map[ranking_type], ascending=False).reset_index(drop=True)
    ranked["Badge"] = ranked.index.map(lambda i: "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "")

    st.dataframe(ranked[["Badge", "learner_name", "avg_score", "highest_recent", "most_improved", "fastest_completion", "best_comm"]], hide_index=True, use_container_width=True)


def render_module_analytics() -> None:
    st.subheader("Module Analytics")
    attempts = _attempts_df()
    if attempts.empty:
        st.info("No module analytics yet.")
        return

    selected = st.selectbox("Select module", sorted(attempts["module_title"].unique().tolist()))
    subset = attempts[attempts["module_title"] == selected]

    metric_row(
        {
            "Attempts": len(subset),
            "Average score": f"{round(subset['total_score'].mean(), 1)}%",
            "Completion proxy": f"{subset['learner_name'].nunique()} learners",
            "Hardest category": subset[["understanding_score", "investigation_score", "solution_score", "communication_score"]].mean().idxmin().replace("_score", ""),
        }
    )

    misses = []
    wrong_dx = []
    for _, row in subset.iterrows():
        misses.extend(parse_json_list(row["missed_points"]))
        wrong_dx.append(row["diagnosis_answer"])

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Common missed clues")
        st.dataframe(pd.Series(misses).value_counts().head(8).rename_axis("Missed clue").reset_index(name="Count"), hide_index=True, use_container_width=True)
    with c2:
        st.markdown("#### Common wrong diagnoses")
        st.dataframe(pd.Series(wrong_dx).value_counts().head(8).rename_axis("Diagnosis").reset_index(name="Count"), hide_index=True, use_container_width=True)

    st.markdown("#### Average score by category")
    st.bar_chart(subset[["understanding_score", "investigation_score", "solution_score", "communication_score"]].mean())
