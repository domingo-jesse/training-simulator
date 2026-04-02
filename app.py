from __future__ import annotations

import streamlit as st

from admin_views import (
    render_admin_overview,
    render_learner_performance,
    render_module_analytics,
    render_rankings,
    render_submission_review,
)
from data_seed import seed_all
from db import fetch_all, init_db
from learner_views import (
    render_learner_home,
    render_module_library,
    render_progress_page,
    render_results_page,
    render_scenario_page,
)
from utils import inject_styles

st.set_page_config(page_title="Troubleshooting Trainer", page_icon="🛠️", layout="wide")

init_db()
seed_all()
inject_styles()

st.title("🛠️ Troubleshooting Trainer")
st.caption("AI-powered simulation practice for issue investigation, diagnosis, and communication.")

users = fetch_all("SELECT * FROM users ORDER BY role, name")
learner_users = [u for u in users if u["role"] == "learner"]
admin_users = [u for u in users if u["role"] == "admin"]

with st.sidebar:
    st.markdown("### Workspace")
    mode = st.radio("Role-based experience", ["Learner Mode", "Admin Mode"])

    if mode == "Learner Mode":
        names = [u["name"] for u in learner_users]
        selected_name = st.selectbox("Signed in as", names)
        current_user = next(u for u in learner_users if u["name"] == selected_name)

        pages = ["Learner Home", "Module Library", "Scenario", "Results", "My Progress"]
        default_index = pages.index(st.session_state.get("page", "Learner Home")) if st.session_state.get("page", "Learner Home") in pages else 0
        page = st.radio("Navigate", pages, index=default_index)
        st.session_state.page = page

    else:
        admin_name = st.selectbox("Admin user", [u["name"] for u in admin_users])
        current_user = next(u for u in admin_users if u["name"] == admin_name)
        page = st.radio("Navigate", ["Admin Overview", "Learner Performance", "Submission Review", "Rankings", "Module Analytics"])

if mode == "Learner Mode":
    if page == "Learner Home":
        render_learner_home(current_user)
    elif page == "Module Library":
        render_module_library()
    elif page == "Scenario":
        render_scenario_page(current_user)
    elif page == "Results":
        render_results_page()
    else:
        render_progress_page(current_user)
else:
    if page == "Admin Overview":
        render_admin_overview()
    elif page == "Learner Performance":
        render_learner_performance()
    elif page == "Submission Review":
        render_submission_review()
    elif page == "Rankings":
        render_rankings()
    else:
        render_module_analytics()
