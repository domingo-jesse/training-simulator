from __future__ import annotations

import streamlit as st

from admin_views import (
    render_admin_dashboard,
    render_assignment_management,
    render_learner_management,
    render_module_builder,
    render_progress_tracking,
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

users = fetch_all("SELECT * FROM users WHERE is_active = 1 ORDER BY role, name")
user_names = [u["name"] for u in users]

with st.sidebar:
    st.markdown("### Workspace")
    selected_name = st.selectbox("Signed in as", user_names)
    current_user = next(u for u in users if u["name"] == selected_name)

    if current_user["role"] == "admin":
        pages = ["Dashboard", "Learner Management", "Assignment Management", "Progress Tracking", "Module Builder"]
        page = st.radio("Navigate", pages)
        st.caption("Admin controls are scoped to your organization.")
    else:
        pages = ["Learner Home", "Module Library", "Scenario", "Results", "My Progress"]
        default_index = pages.index(st.session_state.get("page", "Learner Home")) if st.session_state.get("page", "Learner Home") in pages else 0
        page = st.radio("Navigate", pages, index=default_index)
        st.session_state.page = page

if current_user["role"] == "admin":
    if page == "Dashboard":
        render_admin_dashboard(current_user)
    elif page == "Learner Management":
        render_learner_management(current_user)
    elif page == "Assignment Management":
        render_assignment_management(current_user)
    elif page == "Progress Tracking":
        render_progress_tracking(current_user)
    else:
        render_module_builder(current_user)
else:
    if page == "Learner Home":
        render_learner_home(current_user)
    elif page == "Module Library":
        render_module_library(current_user)
    elif page == "Scenario":
        render_scenario_page(current_user)
    elif page == "Results":
        render_results_page(current_user)
    else:
        render_progress_page(current_user)
