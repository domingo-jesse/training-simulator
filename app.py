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
from db import execute, fetch_all, init_db
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


def _google_identity() -> tuple[str | None, str | None, str | None]:
    user_obj = getattr(st, "user", None)
    if not user_obj:
        return None, None, None

    is_logged_in = getattr(user_obj, "is_logged_in", False)
    if not is_logged_in:
        return None, None, None

    email = user_obj.get("email")
    full_name = user_obj.get("name")
    subject = user_obj.get("sub")
    return email.lower() if email else None, full_name, subject


def _render_google_login() -> bool:
    login_fn = getattr(st, "login", None)
    if not callable(login_fn):
        return False

    st.markdown("### Authentication")
    st.caption("Use your Google Workspace account to sign in.")
    if st.button("Sign in with Google", key="google_sign_in_button", use_container_width=True):
        login_fn("google")
    return True


user_by_name = {u["name"]: u for u in users}
user_by_email = {u["email"].lower(): u for u in users if u["email"]}
google_email, google_name, google_subject = _google_identity()

with st.sidebar:
    st.markdown("### Workspace")
    current_user = user_by_email.get(google_email) if google_email else None

    if current_user:
        st.success(f"Signed in as {current_user['name']}")
        st.caption(current_user["email"] or "")

        if google_subject and google_subject != current_user["google_subject"]:
            execute("UPDATE users SET google_subject = ? WHERE user_id = ?", (google_subject, current_user["user_id"]))

        logout_fn = getattr(st, "logout", None)
        if callable(logout_fn):
            if st.button("Sign out", key="google_sign_out_button", use_container_width=True):
                logout_fn()
                st.rerun()
    else:
        google_login_available = _render_google_login()
        if google_email and not current_user:
            st.warning(
                f"{google_email} is authenticated with Google, but no active user is mapped in this workspace."
            )
            if google_name:
                st.caption(f"Google account: {google_name}")

        st.markdown("---")
        user_names = list(user_by_name.keys())
        selected_name = st.selectbox("Continue in local mode as", user_names, key="local_mode_user_select")
        current_user = user_by_name[selected_name]
        if google_login_available:
            st.caption("Tip: add matching user emails in the admin DB to enable automatic Google sign-in mapping.")

    if current_user["role"] == "admin":
        pages = ["Dashboard", "Learner Management", "Assignment Management", "Progress Tracking", "Module Builder"]
        page = st.radio("Navigate", pages, key="admin_nav_radio")
        st.caption("Admin controls are scoped to your organization.")
    else:
        pages = ["Learner Home", "Module Library", "Scenario", "Results", "My Progress"]
        default_index = pages.index(st.session_state.get("page", "Learner Home")) if st.session_state.get("page", "Learner Home") in pages else 0
        page = st.radio("Navigate", pages, index=default_index, key="learner_nav_radio")
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
