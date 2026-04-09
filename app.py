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


def _google_oauth_configured() -> bool:
    secrets = getattr(st, "secrets", None)
    if secrets is None:
        return False

    try:
        auth_config = secrets.get("auth")
        google_config = auth_config.get("google") if auth_config else None
    except Exception:
        return False

    if not google_config:
        return False

    return bool(google_config.get("client_id") and google_config.get("client_secret"))


def _render_google_login() -> bool:
    login_fn = getattr(st, "login", None)
    if not callable(login_fn):
        return False

    st.markdown("### Authentication")
    st.caption("Use your Google Workspace account to sign in.")

    if not _google_oauth_configured():
        st.info("Google sign-in is not configured for this deployment. Using local mode.")
        return False

    if st.button("Sign in with Google", key="google_sign_in_button", use_container_width=True):
        try:
            login_fn("google")
        except Exception:
            st.error("Google sign-in is unavailable right now. Continue in local mode.")
            return False
    return True


user_by_name = {u["name"]: u for u in users}
user_by_id = {u["user_id"]: u for u in users}
user_by_email = {u["email"].lower(): u for u in users if u["email"]}
google_email, google_name, google_subject = _google_identity()

if "local_user_id" not in st.session_state:
    st.session_state.local_user_id = None


def _reset_local_login() -> None:
    st.session_state.local_user_id = None


def _render_login_page() -> dict | None:
    st.markdown("## Login")
    st.caption("Choose a sign-in method to access your workspace.")

    if google_email:
        mapped_user = user_by_email.get(google_email)
        if mapped_user:
            if google_subject and google_subject != mapped_user["google_subject"]:
                execute("UPDATE users SET google_subject = ? WHERE user_id = ?", (google_subject, mapped_user["user_id"]))
            st.success(f"Signed in with Google as {mapped_user['name']}")
            return mapped_user

    local_user = user_by_id.get(st.session_state.local_user_id)
    if local_user:
        return local_user

    google_tab, local_tab = st.tabs(["Google Sign-In", "Workspace Login"])

    with google_tab:
        google_login_available = _render_google_login()
        if google_email and not user_by_email.get(google_email):
            st.warning(
                f"{google_email} is authenticated with Google, but no active user is mapped in this workspace."
            )
            if google_name:
                st.caption(f"Google account: {google_name}")
        elif google_login_available:
            st.caption("Tip: add matching user emails in the admin DB to enable automatic Google sign-in mapping.")

    with local_tab:
        st.markdown("### Local workspace login")
        user_names = list(user_by_name.keys())
        selected_name = st.selectbox("Select your workspace profile", user_names, key="local_mode_user_select")
        if st.button("Continue to workspace", key="local_sign_in_button", use_container_width=True):
            selected_user = user_by_name[selected_name]
            st.session_state.local_user_id = selected_user["user_id"]
            st.rerun()

    return None


current_user = _render_login_page()
if not current_user:
    st.stop()

with st.sidebar:
    st.markdown("### Workspace")
    st.success(f"Signed in as {current_user['name']}")
    st.caption(current_user["email"] or "")

    logout_fn = getattr(st, "logout", None)
    is_google_user = bool(google_email and current_user.get("email") and google_email == current_user["email"].lower())

    if is_google_user and callable(logout_fn):
        if st.button("Sign out of Google", key="google_sign_out_button", use_container_width=True):
            logout_fn()
            st.rerun()
    else:
        if st.button("Sign out of workspace", key="local_sign_out_button", use_container_width=True):
            _reset_local_login()
            st.rerun()

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
