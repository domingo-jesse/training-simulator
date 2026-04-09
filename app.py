from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components

from admin_views import (
    render_admin_dashboard,
    render_assignment_management,
    render_learner_management,
    render_module_builder,
    render_progress_tracking,
)
from data_seed import seed_all
from db import fetch_all, fetch_one, init_db
from learner_views import (
    render_learner_home,
    render_module_library,
    render_progress_page,
    render_results_page,
    render_scenario_page,
)
from utils import inject_styles

st.set_page_config(
    page_title="Training Simulator",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)


def bootstrap_data() -> None:
    init_db()
    seed_all()


def _active_users_by_role(role: str):
    return fetch_all(
        """
        SELECT user_id, name, email, role, team, organization_id, is_active
        FROM users
        WHERE role = ? AND is_active = 1
        ORDER BY name
        """,
        (role,),
    )


def _current_user() -> dict | None:
    user_id = st.session_state.get("user_id")
    if not user_id:
        return None

    user = fetch_one(
        """
        SELECT user_id, name, email, role, team, organization_id, is_active
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    )

    if not user or not user["is_active"]:
        st.session_state.pop("user_id", None)
        st.session_state.pop("role", None)
        return None

    return dict(user)


def _logout() -> None:
    for key in ["user_id", "role", "page", "active_module_id", "latest_attempt_id"]:
        st.session_state.pop(key, None)
    st.rerun()


def _login_panel(role: str) -> None:
    role_label = "Admin" if role == "admin" else "Learner"
    users = _active_users_by_role(role)

    st.markdown(f"### {role_label} Login")
    if not users:
        st.warning(f"No active {role_label.lower()} accounts found.")
        return

    user_map = {f"{u['name']} ({u['email']})": int(u["user_id"]) for u in users}
    selected = st.selectbox(f"Select {role_label.lower()} account", list(user_map.keys()), key=f"login_select_{role}")

    if st.button(f"Sign in as {role_label}", key=f"login_btn_{role}", type="primary", use_container_width=True):
        st.session_state["user_id"] = user_map[selected]
        st.session_state["role"] = role
        st.session_state["page"] = "Dashboard"
        st.rerun()

    st.caption("Or")
    _render_google_sign_in_button(role)


def _render_google_sign_in_button(role: str) -> None:
    """Render a Google sign-in button directly below each role login form."""
    components.html(
        f"""
        <div id="buttonDiv_{role}"></div>
        <script src="https://accounts.google.com/gsi/client" async defer></script>
        <script>
          function handleCredentialResponse(response) {{
            console.log("Encoded JWT ID token: " + response.credential);
          }}

          function renderGoogleButton() {{
            if (!window.google || !google.accounts || !google.accounts.id) {{
              return;
            }}

            google.accounts.id.initialize({{
              client_id: "YOUR_GOOGLE_CLIENT_ID",
              callback: handleCredentialResponse
            }});

            google.accounts.id.renderButton(
              document.getElementById("buttonDiv_{role}"),
              {{ theme: "outline", size: "large", width: 280 }}
            );
          }}

          window.onload = renderGoogleButton;
        </script>
        """,
        height=92,
    )


def render_login_screen() -> None:
    st.markdown("## Training Simulator")
    st.caption("Use separate role-based entry points for learners and admins.")

    learner_col, admin_col = st.columns(2)
    with learner_col:
        with st.container(border=True):
            _login_panel("learner")
    with admin_col:
        with st.container(border=True):
            _login_panel("admin")


def render_admin_app(user: dict) -> None:
    with st.sidebar:
        st.markdown("### Admin Console")
        st.caption(f"Signed in as **{user['name']}**")
        st.caption(user["email"])
        page = st.radio(
            "Navigation",
            ["Dashboard", "Module Builder", "Assignment Management", "Learner Management", "Progress Tracking"],
            key="admin_nav",
        )
        st.button("Log out", on_click=_logout, use_container_width=True)

    if page == "Dashboard":
        render_admin_dashboard(user)
    elif page == "Module Builder":
        render_module_builder(user)
    elif page == "Assignment Management":
        render_assignment_management(user)
    elif page == "Learner Management":
        render_learner_management(user)
    else:
        render_progress_tracking(user)


def render_learner_app(user: dict) -> None:
    with st.sidebar:
        st.markdown("### Learner Workspace")
        st.caption(f"Signed in as **{user['name']}**")
        st.caption(user["email"])
        page = st.radio(
            "Navigation",
            ["Home", "Module Library", "Scenario", "Results", "My Progress"],
            key="learner_nav",
        )
        st.button("Log out", on_click=_logout, use_container_width=True)

    if page == "Home":
        render_learner_home(user)
    elif page == "Module Library":
        render_module_library(user)
    elif page == "Scenario":
        render_scenario_page(user)
    elif page == "Results":
        render_results_page(user)
    else:
        render_progress_page(user)


def main() -> None:
    inject_styles()
    bootstrap_data()

    user = _current_user()
    if not user:
        render_login_screen()
        return

    if user["role"] == "admin":
        st.title("Training Simulator • Admin")
        render_admin_app(user)
        return

    st.title("Training Simulator • Learner")
    render_learner_app(user)


if __name__ == "__main__":
    main()
