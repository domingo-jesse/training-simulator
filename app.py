from __future__ import annotations

import hashlib
from typing import Any

import streamlit as st

st.set_page_config(
    page_title="Training Simulator",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def hash_password(password: str) -> str:
    """Demo-only hashing helper.

    NOTE: Replace with bcrypt/argon2 + per-user salts in production.
    """
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def load_demo_users() -> list[dict[str, Any]]:
    """Returns starter records for a prototype datastore.

    NOTE: This is intentionally in-memory for demo behavior.
    Replace this function with a DB read (Postgres/SQLite/Firestore/etc.).
    """
    return [
        {
            "id": "u_learner_001",
            "role": "learner",
            "full_name": "Avery Learner",
            "email": "learner@trainingsim.demo",
            "username": "avery",
            "password_hash": hash_password("LearnerPass123!"),
            "is_active": True,
        },
        {
            "id": "u_admin_001",
            "role": "admin",
            "full_name": "Jordan Admin",
            "email": "admin@trainingsim.demo",
            "username": "jadmin",
            "password_hash": hash_password("AdminPass123!"),
            "is_active": True,
        },
    ]


def init_state() -> None:
    defaults = {
        "users_db": load_demo_users(),
        "auth_authenticated": False,
        "auth_method": None,  # local_password | google
        "selected_role": "learner",
        "current_user": None,
        "auth_view": "login",  # login | create_account
        "auth_error": None,
        "auth_info": None,
        "post_create_success": None,
        "pending_google": None,
        "show_password": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def find_user_by_email(email: str) -> dict[str, Any] | None:
    email_norm = (email or "").strip().lower()
    return next(
        (
            user
            for user in st.session_state["users_db"]
            if user["email"].strip().lower() == email_norm and user.get("is_active", True)
        ),
        None,
    )


def find_user_by_username(username: str) -> dict[str, Any] | None:
    username_norm = (username or "").strip().lower()
    return next(
        (
            user
            for user in st.session_state["users_db"]
            if (user.get("username") or "").strip().lower() == username_norm and user.get("is_active", True)
        ),
        None,
    )


def _google_user_email() -> str | None:
    user_obj = getattr(st, "user", None)
    if not user_obj:
        return None

    # Safe fallback across possible st.user object shapes.
    for attr in ("email", "mail", "upn"):
        value = getattr(user_obj, attr, None)
        if value:
            return str(value).strip().lower()

    if isinstance(user_obj, dict):
        for key in ("email", "mail", "upn"):
            value = user_obj.get(key)
            if value:
                return str(value).strip().lower()

    return None


def _google_user_name() -> str:
    user_obj = getattr(st, "user", None)
    if not user_obj:
        return "Google User"

    for attr in ("name", "full_name", "display_name", "given_name"):
        value = getattr(user_obj, attr, None)
        if value:
            return str(value)

    if isinstance(user_obj, dict):
        for key in ("name", "full_name", "display_name", "given_name"):
            value = user_obj.get(key)
            if value:
                return str(value)

    return "Google User"


def validate_local_login(identifier: str, password: str, expected_role: str) -> tuple[bool, str | None, dict[str, Any] | None]:
    ident = (identifier or "").strip()
    if not ident or not password:
        return False, "Please enter both email/username and password.", None

    user = find_user_by_email(ident)
    if user is None:
        user = find_user_by_username(ident)
    if user is None:
        return False, "You do not have an account.", None

    if user["role"] != expected_role:
        return (
            False,
            f"This account is registered as {user['role'].title()}. Please sign in through the {user['role'].title()} area.",
            None,
        )

    if user["password_hash"] != hash_password(password):
        return False, "Incorrect email or password.", None

    return True, None, user


def validate_google_account(expected_role: str) -> tuple[bool, str | None, dict[str, Any] | None, str | None]:
    email = _google_user_email()
    if not email:
        return False, "Google sign-in succeeded but no email was returned.", None, None

    user = find_user_by_email(email)
    if user is None:
        return False, "You do not have an account.", None, email

    if user["role"] != expected_role:
        return (
            False,
            f"This Google account is registered as {user['role'].title()}. Please sign in through the {user['role'].title()} area.",
            None,
            email,
        )

    return True, None, user, email


def _sign_in_user(user: dict[str, Any], auth_method: str) -> None:
    st.session_state["auth_authenticated"] = True
    st.session_state["auth_method"] = auth_method
    st.session_state["current_user"] = {
        "id": user["id"],
        "full_name": user["full_name"],
        "email": user["email"],
        "role": user["role"],
    }
    st.session_state["selected_role"] = user["role"]
    st.session_state["auth_error"] = None
    st.session_state["auth_info"] = None
    st.session_state["pending_google"] = None


def create_account(
    role: str,
    full_name: str,
    email: str,
    username: str,
    password: str,
    confirm_password: str,
) -> tuple[bool, str]:
    role = (role or "").strip().lower()
    full_name = (full_name or "").strip()
    email = (email or "").strip().lower()
    username = (username or "").strip()

    if role not in {"learner", "admin"}:
        return False, "Please select a valid role."
    if not full_name or not email or not password or not confirm_password:
        return False, "Please complete all required fields."
    if "@" not in email or "." not in email.split("@")[-1]:
        return False, "Please enter a valid email address."
    if find_user_by_email(email):
        return False, "An account with this email already exists."
    if username and find_user_by_username(username):
        return False, "That username is already in use."
    if password != confirm_password:
        return False, "Passwords must match."

    new_user = {
        "id": f"u_{role}_{len(st.session_state['users_db']) + 1:03d}",
        "role": role,
        "full_name": full_name,
        "email": email,
        "username": username or None,
        "password_hash": hash_password(password),
        "is_active": True,
    }
    st.session_state["users_db"].append(new_user)
    return True, "Account created successfully. Please sign in."


def logout_user() -> None:
    if st.session_state.get("auth_method") == "google":
        st.logout()

    st.session_state["auth_authenticated"] = False
    st.session_state["auth_method"] = None
    st.session_state["current_user"] = None
    st.session_state["auth_error"] = None
    st.session_state["auth_info"] = None
    st.session_state["pending_google"] = None


def render_auth_shell(content_renderer) -> None:
    st.markdown(
        """
        <style>
            :root {
                --bg: #f4f6fb;
                --card: #ffffff;
                --line: #e7ebf3;
                --text: #121826;
                --muted: #5b6477;
                --brand: #2d6cdf;
                --brand-dark: #2357b2;
                --danger: #b42318;
                --success: #067647;
            }
            .stApp {
                background: radial-gradient(circle at top right, #edf2ff, var(--bg) 35%);
            }
            .auth-wrap {
                max-width: 980px;
                margin: 2rem auto;
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: 18px;
                box-shadow: 0 12px 40px rgba(16, 24, 40, 0.08);
                padding: 1.25rem 1.4rem 1.6rem;
            }
            .auth-title { font-size: 2rem; font-weight: 700; color: var(--text); margin-bottom: .2rem; }
            .auth-subtitle { color: var(--muted); margin-bottom: .3rem; }
            .auth-help { color: var(--muted); font-size: .95rem; margin-bottom: 1rem; }
            .divider {
                display: flex;
                align-items: center;
                color: #7a8294;
                font-size: .86rem;
                margin: .3rem 0 .8rem;
            }
            .divider::before, .divider::after {
                content: "";
                flex: 1;
                border-bottom: 1px solid var(--line);
            }
            .divider::before { margin-right: .55rem; }
            .divider::after { margin-left: .55rem; }
            .role-badge {
                display: inline-block;
                background: #e9f0ff;
                color: #1d4eb8;
                border-radius: 999px;
                padding: 4px 10px;
                font-size: .8rem;
                font-weight: 600;
            }
            .app-card {
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: 14px;
                padding: 1rem;
                min-height: 145px;
                box-shadow: 0 8px 24px rgba(16,24,40,0.06);
            }
        </style>
        <div class="auth-wrap">
          <div class="auth-title">Training Simulator</div>
          <div class="auth-subtitle">Enterprise Readiness Platform</div>
          <div class="auth-help">Use your existing account or create a new one to access Training Simulator.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Keep interactive widgets outside the injected HTML container.
    content_renderer()


def _render_google_button(role: str) -> None:
    st.markdown('<div class="divider">or continue with Google</div>', unsafe_allow_html=True)
    if st.button("Continue with Google", key=f"google_{role}", use_container_width=True):
        st.session_state["selected_role"] = role
        st.login("google")


def _sync_google_identity_if_present() -> None:
    if st.session_state.get("auth_authenticated"):
        return

    google_email = _google_user_email()
    if not google_email:
        return

    expected_role = st.session_state.get("selected_role", "learner")
    ok, message, user, email = validate_google_account(expected_role=expected_role)
    if ok and user:
        _sign_in_user(user, "google")
        st.rerun()

    st.session_state["auth_error"] = message
    st.session_state["pending_google"] = {"email": email, "expected_role": expected_role}


def render_login_view() -> None:
    _sync_google_identity_if_present()

    if st.session_state.get("post_create_success"):
        st.success(st.session_state["post_create_success"])
        st.session_state["post_create_success"] = None

    if st.session_state.get("auth_error"):
        st.error(st.session_state["auth_error"])

    pending = st.session_state.get("pending_google")
    if pending:
        st.info(f"Google account detected: {pending.get('email') or 'Unknown email'}")
        action_a, action_b = st.columns(2)
        with action_a:
            if st.button("Create account", use_container_width=True, key="pending_google_create"):
                st.session_state["auth_view"] = "create_account"
                st.session_state["auth_error"] = None
                st.rerun()
        with action_b:
            if st.button("Back to sign in", use_container_width=True, key="pending_google_back"):
                st.session_state["pending_google"] = None
                st.session_state["auth_error"] = None
                st.logout()

    learner_tab, admin_tab = st.tabs(["Learner", "Admin"])

    with learner_tab:
        st.session_state["selected_role"] = "learner"
        with st.form("local_login_learner", clear_on_submit=False):
            identifier = st.text_input("Email or username", key="learner_identifier")
            pwd = st.text_input("Password", type="password", key="learner_pwd")
            submitted = st.form_submit_button("Sign in as Learner", use_container_width=True, type="primary")
            if submitted:
                ok, message, user = validate_local_login(identifier, pwd, expected_role="learner")
                if ok and user:
                    _sign_in_user(user, "local_password")
                    st.rerun()
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                st.rerun()

        _render_google_button("learner")
        if st.button("Create account", key="create_link_learner", use_container_width=True):
            st.session_state["auth_view"] = "create_account"
            st.session_state["selected_role"] = "learner"
            st.rerun()

    with admin_tab:
        st.session_state["selected_role"] = "admin"
        with st.form("local_login_admin", clear_on_submit=False):
            identifier = st.text_input("Email or username", key="admin_identifier")
            pwd = st.text_input("Password", type="password", key="admin_pwd")
            submitted = st.form_submit_button("Sign in as Admin", use_container_width=True, type="primary")
            if submitted:
                ok, message, user = validate_local_login(identifier, pwd, expected_role="admin")
                if ok and user:
                    _sign_in_user(user, "local_password")
                    st.rerun()
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                st.rerun()

        _render_google_button("admin")
        if st.button("Create account", key="create_link_admin", use_container_width=True):
            st.session_state["auth_view"] = "create_account"
            st.session_state["selected_role"] = "admin"
            st.rerun()


def render_create_account_view() -> None:
    st.markdown("### Create your account")
    with st.form("create_account_form", clear_on_submit=False):
        role = st.radio(
            "Select role",
            options=["learner", "admin"],
            horizontal=True,
            index=0 if st.session_state.get("selected_role") != "admin" else 1,
            format_func=lambda r: r.title(),
        )
        full_name = st.text_input("Full name *")
        email = st.text_input("Email *")
        username = st.text_input("Username (optional)")
        password = st.text_input("Password *", type="password")
        confirm_password = st.text_input("Confirm password *", type="password")

        create_clicked = st.form_submit_button("Create account", use_container_width=True, type="primary")

        if create_clicked:
            ok, message = create_account(role, full_name, email, username, password, confirm_password)
            if ok:
                st.session_state["post_create_success"] = message
                st.session_state["auth_view"] = "login"
                st.session_state["selected_role"] = role
                st.session_state["auth_error"] = None
                st.rerun()
            st.error(message)

    if st.button("Back to sign in", use_container_width=True):
        st.session_state["auth_view"] = "login"
        st.session_state["auth_error"] = None
        st.session_state["pending_google"] = None
        st.rerun()


def render_topbar(user: dict[str, Any]) -> None:
    left, right = st.columns([4, 2])
    with left:
        st.title("Training Simulator")
        st.caption("Simulation workspace and readiness analytics")
    with right:
        st.markdown(f"**{user['full_name']}**")
        st.caption(user["email"])
        st.markdown(
            f"<span class='role-badge'>{user['role'].title()}</span>",
            unsafe_allow_html=True,
        )
        if st.button("Logout", use_container_width=True):
            logout_user()
            st.rerun()


def render_main_app() -> None:
    user = st.session_state["current_user"]
    render_topbar(user)

    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("<div class='app-card'><h4>Start Simulation</h4><p>Launch guided scenarios to practice real-world responses.</p></div>", unsafe_allow_html=True)
    with c2:
        st.markdown("<div class='app-card'><h4>Review Performance</h4><p>Inspect completion metrics, scoring trends, and feedback.</p></div>", unsafe_allow_html=True)
    with c3:
        if user["role"] == "admin":
            st.markdown("<div class='app-card'><h4>Admin Settings</h4><p>Configure users, permissions, simulation rules, and reporting.</p></div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='app-card'><h4>Learning Path</h4><p>Track assigned modules and upcoming simulation milestones.</p></div>", unsafe_allow_html=True)

    if user["role"] == "admin":
        st.info("Admin view enabled: organization-level controls and reporting are available.")
    else:
        st.success("Learner view enabled: continue training and monitor your progress.")


def main() -> None:
    init_state()

    if st.session_state.get("auth_authenticated") and st.session_state.get("current_user"):
        render_main_app()
        return

    if st.session_state.get("auth_view") == "create_account":
        render_auth_shell(render_create_account_view)
    else:
        render_auth_shell(render_login_view)


if __name__ == "__main__":
    main()
