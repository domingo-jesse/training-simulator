from __future__ import annotations

import hashlib
import sys
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

import psycopg2
import streamlit as st
from admin_views import (
    render_admin_dashboard,
    render_assignment_management,
    render_database_tables_view,
    render_grading_center,
    render_learner_management,
    render_admin_log_viewer,
    render_manage_modules,
    render_module_builder,
    render_progress_tracking,
    render_admin_quality_hub,
)
from data_seed import clear_seed_data
from db import execute, fetch_all, fetch_one, get_database_debug_info, init_db
from learner_views import (
    render_learner_home,
    render_module_library,
    render_progress_page,
    render_results_page,
)
from logger import get_logger
from utils import inject_styles

app_logger = get_logger(module="app")


@contextmanager
def get_conn():
    database_url = st.secrets["DATABASE_URL"]

    parsed = urlparse(database_url)
    safe_user = parsed.username
    safe_host = parsed.hostname
    safe_port = parsed.port
    safe_db = parsed.path.lstrip("/")
    safe_query = parsed.query

    st.write("DB debug user:", safe_user)
    st.write("DB debug host:", safe_host)
    st.write("DB debug port:", safe_port)
    st.write("DB debug db:", safe_db)
    st.write("DB debug query:", safe_query)

    try:
        conn = psycopg2.connect(database_url, connect_timeout=10)
        yield conn
        conn.close()
    except Exception as exc:
        raise ConnectionError(
            f"{type(exc).__name__}: {exc}"
        ) from exc

EXPECTED_PLATFORM_TABLES = (
    "organizations",
    "users",
    "modules",
    "module_questions",
    "investigation_actions",
    "attempts",
    "submission_scores",
    "action_logs",
    "assignments",
    "learner_profiles",
    "module_assignments",
    "module_progress",
    "module_generation_runs",
    "module_generation_questions",
)


def _global_exception_handler(exc_type, exc_value, exc_traceback) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    app_logger.opt(exception=(exc_type, exc_value, exc_traceback)).error(
        "Unhandled exception reached global exception hook."
    )


sys.excepthook = _global_exception_handler

st.set_page_config(
    page_title="Training Simulator",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

def hash_password(password: str) -> str:
    """Demo-only hashing helper.

    NOTE: Replace with bcrypt/argon2 + per-user salts in production.
    """
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def init_state() -> None:
    defaults = {
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
        "page": None,
        "bootstrapped": False,
        "bootstrap_error": None,
        "admin_page": "Dashboard",
        "admin_nav_group": "Operations",
        "learner_page": "Home",
        "ui_event": None,
        "db_test_nonce": 0,
        "db_test_result": None,
        "quick_connect_result": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


@st.cache_resource
def initialize_once() -> bool:
    """Run database initialization once per process."""
    init_db()
    return True


def _ensure_platform_data() -> bool:
    if st.session_state.get("bootstrapped"):
        return True
    app_logger.info("Bootstrapping platform data.")
    try:
        with st.spinner("Preparing platform data..."):
            clear_seed_data()
        st.session_state["bootstrapped"] = True
        st.session_state["bootstrap_error"] = None
        app_logger.info("Platform bootstrap complete.")
        return True
    except Exception as exc:
        st.session_state["bootstrap_error"] = str(exc)
        app_logger.exception("Platform bootstrap failed.")
        return False


def _default_org_id() -> int:
    org = fetch_one("SELECT organization_id FROM organizations ORDER BY organization_id LIMIT 1")
    if org:
        return int(org["organization_id"])
    return int(execute("INSERT INTO organizations (name) VALUES (?)", ("Default Org",)))


def _normalize_role(role: str | None) -> str:
    role_norm = (role or "").strip().lower()
    return role_norm if role_norm in {"learner", "admin"} else "learner"


def _get_or_create_platform_user(auth_user: dict[str, Any]) -> dict[str, Any]:
    auth_user_id = auth_user.get("user_id")
    if auth_user_id:
        existing_by_id = fetch_one("SELECT * FROM users WHERE user_id = ? LIMIT 1", (auth_user_id,))
        if existing_by_id:
            return dict(existing_by_id)

    normalized_role = _normalize_role(auth_user.get("role"))
    existing = fetch_one(
        "SELECT * FROM users WHERE LOWER(email) = ? AND role = ? LIMIT 1",
        (auth_user["email"].strip().lower(), normalized_role),
    )
    if existing:
        app_logger.info("Using existing platform user.", user_email=auth_user["email"].strip().lower(), role=normalized_role)
        return dict(existing)

    org_id = _default_org_id()
    user_id = execute(
        """
        INSERT INTO users (name, email, role, team, organization_id, is_active)
        VALUES (?, ?, ?, ?, ?, TRUE)
        """,
        (auth_user["full_name"], auth_user["email"].strip().lower(), normalized_role, "General", org_id),
    )
    created = fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))
    app_logger.info("Created platform user.", user_email=auth_user["email"].strip().lower(), role=normalized_role, user_id=user_id)
    return dict(created)


def find_user_by_email(email: str, role: str | None = None) -> dict[str, Any] | None:
    email_norm = (email or "").strip().lower()
    role_norm = (role or "").strip().lower() or None
    if not email_norm:
        return None

    if role_norm:
        row = fetch_one(
            """
            SELECT user_id, id, role, name AS full_name, email, username, password_hash, auth_provider, is_active
            FROM users
            WHERE LOWER(email) = ? AND role = ? AND is_active = TRUE
            LIMIT 1
            """,
            (email_norm, role_norm),
        )
    else:
        row = fetch_one(
            """
            SELECT user_id, id, role, name AS full_name, email, username, password_hash, auth_provider, is_active
            FROM users
            WHERE LOWER(email) = ? AND is_active = TRUE
            LIMIT 1
            """,
            (email_norm,),
        )
    if not row:
        return None
    return _auth_user_from_row(dict(row))


def _auth_user_from_row(row: dict[str, Any]) -> dict[str, Any]:
    user = dict(row)
    if not user.get("id"):
        user["id"] = f"u_{user.get('role', 'learner')}_{int(user['user_id']):03d}"
    return user


def _find_auth_user_by_query(where_clause: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = fetch_one(
        f"""
        SELECT user_id, id, role, name AS full_name, email, username, password_hash, auth_provider, is_active
        FROM users
        WHERE {where_clause}
        LIMIT 1
        """,
        params,
    )
    if not row:
        return None
    return _auth_user_from_row(dict(row))


def find_user_by_username(username: str, role: str | None = None) -> dict[str, Any] | None:
    username_norm = (username or "").strip().lower()
    role_norm = (role or "").strip().lower() or None
    if not username_norm:
        return None
    if role_norm:
        return _find_auth_user_by_query(
            "LOWER(username) = ? AND role = ? AND is_active = TRUE",
            (username_norm, role_norm),
        )
    return _find_auth_user_by_query("LOWER(username) = ? AND is_active = TRUE", (username_norm,))


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


def validate_dev_login(identifier: str, expected_role: str) -> tuple[bool, str | None, dict[str, Any] | None]:
    """Passwordless login that only authenticates database users."""
    ident = (identifier or "").strip()
    if not ident:
        return False, "Enter your email or username to sign in.", None

    user = find_user_by_email(ident, role=expected_role)
    if user is None:
        user = find_user_by_username(ident, role=expected_role)
    if user is None:
        return False, f"No active {expected_role.title()} account exists yet. Create one first.", None
    return True, None, user


def validate_google_account(expected_role: str) -> tuple[bool, str | None, dict[str, Any] | None, str | None]:
    email = _google_user_email()
    if not email:
        return False, "Google sign-in succeeded but no email was returned.", None, None

    user = find_user_by_email(email, role=expected_role)
    if user is not None:
        return True, None, user, email

    existing_other_role = find_user_by_email(email, role=None)
    if existing_other_role:
        other_role = existing_other_role.get("role", "another").title()
        message = (
            f"This Google email is already linked to a {other_role} account. "
            f"Create a {expected_role.title()} account with Google or switch to the {other_role} tab."
        )
        return False, message, None, email

    return False, f"No {expected_role.title()} account exists yet. Create one instantly with Google.", None, email


def _sign_in_user(user: dict[str, Any], auth_method: str) -> None:
    normalized_role = _normalize_role(user.get("role"))
    platform_user = _get_or_create_platform_user(user)
    st.session_state["auth_authenticated"] = True
    st.session_state["auth_method"] = auth_method
    st.session_state["current_user"] = {
        "id": user["id"],
        "full_name": user["full_name"],
        "email": user["email"],
        "role": normalized_role,
        "user_id": platform_user["user_id"],
        "name": platform_user["name"],
        "organization_id": platform_user["organization_id"],
    }
    st.session_state["selected_role"] = normalized_role
    app_logger.bind(user_id=user["id"], session_id=st.session_state.get("session_id")).info(
        "User signed in.", auth_method=auth_method, role=normalized_role
    )
    st.session_state["page"] = None
    st.session_state["auth_error"] = None
    st.session_state["auth_info"] = None
    st.session_state["pending_google"] = None
    st.session_state["page"] = None
    st.session_state["active_module_id"] = None
    st.session_state["latest_attempt_id"] = None


def create_google_account(role: str, email: str, full_name: str) -> tuple[bool, str, dict[str, Any] | None]:
    role = (role or "").strip().lower()
    email = (email or "").strip().lower()
    full_name = (full_name or "").strip() or "Google User"

    if role not in {"learner", "admin"}:
        return False, "Please select a valid role.", None
    if not email:
        return False, "Google sign-in succeeded but no email was returned.", None

    existing = find_user_by_email(email, role=role)
    if existing:
        if existing.get("auth_provider") == "google":
            return True, f"Welcome back, {existing['full_name']}!", existing
        return False, f"You already have a {role.title()} account with this email. Sign in using your password.", None
    org_id = _default_org_id()
    user_id = execute(
        """
        INSERT INTO users (id, name, email, role, team, organization_id, auth_provider, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
        """,
        (f"u_{role}_{email}", full_name, email, role, "General", org_id, "google"),
    )
    new_user = find_user_by_email(email, role=role)
    if not new_user:
        created_row = fetch_one(
            """
            SELECT user_id, id, role, name AS full_name, email, username, password_hash, auth_provider, is_active
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        )
        new_user = _auth_user_from_row(dict(created_row)) if created_row else None
    app_logger.info("Created new Google-backed account.", role=role)
    return True, f"{role.title()} account created with Google.", new_user


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
    existing_role_user = find_user_by_email(email, role=role)
    if existing_role_user:
        return False, f"You already have a {role.title()} account with this email."
    if username and find_user_by_username(username):
        return False, "That username is already in use."
    if password != confirm_password:
        return False, "Passwords must match."

    org_id = _default_org_id()
    external_id = f"u_{role}_{email}"
    db_info = get_database_debug_info()
    app_logger.info(
        "Attempting local account creation.",
        role=role,
        email=email,
        db_backend=db_info.get("backend"),
        db_host=db_info.get("host"),
        db_name=db_info.get("database"),
        db_path=db_info.get("db_path"),
    )
    try:
        execute(
            """
            INSERT INTO users (id, name, email, role, team, organization_id, username, password_hash, auth_provider, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'local_password', TRUE)
            """,
            (
                external_id,
                full_name,
                email,
                role,
                "General",
                org_id,
                (username or None),
                hash_password(password),
            ),
        )
    except Exception as exc:
        error_text = str(exc).lower()
        app_logger.exception(
            "Local account creation failed.",
            role=role,
            email=email,
            db_backend=db_info.get("backend"),
            db_host=db_info.get("host"),
            db_name=db_info.get("database"),
            db_path=db_info.get("db_path"),
        )
        if "username" in error_text:
            return False, "That username is already in use."
        if "email" in error_text:
            return False, f"You already have a {role.title()} account with this email."
        if "name" in error_text:
            return False, "An account with that full name already exists. Try a slightly different full name."
        return False, "We couldn't create your account right now. Please try again."
    app_logger.info(
        "Created new local account.",
        role=role,
        email=email,
        db_backend=db_info.get("backend"),
    )
    return True, f"{role.title()} account created successfully. Please sign in."


def logout_user() -> None:
    if st.session_state.get("auth_method") == "google":
        st.logout()

    st.session_state["auth_authenticated"] = False
    st.session_state["auth_method"] = None
    st.session_state["current_user"] = None
    st.session_state["auth_error"] = None
    st.session_state["auth_info"] = None
    st.session_state["pending_google"] = None
    app_logger.info("User logged out.")


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
            .main .block-container {
                max-width: 860px;
                padding-top: 1.8rem;
                padding-bottom: 2.2rem;
            }
            .auth-wrap {
                max-width: 860px;
                margin: 0 auto 1rem auto;
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: 18px;
                box-shadow: 0 12px 40px rgba(16, 24, 40, 0.08);
                padding: 1.25rem 1.4rem 1.6rem;
            }
            .auth-title { font-size: 2rem; font-weight: 700; color: var(--text); margin-bottom: .2rem; }
            .auth-subtitle { color: var(--muted); margin-bottom: .3rem; }
            .auth-help { color: var(--muted); font-size: .95rem; margin-bottom: 0; }
            .divider {
                display: flex;
                align-items: center;
                color: #7a8294;
                font-size: .86rem;
                margin: .6rem 0 .9rem;
            }
            .divider::before, .divider::after {
                content: "";
                flex: 1;
                border-bottom: 1px solid var(--line);
            }
            .divider::before { margin-right: .55rem; }
            .divider::after { margin-left: .55rem; }
            [data-testid="stTabs"] {
                background: #ffffff;
                border: 1px solid var(--line);
                border-radius: 14px;
                padding: 0.65rem 0.85rem 0.85rem;
            }
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
    st.session_state["pending_google"] = {
        "email": email,
        "full_name": _google_user_name(),
        "expected_role": expected_role,
    }


def _run_database_connection_test() -> tuple[bool, str, list[str], list[str]]:
    nonce = int(st.session_state.get("db_test_nonce", 0))
    return _run_database_connection_test_cached(nonce)


@st.cache_data(ttl=120, show_spinner=False)
def _run_database_connection_test_cached(_nonce: int) -> tuple[bool, str, list[str], list[str]]:
    """Checks DB connectivity and presence of expected platform tables."""
    try:
        db_info = get_database_debug_info()
        if db_info["backend"] == "postgres":
            rows = fetch_all(
                """
                SELECT table_name AS name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type='BASE TABLE'
                """
            )
        else:
            rows = fetch_all("SELECT name FROM sqlite_master WHERE type='table'")
        discovered_tables = sorted(
            row["name"]
            for row in rows
            if not str(row["name"]).startswith("sqlite_")
        )
        expected = set(EXPECTED_PLATFORM_TABLES)
        discovered = set(discovered_tables)
        missing = sorted(expected - discovered)
        extra = sorted(discovered - expected)

        if missing:
            message = (
                f"Connected to database, but {len(missing)} expected table(s) are missing."
            )
            return False, message, missing, extra

        message = f"Database connection successful. Found all {len(EXPECTED_PLATFORM_TABLES)} expected tables."
        return True, message, [], extra
    except Exception as exc:
        app_logger.exception("Database connection test failed.")
        return False, f"Database connection test failed: {exc}", list(EXPECTED_PLATFORM_TABLES), []


@st.cache_data(ttl=120, show_spinner=False)
def _get_db_info_cached() -> dict[str, Any]:
    return get_database_debug_info()


def _set_ui_event(event_name: str) -> None:
    st.session_state["ui_event"] = event_name


def _render_database_connection_tester() -> None:
    st.markdown("#### Database Connection Tester")
    st.caption("Use this to verify the app can connect and detect the expected platform tables.")
    db_info = _get_db_info_cached()
    if db_info["backend"] == "postgres":
        st.code(
            (
                "Backend: postgres\n"
                f"Host: {db_info.get('host')}\n"
                f"Database: {db_info.get('database')}\n"
                f"Port: {db_info.get('port')}\n"
                f"Username: {db_info.get('username')}"
            )
        )
        if db_info.get("parse_error"):
            st.warning(f"DATABASE_URL format issue: {db_info['parse_error']}")
    else:
        st.code(f"Backend: sqlite\nPath: {db_info.get('db_path')}")

    with st.form("db_tools_form"):
        action_cols = st.columns(2)
        with action_cols[0]:
            quick_connect = st.form_submit_button("Quick connect check", use_container_width=True)
        with action_cols[1]:
            run_db_test = st.form_submit_button("Run database test", use_container_width=True)

    if quick_connect:
        with st.spinner("Checking direct database connectivity..."):
            cur = None
            conn = None
            try:
                conn = psycopg2.connect(
                    st.secrets["DATABASE_URL"],
                    connect_timeout=10,
                )
                cur = conn.cursor()
                cur.execute("SELECT version();")
                result = cur.fetchone()
                st.session_state["quick_connect_result"] = ("success", result)
            except Exception as exc:
                st.session_state["quick_connect_result"] = ("error", f"{type(exc).__name__}: {exc}")
            finally:
                try:
                    if cur:
                        cur.close()
                except Exception:
                    pass
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass

    if run_db_test:
        st.session_state["db_test_nonce"] = int(st.session_state.get("db_test_nonce", 0)) + 1
        with st.spinner("Inspecting database tables..."):
            st.session_state["db_test_result"] = _run_database_connection_test()

    quick_connect_result = st.session_state.get("quick_connect_result")
    if quick_connect_result:
        status, payload = quick_connect_result
        if status == "success":
            st.success("✅ Connected to database!")
            st.write(payload)
        else:
            st.error("❌ Connection failed")
            st.write(payload)

    db_test_result = st.session_state.get("db_test_result")
    if db_test_result:
        ok, message, missing, extra = db_test_result
        if ok:
            st.success(message)
        else:
            st.error(message)

        if missing:
            st.warning(f"Missing tables: {', '.join(missing)}")
        if extra:
            st.info(f"Additional tables detected: {', '.join(extra)}")


def render_login_view() -> None:
    app_logger.info("Rendering login view.", page="login")

    if st.session_state.get("bootstrap_error"):
        st.error(
            "Database bootstrap failed, so sign-in and app pages are unavailable right now. "
            "Open Database tools below and run the database test for details."
        )
        st.caption(f"Bootstrap error: {st.session_state['bootstrap_error']}")

    if st.session_state.get("bootstrap_error"):
        with st.expander("Database tools", expanded=True):
            _render_database_connection_tester()
        return

    _sync_google_identity_if_present()

    if st.session_state.get("post_create_success"):
        st.success(st.session_state["post_create_success"])
        st.session_state["post_create_success"] = None

    if st.session_state.get("auth_error"):
        st.error(st.session_state["auth_error"])

    pending = st.session_state.get("pending_google")
    if pending:
        pending_placeholder = st.empty()
        with pending_placeholder.container():
            st.info(f"Google account detected: {pending.get('email') or 'Unknown email'}")
            action_a, action_b = st.columns(2)
            with action_a:
                st.button(
                    "Send to database: Create account",
                    use_container_width=True,
                    key="pending_google_create",
                    on_click=_set_ui_event,
                    args=("pending_google_create",),
                )
            with action_b:
                st.button(
                    "Back to sign in",
                    use_container_width=True,
                    key="pending_google_back",
                    on_click=_set_ui_event,
                    args=("pending_google_back",),
                )

    learner_tab, admin_tab = st.tabs(["Learner", "Admin"])

    with learner_tab:
        with st.form("local_login_learner", clear_on_submit=False):
            identifier = st.text_input("Email or username", key="learner_identifier")
            submitted = st.form_submit_button("Sign in as Learner", use_container_width=True, type="primary")
            if submitted:
                app_logger.info("Login form submitted.", role="learner")
                ok, message, user = validate_dev_login(identifier, expected_role="learner")
                if ok and user:
                    _sign_in_user(user, "dev_quick")
                    st.rerun()
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                st.rerun()

        _render_google_button("learner")
        st.button(
            "Create account",
            key="create_link_learner",
            use_container_width=True,
            on_click=_set_ui_event,
            args=("create_link_learner",),
        )

    with admin_tab:
        with st.form("local_login_admin", clear_on_submit=False):
            identifier = st.text_input("Email or username", key="admin_identifier")
            submitted = st.form_submit_button("Sign in as Admin", use_container_width=True, type="primary")
            if submitted:
                app_logger.info("Login form submitted.", role="admin")
                ok, message, user = validate_dev_login(identifier, expected_role="admin")
                if ok and user:
                    _sign_in_user(user, "dev_quick")
                    st.rerun()
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                st.rerun()

        _render_google_button("admin")
        st.button(
            "Create account",
            key="create_link_admin",
            use_container_width=True,
            on_click=_set_ui_event,
            args=("create_link_admin",),
        )

    with st.expander("Database tools", expanded=False):
        _render_database_connection_tester()

    ui_event = st.session_state.get("ui_event")
    if not ui_event:
        return

    st.session_state["ui_event"] = None
    if ui_event == "pending_google_create":
        ok, message, user = create_google_account(
            role=pending.get("expected_role", "learner") if pending else "learner",
            email=pending.get("email", "") if pending else "",
            full_name=pending.get("full_name", "Google User") if pending else "Google User",
        )
        if ok and user:
            _sign_in_user(user, "google")
            st.session_state["post_create_success"] = message
            st.rerun()
        st.session_state["auth_error"] = message
        st.rerun()

    if ui_event == "pending_google_back":
        st.session_state["pending_google"] = None
        st.session_state["auth_error"] = None
        st.logout()

    if ui_event == "create_link_learner":
        st.session_state["auth_view"] = "create_account"
        st.session_state["selected_role"] = "learner"
        st.rerun()

    if ui_event == "create_link_admin":
        st.session_state["auth_view"] = "create_account"
        st.session_state["selected_role"] = "admin"
        st.rerun()



def render_create_account_view() -> None:
    app_logger.info("Rendering create-account view.", page="create_account")
    st.markdown("### Create your account")
    st.caption("You can register both Learner and Admin accounts using the same email address.")
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

        create_clicked = st.form_submit_button("Send to database: Create account", use_container_width=True, type="primary")

        if create_clicked:
            app_logger.info("Create account form submitted.", role=role)
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
    user_logger = app_logger.bind(
        user_id=user.get("id"),
        session_id=st.session_state.get("session_id"),
    )
    render_topbar(user)
    st.markdown("---")
    if user["role"] == "admin":
        operations_pages = [
            "Dashboard",
            "Assignment Management",
            "Submission Grading",
            "Progress Tracking",
            "Learner Management",
            "Module Builder",
            "Manage Modules",
            "Database Tables",
            "Debug Logs",
        ]
        qa_pages = ["QA Test Center"]
        all_pages = operations_pages + qa_pages

        requested_page = st.session_state.get("page")
        if requested_page in operations_pages:
            st.session_state["admin_nav_group"] = "Operations"
            st.session_state["admin_page"] = requested_page
        elif requested_page in qa_pages:
            st.session_state["admin_nav_group"] = "Quality Assurance"
            st.session_state["admin_page"] = requested_page

        st.radio(
            "Admin Workspace",
            options=["Operations", "Quality Assurance"],
            horizontal=True,
            key="admin_nav_group",
        )
        current_group = st.session_state.get("admin_nav_group", "Operations")
        visible_pages = operations_pages if current_group == "Operations" else qa_pages
        if st.session_state.get("admin_page") not in visible_pages:
            st.session_state["admin_page"] = visible_pages[0]
        st.session_state["page"] = None
        st.sidebar.radio(
            "Admin Navigation",
            options=visible_pages,
            key="admin_page",
        )
        current_page = st.session_state.get("admin_page", "Dashboard")
        user_logger.info("Admin page load.", page=current_page)
        if current_page == "Dashboard":
            render_admin_dashboard(user)
        elif current_page == "Assignment Management":
            render_assignment_management(user)
        elif current_page == "Submission Grading":
            render_grading_center(user)
        elif current_page == "Progress Tracking":
            render_progress_tracking(user)
        elif current_page == "Learner Management":
            render_learner_management(user)
        elif current_page == "Module Builder":
            render_module_builder(user)
        elif current_page == "Manage Modules":
            render_manage_modules(user)
        elif current_page == "Database Tables":
            render_database_tables_view()
        elif current_page == "Debug Logs":
            render_admin_log_viewer()
        elif current_page == "QA Test Center":
            render_admin_quality_hub(user)
    else:
        pages = ["Home", "Assigned Modules", "Module Workspace", "Results", "My Progress"]
        requested_page = st.session_state.get("page")
        if requested_page in pages and st.session_state.get("learner_page") != requested_page:
            st.session_state["learner_page"] = requested_page
        st.session_state["page"] = None
        st.sidebar.radio(
            "Learner Navigation",
            options=pages,
            key="learner_page",
        )
        current_page = st.session_state.get("learner_page", "Home")
        user_logger.info("Learner page load.", page=current_page)
        if current_page == "Home":
            render_learner_home(user)
        elif current_page == "Assigned Modules":
            render_module_library(user)
        elif current_page == "Module Workspace":
            render_module_library(user)
        elif current_page == "Results":
            render_results_page(user)
        elif current_page == "My Progress":
            render_progress_page(user)


def main() -> None:
    init_state()
    st.session_state.setdefault("session_id", st.session_state.get("session_id") or f"sess_{hashlib.md5(str(id(st.session_state)).encode()).hexdigest()[:12]}")
    app_logger.info("App startup.", session_id=st.session_state.get("session_id"))
    initialize_once()
    _ensure_platform_data()

    if st.session_state.get("auth_authenticated") and st.session_state.get("current_user"):
        inject_styles()
        render_main_app()
        return

    if st.session_state.get("auth_view") == "create_account":
        render_auth_shell(render_create_account_view)
    else:
        render_auth_shell(render_login_view)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        app_logger.exception("Unexpected exception during app execution.")
        raise
