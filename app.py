from __future__ import annotations

import hashlib
import re
import sys
from html import escape
from typing import Any

import streamlit as st
from admin_views import (
    render_admin_dashboard,
    render_admin_assignment_review,
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
from db import execute, execute_update, fetch_all, fetch_one, get_database_debug_info, init_db
from learner_views import (
    render_learner_home,
    render_module_library,
    render_progress_results_page,
    render_scenario_page,
)
from logger import get_logger
from permissions import is_dev_account
from utils import inject_scroll_to_top, inject_styles, page_container, render_page_header

DEBUG = False  # set to True only when debugging

app_logger = get_logger(module="app")

EXPECTED_PLATFORM_TABLES = (
    "organizations",
    "users",
    "modules",
    "module_questions",
    "investigation_actions",
    "attempts",
    "submission_scores",
    "submission_question_scores",
    "action_logs",
    "assignments",
    "learner_profiles",
    "module_assignments",
    "module_progress",
    "module_generation_runs",
    "module_generation_questions",
    "assignment_workspace_state",
    "module_rubric_criteria",
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

ADMIN_PAGE_TO_NAV = {
    "Dashboard": "dashboard",
    "Assignment Management": "assignment-management",
    "Submission Grading": "submission-grading",
    "Progress Tracking": "progress-tracking",
    "Learner Management": "learner-management",
    "Module Builder": "module-builder",
    "Manage Modules": "manage-modules",
    "Database Tables": "database-tables",
    "Debug Logs": "debug-logs",
    "QA Test Center": "qa-test-center",
    "Profile": "profile",
    "Settings": "settings",
}
NAV_TO_ADMIN_PAGE = {value: key for key, value in ADMIN_PAGE_TO_NAV.items()}
DEV_ONLY_ADMIN_PAGES = {"Database Tables", "Debug Logs", "QA Test Center"}
DEV_ONLY_ADMIN_NAV_SLUGS = {ADMIN_PAGE_TO_NAV[name] for name in DEV_ONLY_ADMIN_PAGES}

LEARNER_NAV_CONFIG = {
    "home": "Home",
    "assigned_modules": "Assigned Modules",
    "module_workspace": "Module Workspace",
    "results": "Progress & Results",
    "database_tables": "Database Tables",
    "debug_logs": "Debug Logs",
}
LEARNER_PAGE_TO_NAV = {
    "home": "home",
    "assigned_modules": "assigned-modules",
    "module_workspace": "module-workspace",
    "results": "progress-results",
    "database_tables": "database-tables",
    "debug_logs": "debug-logs",
    "profile": "profile",
    "settings": "settings",
}
NAV_TO_LEARNER_PAGE = {value: key for key, value in LEARNER_PAGE_TO_NAV.items()}
NAV_TO_LEARNER_PAGE["results"] = "results"
NAV_TO_LEARNER_PAGE["my-progress"] = "results"
DEV_ONLY_LEARNER_PAGES = {"database_tables", "debug_logs"}
DEV_ONLY_LEARNER_NAV_SLUGS = {LEARNER_PAGE_TO_NAV[name] for name in DEV_ONLY_LEARNER_PAGES}
ADMIN_MAIN_NAV_SLUGS = {
    slug for slug in ADMIN_PAGE_TO_NAV.values() if slug not in {"profile", "settings"}
}
LEARNER_MAIN_NAV_SLUGS = {
    slug for slug in LEARNER_PAGE_TO_NAV.values() if slug not in {"profile", "settings"}
}


def _normalize_nav_slug(value: Any) -> str:
    if isinstance(value, list):
        value = value[0] if value else None
    slug = re.sub(r"[^a-z0-9-]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "dashboard"


def _read_nav_from_query_params() -> str:
    return _normalize_nav_slug(st.query_params.get("page"))


def _read_assignment_id_from_query_params() -> int | None:
    value = st.query_params.get("assignment_id")
    if isinstance(value, list):
        value = value[0] if value else None
    try:
        parsed = int(value) if value is not None and str(value).strip() else None
        if parsed is None or parsed <= 0:
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _set_nav(slug: str) -> None:
    nav_slug = _normalize_nav_slug(slug)
    st.session_state["nav"] = nav_slug
    st.query_params["page"] = nav_slug


def _set_nav_for_page(page_name: str, role: str) -> None:
    if role == "admin":
        slug = ADMIN_PAGE_TO_NAV.get(page_name, "dashboard")
    else:
        slug = LEARNER_PAGE_TO_NAV.get(page_name, "home")
    _set_nav(slug)


def _default_main_nav_for_role(role: str) -> str:
    return "dashboard" if role == "admin" else "home"


def _build_main_page_key(role: str, nav_slug: str) -> str:
    return f"{role}:{_normalize_nav_slug(nav_slug)}"


def _is_valid_main_page_key(page_key: str | None) -> bool:
    if not page_key or ":" not in str(page_key):
        return False
    role, nav_slug = str(page_key).split(":", 1)
    if role == "admin":
        user = st.session_state.get("current_user") or {}
        if nav_slug in DEV_ONLY_ADMIN_NAV_SLUGS and not is_dev_account(user):
            return False
        return nav_slug in ADMIN_MAIN_NAV_SLUGS
    if role == "learner":
        user = st.session_state.get("current_user") or {}
        if nav_slug in DEV_ONLY_LEARNER_NAV_SLUGS and not is_dev_account(user):
            return False
        return nav_slug in LEARNER_MAIN_NAV_SLUGS
    return False


def _navigate_to_account_page(page_key: str) -> None:
    current_page = st.session_state.get("current_page")
    if _is_valid_main_page_key(current_page):
        st.session_state["previous_page"] = current_page
    st.session_state["current_page"] = page_key
    st.session_state["page"] = page_key.title()
    _set_nav(page_key)
    st.rerun()


def _navigate_back_to_main_app() -> None:
    user = st.session_state.get("current_user") or {}
    role = _normalize_role(user.get("role"))
    fallback_nav = _default_main_nav_for_role(role)
    target_key = st.session_state.get("previous_page")
    if not _is_valid_main_page_key(target_key):
        target_key = _build_main_page_key(role, fallback_nav)
    target_role, target_nav = target_key.split(":", 1)
    st.session_state["current_page"] = target_key
    st.session_state["page"] = None
    if target_role == "admin":
        st.session_state["admin_page"] = NAV_TO_ADMIN_PAGE.get(target_nav, "Dashboard")
    else:
        st.session_state["learner_page"] = NAV_TO_LEARNER_PAGE.get(target_nav, "home")
    _set_nav(target_nav)
    st.rerun()


def _sync_current_page_with_query(role: str) -> str:
    default_nav = _default_main_nav_for_role(role)
    if role == "admin":
        user = st.session_state.get("current_user") or {}
        main_slugs = ADMIN_MAIN_NAV_SLUGS - DEV_ONLY_ADMIN_NAV_SLUGS if not is_dev_account(user) else ADMIN_MAIN_NAV_SLUGS
    else:
        user = st.session_state.get("current_user") or {}
        main_slugs = LEARNER_MAIN_NAV_SLUGS - DEV_ONLY_LEARNER_NAV_SLUGS if not is_dev_account(user) else LEARNER_MAIN_NAV_SLUGS
    query_slug = _read_nav_from_query_params()
    active_key = st.session_state.get("current_page")

    if query_slug in {"profile", "settings"}:
        st.session_state["current_page"] = query_slug
        st.session_state["nav"] = query_slug
        return query_slug

    if query_slug in main_slugs:
        resolved_slug = query_slug
    elif _is_valid_main_page_key(active_key):
        key_role, key_slug = str(active_key).split(":", 1)
        resolved_slug = key_slug if key_role == role and key_slug in main_slugs else default_nav
    else:
        resolved_slug = default_nav

    page_key = _build_main_page_key(role, resolved_slug)
    if st.session_state.get("current_page") != page_key:
        st.session_state["current_page"] = page_key
    st.session_state["nav"] = resolved_slug
    if st.query_params.get("page") != resolved_slug:
        st.query_params["page"] = resolved_slug
    return resolved_slug


def _get_current_main_nav_slug(role: str) -> str:
    default_nav = _default_main_nav_for_role(role)
    main_slugs = ADMIN_MAIN_NAV_SLUGS if role == "admin" else LEARNER_MAIN_NAV_SLUGS
    active_key = st.session_state.get("current_page")
    if _is_valid_main_page_key(active_key):
        key_role, key_slug = str(active_key).split(":", 1)
        if key_role == role and key_slug in main_slugs:
            return key_slug
    return default_nav


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
        "nav": "dashboard",
        "bootstrapped": False,
        "bootstrap_error": None,
        "admin_page": "Dashboard",
        "admin_nav_group": "Operations",
        "learner_page": "home",
        "ui_event": None,
        "db_test_nonce": 0,
        "db_test_result": None,
        "quick_connect_result": None,
        "profile_feedback": None,
        "profile_form_initialized_for": None,
        "active_assignment_id": None,
        "current_page": "learner:home",
        "previous_page": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _increment_rerun_cycle() -> int:
    cycle = int(st.session_state.get("_rerun_cycle", 0)) + 1
    st.session_state["_rerun_cycle"] = cycle
    st.session_state["_render_log_seen"] = set()
    return cycle


def _log_render_debug(event: str, **context: Any) -> None:
    cycle = int(st.session_state.get("_rerun_cycle", 0))
    dedupe_key = f"{cycle}:{event}:{tuple(sorted(context.items()))}"
    seen = st.session_state.setdefault("_render_log_seen", set())
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)
    app_logger.debug(event, **context)


def render_horizontal_button_group(
    label: str,
    options: list[str],
    state_key: str,
    *,
    container=st,
    format_func=None,
    layout: str = "horizontal",
) -> str:
    if not options:
        return ""

    if st.session_state.get(state_key) not in options:
        st.session_state[state_key] = options[0]

    if label:
        container.markdown(f"**{label}**")

    if layout == "vertical":
        for index, option in enumerate(options):
            button_label = format_func(option) if format_func else str(option)
            button_type = "primary" if st.session_state.get(state_key) == option else "secondary"
            if container.button(
                button_label,
                key=f"{state_key}_btn_{index}",
                type=button_type,
                use_container_width=True,
            ):
                st.session_state[state_key] = option
    else:
        columns = container.columns(len(options), gap="small")
        for index, option in enumerate(options):
            button_label = format_func(option) if format_func else str(option)
            button_type = "primary" if st.session_state.get(state_key) == option else "secondary"
            if columns[index].button(
                button_label,
                key=f"{state_key}_btn_{index}",
                type=button_type,
                use_container_width=True,
            ):
                st.session_state[state_key] = option

    return st.session_state[state_key]


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
    return int(execute("INSERT INTO organizations (name) VALUES (?) RETURNING organization_id AS id", ("Default Org",)))


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

    if is_dev_account(auth_user):
        existing_dev_user = fetch_one(
            "SELECT * FROM users WHERE LOWER(email) = ? LIMIT 1",
            (auth_user["email"].strip().lower(),),
        )
        if existing_dev_user:
            app_logger.info(
                "Using existing dev platform user across role tabs.",
                user_email=auth_user["email"].strip().lower(),
                role=normalized_role,
                stored_role=existing_dev_user.get("role"),
            )
            return dict(existing_dev_user)

    org_id = _default_org_id()
    user_id = execute(
        """
        INSERT INTO users (name, email, role, team, organization_id, is_active)
        VALUES (?, ?, ?, ?, ?, TRUE)
        RETURNING user_id AS id
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
    """Passwordless login that authenticates local users and keeps dev logins role-agnostic."""
    ident = (identifier or "").strip()
    if not ident:
        return False, "Enter your email or username to sign in.", None

    user = find_user_by_email(ident, role=expected_role)
    if user is None:
        user = find_user_by_username(ident, role=expected_role)

    if user is not None:
        return True, None, user

    # Developer accounts should stay lean: if one role exists, allow sign-in from either tab.
    fallback_user = find_user_by_email(ident, role=None)
    if fallback_user is None:
        fallback_user = find_user_by_username(ident, role=None)
    if fallback_user and is_dev_account(fallback_user):
        fallback_with_selected_role = dict(fallback_user)
        fallback_with_selected_role["role"] = expected_role
        return True, None, fallback_with_selected_role

    return False, f"No active {expected_role.title()} account exists yet. Create one first.", None


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
        "User sign-in succeeded.", auth_method=auth_method, role=normalized_role
    )
    st.session_state["page"] = None
    st.session_state["auth_error"] = None
    st.session_state["auth_info"] = None
    st.session_state["pending_google"] = None
    st.session_state["page"] = None
    st.session_state["active_module_id"] = None
    st.session_state["active_assignment_id"] = None
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
        RETURNING user_id AS id
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
    current_user = st.session_state.get("current_user") or {}
    if not is_dev_account(current_user):
        st.warning("You do not have access to this section.")
        return

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
            try:
                result = fetch_one("SELECT version() AS version")
                st.session_state["quick_connect_result"] = ("success", result["version"] if result else "Unknown")
            except Exception as exc:
                st.session_state["quick_connect_result"] = ("error", f"{type(exc).__name__}: {exc}")

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
    _log_render_debug("Render login view.", page="login")

    current_user = st.session_state.get("current_user") or {}
    can_view_db_tools = is_dev_account(current_user)

    if st.session_state.get("bootstrap_error"):
        st.error(
            "Database bootstrap failed, so sign-in and app pages are unavailable right now. "
            "A developer account can open Database tools and run the database test for details."
        )
        st.caption(f"Bootstrap error: {st.session_state['bootstrap_error']}")

    if st.session_state.get("bootstrap_error") and can_view_db_tools:
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
                app_logger.info("User submitted login form.", role="learner")
                ok, message, user = validate_dev_login(identifier, expected_role="learner")
                if ok and user:
                    _sign_in_user(user, "dev_quick")
                    return
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                return

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
                app_logger.info("User submitted login form.", role="admin")
                ok, message, user = validate_dev_login(identifier, expected_role="admin")
                if ok and user:
                    _sign_in_user(user, "dev_quick")
                    return
                st.session_state["auth_error"] = message
                st.session_state["pending_google"] = None
                return

        _render_google_button("admin")
        st.button(
            "Create account",
            key="create_link_admin",
            use_container_width=True,
            on_click=_set_ui_event,
            args=("create_link_admin",),
        )

    if can_view_db_tools:
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
            return
        st.session_state["auth_error"] = message
        return

    if ui_event == "pending_google_back":
        st.session_state["pending_google"] = None
        st.session_state["auth_error"] = None
        st.logout()

    if ui_event == "create_link_learner":
        st.session_state["auth_view"] = "create_account"
        st.session_state["selected_role"] = "learner"
        return

    if ui_event == "create_link_admin":
        st.session_state["auth_view"] = "create_account"
        st.session_state["selected_role"] = "admin"
        return



def render_create_account_view() -> None:
    _log_render_debug("Render create-account view.", page="create_account")
    render_page_header("Create your account", "You can register both Learner and Admin accounts using the same email address.")
    role = render_horizontal_button_group(
        "Select role",
        ["learner", "admin"],
        "selected_role",
        format_func=lambda r: r.title(),
    )
    with st.form("create_account_form", clear_on_submit=False):
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
                return
            st.error(message)

    if st.button("Back to sign in", use_container_width=True):
        st.session_state["auth_view"] = "login"
        st.session_state["auth_error"] = None
        st.session_state["pending_google"] = None
        return


def _avatar_initials(full_name: str) -> str:
    tokens = [token for token in (full_name or "").strip().split() if token]
    if not tokens:
        return "U"
    if len(tokens) == 1:
        return tokens[0][0].upper()
    return f"{tokens[0][0]}{tokens[-1][0]}".upper()


def _is_valid_email(email: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (email or "").strip()))


def load_current_user_profile() -> dict[str, Any] | None:
    current_user = st.session_state.get("current_user") or {}
    user_id = current_user.get("user_id")
    if not user_id:
        return None
    row = fetch_one(
        """
        SELECT user_id, id, name, email, username, role, password_hash, auth_provider, organization_id
        FROM users
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,),
    )
    return dict(row) if row else None


def _refresh_current_user_session(profile_row: dict[str, Any]) -> None:
    current = dict(st.session_state.get("current_user") or {})
    current.update(
        {
            "id": profile_row.get("id", current.get("id")),
            "user_id": profile_row.get("user_id", current.get("user_id")),
            "full_name": profile_row.get("name", current.get("full_name")),
            "name": profile_row.get("name", current.get("name")),
            "email": profile_row.get("email", current.get("email")),
            "role": _normalize_role(profile_row.get("role", current.get("role"))),
            "organization_id": profile_row.get("organization_id", current.get("organization_id")),
        }
    )
    st.session_state["current_user"] = current


def save_user_profile_updates(
    *,
    full_name: str,
    email: str,
    username: str,
    current_password: str,
    new_password: str,
    confirm_new_password: str,
) -> tuple[bool, str]:
    profile = load_current_user_profile()
    if not profile:
        return False, "Could not load your account details. Please sign in again."

    full_name = (full_name or "").strip()
    email = (email or "").strip().lower()
    username_clean = (username or "").strip() or None
    current_password = (current_password or "").strip()
    new_password = (new_password or "").strip()
    confirm_new_password = (confirm_new_password or "").strip()

    if not full_name:
        return False, "Full name is required."
    if not email:
        return False, "Email is required."
    if not _is_valid_email(email):
        return False, "Please enter a valid email address."

    existing_email = fetch_one(
        """
        SELECT user_id
        FROM users
        WHERE LOWER(email) = ? AND role = ? AND user_id <> ?
        LIMIT 1
        """,
        (email, profile["role"], profile["user_id"]),
    )
    if existing_email:
        return False, "That email is already linked to another account for this role."

    if username_clean:
        existing_username = fetch_one(
            "SELECT user_id FROM users WHERE LOWER(username) = ? AND user_id <> ? LIMIT 1",
            (username_clean.lower(), profile["user_id"]),
        )
        if existing_username:
            return False, "That username is already in use."

    updates: dict[str, Any] = {"name": full_name, "email": email, "username": username_clean}

    wants_password_change = any([current_password, new_password, confirm_new_password])
    if wants_password_change:
        if profile.get("auth_provider") != "local_password":
            return False, "Password updates are unavailable for Google-authenticated accounts."
        if not current_password:
            return False, "Current password is required to set a new password."
        if hash_password(current_password) != (profile.get("password_hash") or ""):
            return False, "Current password is incorrect."
        if not new_password or not confirm_new_password:
            return False, "Enter and confirm your new password."
        if len(new_password) < 8:
            return False, "New password must be at least 8 characters."
        if new_password != confirm_new_password:
            return False, "New password and confirmation must match."
        updates["password_hash"] = hash_password(new_password)

    execute_update("users", updates, "user_id = ?", (profile["user_id"],))

    refreshed = load_current_user_profile()
    if refreshed:
        _refresh_current_user_session(refreshed)
    return True, "Profile updated successfully."


def render_sidebar_profile_section(user: dict[str, Any]) -> None:
    display_name = user.get("full_name") or "User"
    role = str(user.get("role", "learner")).title()
    email = (user.get("email") or "").strip()
    safe_display_name = escape(display_name)
    safe_role = escape(role)
    safe_email = escape(email)

    with st.container():
        st.markdown(
            f"""
            <section class="sidebar-profile sidebar-profile-compact" data-testid="sidebar-profile-compact">
                <div class="sidebar-profile-name">{safe_display_name}</div>
                <div class="sidebar-profile-label">{safe_role}</div>
                {"<div class='sidebar-profile-email'>" + safe_email + "</div>" if safe_email else ""}
            </section>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Settings", key="sidebar_profile_settings_btn", type="secondary", use_container_width=True):
            _navigate_to_account_page("settings")
        if st.button("Logout", key="sidebar_profile_logout_btn", type="secondary", use_container_width=True):
            logout_user()
            st.rerun()


def _initialize_profile_form(profile: dict[str, Any]) -> None:
    profile_user_id = profile.get("user_id")
    if st.session_state.get("profile_form_initialized_for") == profile_user_id:
        return
    st.session_state["profile_full_name"] = profile.get("name", "")
    st.session_state["profile_email"] = profile.get("email", "")
    st.session_state["profile_username"] = profile.get("username") or ""
    st.session_state["profile_current_password"] = ""
    st.session_state["profile_new_password"] = ""
    st.session_state["profile_confirm_password"] = ""
    st.session_state["profile_form_initialized_for"] = profile_user_id


def render_profile_page() -> None:
    profile = load_current_user_profile()
    back_col, _ = st.columns([1.2, 5], vertical_alignment="center")
    with back_col:
        if st.button("← Back to Dashboard", key="profile_back_btn", use_container_width=True):
            _navigate_back_to_main_app()
    st.caption("Dashboard / Profile")
    render_page_header("Profile", "Update your account information and security settings.")
    if not profile:
        st.error("Could not load profile details.")
        return

    _initialize_profile_form(profile)
    feedback = st.session_state.get("profile_feedback")
    if feedback:
        status, message = feedback
        if status == "success":
            st.success(message)
        else:
            st.error(message)

    _, content, _ = st.columns([1, 2.2, 1])
    with content:
        st.markdown("#### Account Information")
        with st.form("profile_account_form", clear_on_submit=False):
            st.text_input("Full name *", key="profile_full_name")
            st.text_input("Email *", key="profile_email")
            st.text_input("Username", key="profile_username")
            st.text_input("Role", value=str(profile.get("role", "")).title(), disabled=True)

            st.markdown("#### Security")
            if profile.get("auth_provider") == "local_password":
                st.text_input("Current password", type="password", key="profile_current_password")
                st.text_input("New password", type="password", key="profile_new_password")
                st.text_input("Confirm new password", type="password", key="profile_confirm_password")
            else:
                st.info("Password is managed by Google for this account.")

            save_col, reset_col = st.columns(2)
            with save_col:
                save_clicked = st.form_submit_button("Save changes", use_container_width=True, type="primary")
            with reset_col:
                reset_clicked = st.form_submit_button("Reset", use_container_width=True)

            if save_clicked:
                ok, message = save_user_profile_updates(
                    full_name=st.session_state.get("profile_full_name", ""),
                    email=st.session_state.get("profile_email", ""),
                    username=st.session_state.get("profile_username", ""),
                    current_password=st.session_state.get("profile_current_password", ""),
                    new_password=st.session_state.get("profile_new_password", ""),
                    confirm_new_password=st.session_state.get("profile_confirm_password", ""),
                )
                st.session_state["profile_feedback"] = ("success" if ok else "error", message)
                if ok:
                    refreshed = load_current_user_profile()
                    if refreshed:
                        _initialize_profile_form(refreshed)
                return

            if reset_clicked:
                st.session_state["profile_form_initialized_for"] = None
                st.session_state["profile_feedback"] = None
                return


def render_settings_page() -> None:
    back_col, _ = st.columns([1.2, 5], vertical_alignment="center")
    with back_col:
        if st.button("← Back to Dashboard", key="settings_back_btn", use_container_width=True):
            _navigate_back_to_main_app()
    st.caption("Dashboard / Settings")
    render_page_header("Settings", "Personalization and account preferences.")
    _, content, _ = st.columns([1, 2.2, 1])
    with content:
        with st.container(border=True):
            st.markdown("#### Account Preferences")
            st.caption("Workspace defaults and account-level preferences will appear here.")
        with st.container(border=True):
            st.markdown("#### Display")
            st.caption("Theme, density, and dashboard layout preferences are coming soon.")
        with st.container(border=True):
            st.markdown("#### Notifications")
            st.caption("Email and in-app notification preferences are coming soon.")


def render_main_app() -> None:
    user = st.session_state["current_user"]
    is_dev_user = is_dev_account(user)
    with st.sidebar:
        render_sidebar_profile_section(user)
    requested_page = st.session_state.get("page")
    nav_page = _sync_current_page_with_query(user["role"])
    assignment_from_url = _read_assignment_id_from_query_params()
    if assignment_from_url is not None:
        st.session_state["active_assignment_id"] = assignment_from_url
    if requested_page == "Profile" or nav_page == "profile":
        st.session_state["page"] = "Profile"
        st.session_state["current_page"] = "profile"
        _set_nav("profile")
        with page_container("narrow"):
            render_profile_page()
        return
    if requested_page == "Settings" or nav_page == "settings":
        st.session_state["page"] = "Settings"
        st.session_state["current_page"] = "settings"
        _set_nav("settings")
        with page_container("narrow"):
            render_settings_page()
        return

    if user["role"] == "admin":
        if nav_page == "admin-assignment-review":
            with page_container("medium"):
                render_admin_assignment_review(user, st.session_state.get("active_assignment_id"))
            return
        operations_pages = [
            "📊 Dashboard",
            "🧩 Module Builder",
            "📚 Manage Modules",
            "📁 Assignment Management",
            "✅ Submission Grading",
            "📈 Progress Tracking",
            "👥 Learner Management",
        ]
        if is_dev_user:
            operations_pages.extend(["🛠️ Database Tables", "📝 Debug Logs"])
        qa_pages = ["🧪 QA Test Center"] if is_dev_user else []
        base_page_lookup = {re.sub(r"^[^\w]+", "", p).strip(): p for p in (operations_pages + qa_pages)}
        all_pages = operations_pages + qa_pages

        nav_requested_page = NAV_TO_ADMIN_PAGE.get(nav_page)
        if nav_requested_page in DEV_ONLY_ADMIN_PAGES and not is_dev_user:
            nav_requested_page = "Dashboard"
            _set_nav(ADMIN_PAGE_TO_NAV["Dashboard"])
        if nav_requested_page in base_page_lookup:
            st.session_state["admin_page"] = base_page_lookup[nav_requested_page]

        if requested_page in operations_pages or requested_page in base_page_lookup:
            st.session_state["admin_nav_group"] = "Operations"
            st.session_state["admin_page"] = base_page_lookup.get(requested_page, requested_page)
        elif requested_page in qa_pages:
            st.session_state["admin_nav_group"] = "Quality Assurance"
            st.session_state["admin_page"] = requested_page

        active_admin_page = st.session_state.get("admin_page", "📊 Dashboard")
        show_workspace_toggle = active_admin_page in {"📝 Debug Logs", "🧪 QA Test Center"}
        if show_workspace_toggle:
            render_horizontal_button_group(
                "Admin Workspace",
                ["Operations", "Quality Assurance"],
                "admin_nav_group",
            )
        else:
            st.session_state["admin_nav_group"] = "Operations"

        current_group = st.session_state.get("admin_nav_group", "Operations")
        visible_pages = operations_pages if current_group == "Operations" else qa_pages
        if not visible_pages:
            st.session_state["admin_nav_group"] = "Operations"
            visible_pages = operations_pages
        if st.session_state.get("admin_page") not in visible_pages:
            st.session_state["admin_page"] = visible_pages[0]
        st.session_state["page"] = None
        st.sidebar.markdown(
            """
            <div class="sidebar-header">
                <div class="sidebar-eyebrow">Admin workspace</div>
                <div class="sidebar-title">Navigation</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        current_slug = _get_current_main_nav_slug("admin")
        current_page_name = NAV_TO_ADMIN_PAGE.get(current_slug, "Dashboard")
        current_display_page = base_page_lookup.get(current_page_name, "📊 Dashboard")
        st.session_state["admin_page"] = current_display_page
        for index, option in enumerate(visible_pages):
            option_name = re.sub(r"^[^\w]+", "", option).strip()
            option_slug = ADMIN_PAGE_TO_NAV.get(option_name, "dashboard")
            button_type = "primary" if option_slug == current_slug else "secondary"
            if st.sidebar.button(
                option,
                key=f"admin_sidebar_nav_btn_{index}",
                type=button_type,
                use_container_width=True,
            ):
                if option_slug != current_slug:
                    st.session_state["admin_page"] = option
                    st.session_state["current_page"] = _build_main_page_key("admin", option_slug)
                    st.session_state["page"] = None
                    _set_nav(option_slug)
                    st.rerun()
        normalized_page = NAV_TO_ADMIN_PAGE.get(current_slug, "Dashboard")
        if normalized_page in DEV_ONLY_ADMIN_PAGES and not is_dev_user:
            st.warning("You do not have access to this section.")
            normalized_page = "Dashboard"
            current_slug = ADMIN_PAGE_TO_NAV["Dashboard"]
            st.session_state["current_page"] = _build_main_page_key("admin", current_slug)
            _set_nav(current_slug)
        if st.session_state.get("nav") != current_slug:
            _set_nav(current_slug)
        st.session_state["current_page"] = _build_main_page_key("admin", current_slug)
        _log_render_debug("Render admin page.", page=normalized_page)
        admin_container_variant = {
            "Dashboard": "wide",
            "Assignment Management": "wide",
            "Submission Grading": "medium",
            "Progress Tracking": "wide",
            "Learner Management": "wide",
            "Module Builder": "medium",
            "Manage Modules": "wide",
            "Database Tables": "wide",
            "Debug Logs": "wide",
            "QA Test Center": "wide",
        }.get(normalized_page, "wide")
        with page_container(admin_container_variant):
            if normalized_page == "Dashboard":
                render_admin_dashboard(user)
            elif normalized_page == "Assignment Management":
                render_assignment_management(user)
            elif normalized_page == "Submission Grading":
                render_grading_center(user)
            elif normalized_page == "Progress Tracking":
                render_progress_tracking(user)
            elif normalized_page == "Learner Management":
                render_learner_management(user)
            elif normalized_page == "Module Builder":
                render_module_builder(user)
            elif normalized_page == "Manage Modules":
                render_manage_modules(user)
            elif normalized_page == "Database Tables":
                render_database_tables_view()
            elif normalized_page == "Debug Logs":
                render_admin_log_viewer()
            elif normalized_page == "QA Test Center":
                render_admin_quality_hub(user)
    else:
        pages = ["home", "assigned_modules", "module_workspace", "results"]
        if is_dev_user:
            pages.extend(["database_tables", "debug_logs"])
        if st.session_state.get("learner_page") not in pages:
            st.session_state["learner_page"] = "home"

        nav_requested_page = NAV_TO_LEARNER_PAGE.get(nav_page)
        if nav_requested_page in LEARNER_NAV_CONFIG and st.session_state.get("learner_page") != nav_requested_page:
            st.session_state["learner_page"] = nav_requested_page

        legacy_requested_page = {
            "🏠 Home": "home",
            "📦 Assigned Modules": "assigned_modules",
            "🧪 Module Workspace": "module_workspace",
            "📝 Results": "results",
            "📉 My Progress": "results",
            "📊 Progress & Results": "results",
            "Home": "home",
            "Assigned Modules": "assigned_modules",
            "Module Workspace": "module_workspace",
            "Results": "results",
            "My Progress": "results",
            "Progress & Results": "results",
        }.get(str(requested_page))
        if legacy_requested_page and st.session_state.get("learner_page") != legacy_requested_page:
            st.session_state["learner_page"] = legacy_requested_page
        st.session_state["page"] = None
        st.sidebar.markdown(
            """
            <div class="sidebar-header">
                <div class="sidebar-eyebrow">Learner workspace</div>
                <div class="sidebar-title">Navigation</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        current_slug = _get_current_main_nav_slug("learner")
        current_page = NAV_TO_LEARNER_PAGE.get(current_slug, "home")
        st.session_state["learner_page"] = current_page
        for index, option in enumerate(pages):
            option_slug = LEARNER_PAGE_TO_NAV.get(option, "home")
            button_type = "primary" if option_slug == current_slug else "secondary"
            if st.sidebar.button(
                LEARNER_NAV_CONFIG.get(option, str(option)),
                key=f"learner_sidebar_nav_btn_{index}",
                type=button_type,
                use_container_width=True,
            ):
                if option_slug != current_slug:
                    st.session_state["learner_page"] = option
                    st.session_state["current_page"] = _build_main_page_key("learner", option_slug)
                    st.session_state["page"] = None
                    _set_nav(option_slug)
                    st.rerun()
        if st.session_state.get("nav") != current_slug:
            _set_nav(current_slug)
        st.session_state["current_page"] = _build_main_page_key("learner", current_slug)
        render_branch = current_page
        if DEBUG:
            app_logger.debug("learner_page key = {}", current_page)
            app_logger.debug("render branch = {}", render_branch)
        _log_render_debug("Render learner page.", page=current_page)
        learner_container_variant = {
            "home": "medium",
            "assigned_modules": "medium",
            "module_workspace": "medium",
            "results": "medium",
            "database_tables": "wide",
            "debug_logs": "wide",
        }.get(current_page, "medium")
        with page_container(learner_container_variant):
            if current_page == "home":
                render_learner_home(user)
            elif current_page == "assigned_modules":
                render_module_library(user)
            elif current_page == "module_workspace":
                render_scenario_page(user)
            elif current_page == "results":
                render_progress_results_page(user)
            elif current_page == "database_tables":
                render_database_tables_view()
            elif current_page == "debug_logs":
                render_admin_log_viewer()
            else:
                st.warning(f"Debug warning: unknown learner_page `{current_page}`; defaulting to assigned_modules.")
                render_branch = "assigned_modules_fallback"
                render_module_library(user)
        if st.session_state.get("learner_page") == "module_workspace" and render_branch != "module_workspace":
            st.warning("Debug warning: learner_page is `module_workspace`, but Module Workspace branch was not rendered.")


def main() -> None:
    init_state()
    _increment_rerun_cycle()
    assignment_from_url = _read_assignment_id_from_query_params()
    if assignment_from_url is not None:
        st.session_state["active_assignment_id"] = assignment_from_url
    active_assignment_id = st.session_state.get("active_assignment_id")
    if active_assignment_id is not None and str(st.query_params.get("assignment_id") or "") != str(active_assignment_id):
        st.query_params["assignment_id"] = str(active_assignment_id)

    st.session_state.setdefault("session_id", st.session_state.get("session_id") or f"sess_{hashlib.md5(str(id(st.session_state)).encode()).hexdigest()[:12]}")
    _log_render_debug("App rerun started.", session_id=st.session_state.get("session_id"))
    initialize_once()
    inject_scroll_to_top()

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
