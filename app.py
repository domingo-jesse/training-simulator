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

# Sample .streamlit/secrets.toml (Google OIDC)
# -------------------------------------------------------------
# [auth]
# redirect_uri = "http://localhost:8501/oauth2callback"
# cookie_secret = "replace-with-a-long-random-secret"
# allowed_domains = ["gmail.com", "mycompany.com"]
#
# [auth.google]
# client_id = "YOUR_GOOGLE_OAUTH_CLIENT_ID"
# client_secret = "YOUR_GOOGLE_OAUTH_CLIENT_SECRET"
# server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
# -------------------------------------------------------------
# Redirect URI notes:
# 1) The Google Cloud OAuth client Authorized redirect URIs must EXACTLY match Streamlit.
# 2) Local example:    http://localhost:8501/oauth2callback
# 3) Deployed example: https://your-app.streamlit.app/oauth2callback


def _safe_user_value(key: str, default: str = "") -> str:
    """Return a user field from st.user safely, without crashing if missing."""
    user_obj = getattr(st, "user", None)
    if user_obj is None:
        return default

    try:
        value = user_obj.get(key)
    except Exception:
        value = getattr(user_obj, key, None)

    if value is None:
        return default

    return str(value)


def get_allowed_domains() -> list[str]:
    """Read allowed domains from Streamlit secrets; empty list means allow all domains."""
    default_domains: list[str] = ["gmail.com"]

    try:
        auth_cfg = st.secrets.get("auth", {})
        configured = auth_cfg.get("allowed_domains", default_domains)
    except Exception:
        configured = default_domains

    if isinstance(configured, str):
        domains = [configured]
    elif isinstance(configured, list):
        domains = [str(item) for item in configured]
    else:
        domains = default_domains

    return [domain.strip().lower() for domain in domains if domain and domain.strip()]


def check_allowed_domain(email: str | None, allowed_domains: list[str]) -> bool:
    """Return True if email is allowed or no restrictions are configured."""
    if not allowed_domains:
        return True

    if not email or "@" not in email:
        return False

    domain = email.rsplit("@", 1)[1].strip().lower()
    return domain in allowed_domains


def oauth_preflight_checks() -> tuple[list[str], list[str]]:
    """Validate OIDC config and return (errors, warnings) for operator troubleshooting."""
    errors: list[str] = []
    warnings: list[str] = []

    try:
        auth_cfg = st.secrets.get("auth", {})
        google_cfg = auth_cfg.get("google", {})
    except Exception:
        return (
            ["Unable to read `st.secrets`. Confirm `.streamlit/secrets.toml` exists and is valid TOML."],
            [],
        )

    redirect_uri = str(auth_cfg.get("redirect_uri", "")).strip()
    cookie_secret = str(auth_cfg.get("cookie_secret", "")).strip()
    client_id = str(google_cfg.get("client_id", "")).strip()
    client_secret = str(google_cfg.get("client_secret", "")).strip()
    metadata_url = str(google_cfg.get("server_metadata_url", "")).strip()

    if not redirect_uri:
        errors.append("Missing `auth.redirect_uri`.")
    elif not redirect_uri.endswith("/oauth2callback"):
        warnings.append(
            "`auth.redirect_uri` should usually end with `/oauth2callback` for Streamlit OIDC."
        )

    if not cookie_secret:
        errors.append("Missing `auth.cookie_secret`.")
    elif len(cookie_secret) < 24:
        warnings.append("`auth.cookie_secret` is short. Use a long random value (at least 24+ chars).")

    if not client_id:
        errors.append("Missing `auth.google.client_id`.")
    elif ".apps.googleusercontent.com" not in client_id:
        warnings.append("`auth.google.client_id` does not look like a Google OAuth client ID.")

    if not client_secret:
        errors.append("Missing `auth.google.client_secret`.")

    if not metadata_url:
        errors.append("Missing `auth.google.server_metadata_url`.")
    elif "accounts.google.com" not in metadata_url:
        warnings.append("`auth.google.server_metadata_url` is non-standard for Google.")

    return errors, warnings


def render_auth_troubleshooting() -> None:
    """Render a compact troubleshooting panel for common Google OAuth issues."""
    errors, warnings = oauth_preflight_checks()

    with st.expander("OAuth setup & troubleshooting", expanded=False):
        st.markdown("#### Quick setup checklist")
        st.markdown(
            """
1. In Google Cloud Console, create an OAuth Client ID (Web application).
2. Add this exact redirect URI in Google Cloud:
   - `http://localhost:8501/oauth2callback` (local), or
   - `https://<your-app>.streamlit.app/oauth2callback` (deployed)
3. Copy `client_id` and `client_secret` into `.streamlit/secrets.toml`.
4. Keep `server_metadata_url` set to:
   `https://accounts.google.com/.well-known/openid-configuration`
5. Restart Streamlit after editing secrets.
            """
        )

        if errors:
            st.error("Configuration errors found:")
            for item in errors:
                st.write(f"- {item}")
        else:
            st.success("No blocking configuration errors detected.")

        if warnings:
            st.warning("Potential issues:")
            for item in warnings:
                st.write(f"- {item}")

        st.markdown("#### Common auth errors")
        st.markdown(
            """
- **`redirect_uri_mismatch`**: the URI in Google Cloud does not exactly match `auth.redirect_uri`.
- **`invalid_client`**: wrong client ID/secret or wrong OAuth app.
- **Blank/looping login**: stale cookies; sign out and clear browser cookies for the app domain.
- **Access denied after login**: your email domain is blocked by `auth.allowed_domains`.
            """
        )


def render_login() -> None:
    """Render unauthenticated landing/login UI."""
    _, center, _ = st.columns([1.2, 1.8, 1.2])

    with center:
        st.markdown('<div class="login-shell">', unsafe_allow_html=True)
        st.title("🛠️ Troubleshooting Trainer")
        st.caption("Practice diagnosis, communication, and incident response with AI-powered simulations.")
        st.write("")

        if st.button("Continue with Google", use_container_width=True, type="primary"):
            missing_fields: list[str] = []
            try:
                google_cfg = st.secrets.get("auth", {}).get("google", {})
                client_id = str(google_cfg.get("client_id", "")).strip()
                client_secret = str(google_cfg.get("client_secret", "")).strip()
                if not client_id:
                    missing_fields.append("auth.google.client_id")
                if not client_secret:
                    missing_fields.append("auth.google.client_secret")
            except Exception:
                missing_fields = ["auth.google.client_id", "auth.google.client_secret"]

            if missing_fields:
                st.error(
                    "Google OAuth is not configured. Add values to `.streamlit/secrets.toml` for: "
                    + ", ".join(missing_fields)
                )
            else:
                try:
                    st.login("google")
                except Exception:
                    st.error(
                        "Google login could not be started. Check your Streamlit OIDC secrets and redirect URI configuration."
                    )

        st.caption("Need help? Ask your admin to confirm Google OAuth redirect URI and client credentials.")
        render_auth_troubleshooting()
        st.markdown("</div>", unsafe_allow_html=True)


def render_authenticated_app() -> None:
    """Render post-login state and route users into the training app."""
    name = _safe_user_value("name", "User")
    email = _safe_user_value("email", "")
    photo = _safe_user_value("picture", "")

    allowed_domains = get_allowed_domains()
    if not check_allowed_domain(email, allowed_domains):
        st.error("Access denied: your email domain is not authorized for this workspace.")
        st.caption(f"Allowed domains: {', '.join(allowed_domains) if allowed_domains else 'All domains'}")
        try:
            st.logout()
        except Exception:
            st.info("Please sign out and contact your administrator.")
        st.stop()

    users = fetch_all("SELECT * FROM users WHERE is_active = 1 ORDER BY role, name")
    user_by_email = {u["email"].lower(): u for u in users if u["email"]}
    workspace_user = user_by_email.get(email.lower()) if email else None

    with st.sidebar:
        st.markdown("### Account")
        if photo:
            st.image(photo, width=72)
        st.write(name)
        st.caption(email or "Email unavailable from identity provider")

        if st.button("Sign out", use_container_width=True):
            try:
                st.logout()
            except Exception:
                st.warning("Sign out encountered an issue. Refresh and try again.")
            st.stop()

    if not workspace_user:
        st.title("Signed in successfully")
        st.warning("Your Google account is authenticated, but no active workspace profile is mapped to this email.")
        st.write("Please contact an administrator to grant access.")
        return

    st.title("🛠️ Troubleshooting Trainer")
    st.caption("AI-powered simulation practice for issue investigation, diagnosis, and communication.")

    if workspace_user["role"] == "admin":
        pages = ["Dashboard", "Learner Management", "Assignment Management", "Progress Tracking", "Module Builder"]
        page = st.sidebar.radio("Navigate", pages, key="admin_nav_radio")

        if page == "Dashboard":
            render_admin_dashboard(workspace_user)
        elif page == "Learner Management":
            render_learner_management(workspace_user)
        elif page == "Assignment Management":
            render_assignment_management(workspace_user)
        elif page == "Progress Tracking":
            render_progress_tracking(workspace_user)
        else:
            render_module_builder(workspace_user)
        return

    pages = ["Learner Home", "Module Library", "Scenario", "Results", "My Progress"]
    default_page = st.session_state.get("page", "Learner Home")
    default_index = pages.index(default_page) if default_page in pages else 0
    page = st.sidebar.radio("Navigate", pages, index=default_index, key="learner_nav_radio")
    st.session_state.page = page

    if page == "Learner Home":
        render_learner_home(workspace_user)
    elif page == "Module Library":
        render_module_library(workspace_user)
    elif page == "Scenario":
        render_scenario_page(workspace_user)
    elif page == "Results":
        render_results_page(workspace_user)
    else:
        render_progress_page(workspace_user)


def main() -> None:
    init_db()
    seed_all()
    inject_styles()

    user_obj = getattr(st, "user", None)
    is_logged_in = bool(getattr(user_obj, "is_logged_in", False)) if user_obj else False

    if not is_logged_in:
        render_login()
        return

    render_authenticated_app()


if __name__ == "__main__":
    main()
