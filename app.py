from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

import streamlit as st


st.set_page_config(
    page_title="Training Simulator",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)


@dataclass(frozen=True)
class DashboardCard:
    title: str
    description: str


@dataclass(frozen=True)
class SetupCheck:
    label: str
    passed: bool
    detail: str


DASHBOARD_CARDS: tuple[DashboardCard, ...] = (
    DashboardCard(
        title="Start Simulation",
        description="Spin up a fresh scenario with role goals, challenge level, and AI facilitator guidance.",
    ),
    DashboardCard(
        title="Review Performance",
        description="Inspect score breakdowns, transcript highlights, and targeted coaching recommendations.",
    ),
    DashboardCard(
        title="Admin / Settings",
        description="Configure workspace defaults, evaluation rubrics, and team-level access controls.",
    ),
)


def inject_ui_css() -> None:
    """Apply refreshed UI styling for auth and dashboard views."""
    st.markdown(
        """
        <style>
            .main > div {
                padding-top: 0.8rem;
            }

            .stApp {
                background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 45%, #f8fafc 100%);
            }

            .app-shell {
                max-width: 1150px;
                margin: 0 auto;
            }

            .app-header {
                background: rgba(255, 255, 255, 0.92);
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 16px;
                padding: 0.9rem 1rem;
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
                margin-bottom: 1rem;
            }

            .app-title {
                font-size: 1.55rem;
                font-weight: 800;
                letter-spacing: -0.01em;
                margin: 0;
                color: #0f172a;
            }

            .status-pill {
                display: inline-block;
                margin-top: 0.35rem;
                padding: 0.18rem 0.56rem;
                border-radius: 999px;
                font-size: 0.78rem;
                font-weight: 600;
                color: #1e3a8a;
                background: #dbeafe;
                border: 1px solid #bfdbfe;
            }

            .profile-shell {
                background: #ffffff;
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 14px;
                padding: 0.7rem 0.9rem;
            }

            .hero-card,
            .dashboard-card,
            .login-card {
                background: #ffffff;
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 16px;
                box-shadow: 0 6px 28px rgba(15, 23, 42, 0.07);
            }

            .hero-card {
                padding: 1.25rem;
                margin: 0.35rem 0 1rem 0;
                background: linear-gradient(125deg, #ffffff 0%, #f8faff 100%);
            }

            .dashboard-card {
                padding: 1rem 1.05rem;
                min-height: 180px;
            }

            .login-card {
                padding: 2.15rem 1.65rem;
                margin-top: 7vh;
            }

            .card-title {
                margin: 0 0 0.35rem 0;
                font-size: 1.08rem;
                font-weight: 680;
            }

            .muted {
                color: #475569;
                font-size: 0.94rem;
                line-height: 1.5;
            }

            .tiny-note {
                margin-top: 0.45rem;
                color: #64748b;
                font-size: 0.8rem;
            }

            .avatar-fallback {
                width: 46px;
                height: 46px;
                border-radius: 999px;
                background: #e2e8f0;
                color: #0f172a;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                font-size: 1rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_user_field(field: str, default: str | None = None) -> str | None:
    """Safely read fields from st.user without hard failures."""
    user = getattr(st, "user", None)
    if not user:
        return default

    try:
        value = user.get(field)
    except Exception:
        value = getattr(user, field, None)

    if value in (None, ""):
        return default

    return str(value)


def _secret_get(path: tuple[str, ...], default: str | list[str] | None = None):
    current = st.secrets
    try:
        for key in path:
            current = current[key]
    except Exception:
        return default
    return current


def oauth_config() -> dict[str, str | list[str] | None]:
    """Read OAuth configuration from Streamlit secrets with safe defaults."""
    return {
        "redirect_uri": _secret_get(("auth", "redirect_uri"), ""),
        "cookie_secret": _secret_get(("auth", "cookie_secret"), ""),
        "allowed_domains": _secret_get(("auth", "allowed_domains"), []),
        "client_id": _secret_get(("auth", "google", "client_id"), ""),
        "client_secret": _secret_get(("auth", "google", "client_secret"), ""),
        "server_metadata_url": _secret_get(("auth", "google", "server_metadata_url"), ""),
    }


def oauth_setup_checks() -> tuple[list[SetupCheck], list[str]]:
    """Return setup checks and user-facing hints for Google OAuth."""
    cfg = oauth_config()
    checks: list[SetupCheck] = []
    hints: list[str] = []

    redirect_uri = str(cfg["redirect_uri"] or "").strip()
    cookie_secret = str(cfg["cookie_secret"] or "").strip()
    client_id = str(cfg["client_id"] or "").strip()
    client_secret = str(cfg["client_secret"] or "").strip()
    metadata_url = str(cfg["server_metadata_url"] or "").strip()

    is_valid_redirect = False
    if redirect_uri:
        parsed = urlparse(redirect_uri)
        is_valid_redirect = bool(parsed.scheme in {"http", "https"} and parsed.netloc and parsed.path.endswith("/oauth2callback"))

    checks.append(
        SetupCheck(
            label="auth.redirect_uri",
            passed=is_valid_redirect,
            detail=redirect_uri or "Missing. Add your app callback URI.",
        )
    )
    checks.append(
        SetupCheck(
            label="auth.cookie_secret",
            passed=len(cookie_secret) >= 24 and "replace" not in cookie_secret.lower(),
            detail=f"Length: {len(cookie_secret)}",
        )
    )
    checks.append(
        SetupCheck(
            label="auth.google.client_id",
            passed=bool(client_id and ".apps.googleusercontent.com" in client_id and "replace" not in client_id.lower()),
            detail=client_id or "Missing",
        )
    )
    checks.append(
        SetupCheck(
            label="auth.google.client_secret",
            passed=bool(client_secret and "replace" not in client_secret.lower()),
            detail="Configured" if client_secret else "Missing",
        )
    )
    checks.append(
        SetupCheck(
            label="auth.google.server_metadata_url",
            passed=metadata_url.startswith("https://"),
            detail=metadata_url or "Missing",
        )
    )

    if redirect_uri and not redirect_uri.endswith("/oauth2callback"):
        hints.append("Redirect URI should end with `/oauth2callback`.")
    if redirect_uri.startswith("http://") and "localhost" not in redirect_uri:
        hints.append("Use HTTPS for non-localhost redirect URIs.")
    if not cfg["allowed_domains"]:
        hints.append("`auth.allowed_domains` is empty, so domain allow-listing is not enforced.")

    return checks, hints


def oauth_ready() -> bool:
    checks, _ = oauth_setup_checks()
    return all(check.passed for check in checks)


def set_demo_login() -> None:
    """Store a local demo user profile in session state."""
    st.session_state["demo_auth"] = {
        "name": "Demo User",
        "email": "demo.user@training-simulator.local",
        "picture": None,
        "source": "demo",
    }


def clear_demo_login() -> None:
    """Remove demo auth markers from session state."""
    st.session_state.pop("demo_auth", None)


def is_oidc_logged_in() -> bool:
    user = getattr(st, "user", None)
    return bool(getattr(user, "is_logged_in", False)) if user else False


def get_identity() -> tuple[bool, str, str, str | None, str]:
    """Return auth state and user identity details from OIDC or demo mode."""
    if is_oidc_logged_in():
        return (
            True,
            get_user_field("name", "User") or "User",
            get_user_field("email", "") or "",
            get_user_field("picture", None),
            "google",
        )

    demo_auth = st.session_state.get("demo_auth")
    if isinstance(demo_auth, dict):
        return (
            True,
            str(demo_auth.get("name", "Demo User")),
            str(demo_auth.get("email", "demo.user@training-simulator.local")),
            demo_auth.get("picture"),
            str(demo_auth.get("source", "demo")),
        )

    return False, "", "", None, ""


def attempt_google_login() -> None:
    if not oauth_ready():
        st.warning("Google sign-in is not fully configured yet. Check setup diagnostics below.")
        return
    try:
        st.login("google")
    except Exception as exc:
        st.error("Google sign-in is not ready yet. Use Demo Login while setup is in progress.")
        st.caption(f"Details: {exc}")


def is_domain_allowed(email: str) -> bool:
    allowed_domains = oauth_config()["allowed_domains"]
    if not isinstance(allowed_domains, list) or not allowed_domains:
        return True
    if "@" not in email:
        return False
    domain = email.rsplit("@", 1)[1].lower().strip()
    return domain in {str(item).lower().strip() for item in allowed_domains}


def render_login_screen() -> None:
    """Render the logged-out view with Google and demo login options."""
    left, center, right = st.columns([1.15, 1.7, 1.15])
    with center:
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        st.markdown("## Training Simulator")
        st.markdown(
            '<p class="muted">A refreshed training workspace for simulations, coaching loops, and progress tracking.</p>',
            unsafe_allow_html=True,
        )

        col_google, col_demo = st.columns(2)
        with col_google:
            st.button(
                "Continue with Google",
                use_container_width=True,
                type="primary",
                on_click=attempt_google_login,
            )
        with col_demo:
            st.button(
                "Use Demo Login",
                use_container_width=True,
                on_click=set_demo_login,
            )

        st.markdown(
            '<p class="tiny-note">Demo Login gives you instant local access while Google OIDC configuration is in progress.</p>',
            unsafe_allow_html=True,
        )
        render_oauth_setup_panel()
        st.markdown("</div>", unsafe_allow_html=True)


def render_oauth_setup_panel() -> None:
    checks, hints = oauth_setup_checks()
    with st.expander("OAuth setup & troubleshooting", expanded=not oauth_ready()):
        for check in checks:
            icon = "✅" if check.passed else "❌"
            st.write(f"{icon} `{check.label}` — {check.detail}")
        if hints:
            st.markdown("**Recommended fixes**")
            for hint in hints:
                st.write(f"- {hint}")
        st.caption("After changes to `.streamlit/secrets.toml`, restart Streamlit.")


def handle_logout(auth_source: str) -> None:
    """Logout behavior depends on active auth provider."""
    if auth_source == "google":
        clear_demo_login()
        st.logout()
        return

    clear_demo_login()
    st.rerun()


def render_header(name: str, email: str, avatar_url: str | None, auth_source: str) -> None:
    """Render top header with title and profile summary."""
    title_col, profile_col = st.columns([3.4, 1.7])

    with title_col:
        st.markdown('<div class="app-header">', unsafe_allow_html=True)
        st.markdown('<div class="app-title">Training Simulator</div>', unsafe_allow_html=True)
        badge_text = "Google Auth" if auth_source == "google" else "Demo Auth"
        st.markdown(f'<div class="status-pill">{badge_text}</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with profile_col:
        st.markdown('<div class="profile-shell">', unsafe_allow_html=True)
        meta_col, action_col = st.columns([3, 1.1])
        with meta_col:
            if avatar_url:
                st.image(avatar_url, width=46)
            else:
                initial = (name[:1] if name else "U").upper()
                st.markdown(
                    f'<div class="avatar-fallback">{initial}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown(f"**{name}**")
            st.caption(email or "Email unavailable")
        with action_col:
            st.write("")
            st.write("")
            st.button(
                "Log out",
                key="header_logout",
                on_click=handle_logout,
                kwargs={"auth_source": auth_source},
                use_container_width=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard() -> None:
    """Render signed-in hero and dashboard placeholders."""
    st.markdown(
        """
        <div class="hero-card">
            <h3 style="margin: 0 0 0.45rem 0;">Welcome back 👋</h3>
            <p class="muted" style="margin: 0;">
                Build role-play scenarios, run sessions, and quickly close the coaching feedback loop.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cols = st.columns(3)
    for col, card in zip(cols, DASHBOARD_CARDS):
        with col:
            st.markdown(
                f"""
                <div class="dashboard-card">
                    <p class="card-title">{card.title}</p>
                    <p class="muted">{card.description}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_sidebar(email: str, auth_source: str) -> None:
    """Render sidebar placeholders and account controls."""
    with st.sidebar:
        st.markdown("### Navigation")
        st.radio(
            "Sections",
            ["Dashboard", "Scenarios", "Reports", "Coaching"],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("### Account")
        st.caption(email or "Email unavailable")
        st.caption(f"Session: {'Google' if auth_source == 'google' else 'Demo'}")
        st.button(
            "Log out",
            key="sidebar_logout",
            on_click=handle_logout,
            kwargs={"auth_source": auth_source},
            use_container_width=True,
        )


def render_debug_panel(auth_source: str) -> None:
    """Render safe auth-state debugging info with no secret exposure."""
    fields = {}
    for key in ["name", "email", "picture", "sub"]:
        value = get_user_field(key, None)
        fields[key] = value if value else "<missing>"

    with st.expander("Debug: Auth state", expanded=False):
        st.write({"oidc_logged_in": is_oidc_logged_in()})
        st.write({"active_auth_source": auth_source or "<none>"})
        st.write({"user_fields": fields})
        st.write({"demo_auth_present": bool(st.session_state.get("demo_auth"))})


def main() -> None:
    inject_ui_css()
    st.markdown('<div class="app-shell">', unsafe_allow_html=True)

    is_authenticated, name, email, avatar_url, auth_source = get_identity()

    if not is_authenticated:
        render_login_screen()
        render_debug_panel(auth_source="")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if auth_source == "google" and not is_domain_allowed(email):
        st.error("This Google account is outside the allowed email domains for this workspace.")
        st.info("Sign out and retry with an approved account, or update `auth.allowed_domains`.")
        st.button(
            "Log out",
            key="domain_block_logout",
            on_click=handle_logout,
            kwargs={"auth_source": auth_source},
            use_container_width=False,
        )
        render_debug_panel(auth_source=auth_source)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    render_sidebar(email=email, auth_source=auth_source)
    render_header(name=name, email=email, avatar_url=avatar_url, auth_source=auth_source)
    render_dashboard()
    render_debug_panel(auth_source=auth_source)
    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
