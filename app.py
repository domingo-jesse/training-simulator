from __future__ import annotations

import streamlit as st


st.set_page_config(
    page_title="Training Simulator",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_ui_css() -> None:
    """Apply lightweight, professional styling for auth and dashboard shells."""
    st.markdown(
        """
        <style>
            .main > div {
                padding-top: 1.25rem;
            }

            .app-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 1rem;
                margin-bottom: 1rem;
            }

            .app-title {
                font-size: 1.6rem;
                font-weight: 700;
                letter-spacing: -0.01em;
                margin: 0;
            }

            .profile-shell {
                background: #ffffff;
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 14px;
                padding: 0.7rem 0.9rem;
                box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08);
            }

            .hero-card,
            .dashboard-card,
            .login-card {
                background: #ffffff;
                border: 1px solid rgba(15, 23, 42, 0.08);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(15, 23, 42, 0.06);
            }

            .hero-card {
                padding: 1.2rem 1.25rem;
                margin: 0.35rem 0 1rem 0;
            }

            .dashboard-card {
                padding: 1rem 1.05rem;
                min-height: 170px;
            }

            .login-card {
                padding: 2.1rem 1.6rem;
                margin-top: 9vh;
            }

            .card-title {
                margin: 0 0 0.35rem 0;
                font-size: 1.05rem;
                font-weight: 650;
            }

            .muted {
                color: #475569;
                font-size: 0.94rem;
                line-height: 1.5;
            }

            .tiny-note {
                margin-top: 0.5rem;
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


def render_login_screen() -> None:
    """Render the logged-out view with a centered sign-in card."""
    left, center, right = st.columns([1.25, 1.6, 1.25])
    with center:
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        st.markdown("## Training Simulator")
        st.markdown(
            '<p class="muted">An AI-powered training and evaluation workspace for teams to run realistic practice sessions, track progress, and improve outcomes.</p>',
            unsafe_allow_html=True,
        )

        if st.button("Continue with Google", use_container_width=True, type="primary"):
            try:
                st.login("google")
            except Exception as exc:
                st.error(
                    "Could not start Google sign-in. Please verify your Streamlit OIDC secrets and redirect URI configuration."
                )
                st.caption(f"Details: {exc}")

        st.markdown(
            '<p class="tiny-note">We use secure Google authentication via Streamlit OIDC. Your credentials are never handled directly by this app.</p>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)


def render_header(name: str, email: str, avatar_url: str | None) -> None:
    """Render top header with title and profile summary."""
    title_col, profile_col = st.columns([3.4, 1.7])

    with title_col:
        st.markdown('<div class="app-title">Training Simulator</div>', unsafe_allow_html=True)

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
            st.markdown(f"**{name or 'Signed-in user'}**")
            st.caption(email or "Email unavailable")
        with action_col:
            st.write("")
            st.write("")
            st.button("Log out", key="header_logout", on_click=st.logout, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard() -> None:
    """Render signed-in hero and dashboard placeholders."""
    st.markdown(
        """
        <div class="hero-card">
            <h3 style="margin: 0 0 0.45rem 0;">Welcome back 👋</h3>
            <p class="muted" style="margin: 0;">
                Launch role-play simulations, review outcomes, and iterate on coaching plans from one workspace.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cards = [
        (
            "Start Simulation",
            "Begin a new scenario session with configurable objectives, difficulty, and AI facilitator settings.",
        ),
        (
            "Review Performance",
            "Explore transcript highlights, scoring dimensions, and coaching notes from completed runs.",
        ),
        (
            "Admin / Settings",
            "Manage users, workspace defaults, evaluation rubrics, and system-level configuration options.",
        ),
    ]

    cols = st.columns(3)
    for col, (title, description) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class="dashboard-card">
                    <p class="card-title">{title}</p>
                    <p class="muted">{description}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_sidebar(email: str) -> None:
    """Render sidebar placeholders and account controls."""
    with st.sidebar:
        st.markdown("### Navigation")
        st.caption("Placeholder menu")
        st.radio(
            "Sections",
            ["Dashboard", "Scenarios", "Reports"],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("### Account")
        st.caption(email or "Email unavailable")
        st.button("Log out", key="sidebar_logout", on_click=st.logout, use_container_width=True)


def render_debug_panel() -> None:
    """Render safe auth-state debugging info with no secret exposure."""
    user = getattr(st, "user", None)
    is_logged_in = bool(getattr(user, "is_logged_in", False)) if user else False

    fields = {}
    for key in ["name", "email", "picture", "sub"]:
        value = get_user_field(key, None)
        fields[key] = value if value else "<missing>"

    with st.expander("Debug: Auth state", expanded=False):
        st.write({"st.user.is_logged_in": is_logged_in})
        st.write({"user_fields": fields})


def main() -> None:
    inject_ui_css()

    user = getattr(st, "user", None)
    is_logged_in = bool(getattr(user, "is_logged_in", False)) if user else False

    # Logged-out state: render only login UI.
    if not is_logged_in:
        render_login_screen()
        render_debug_panel()
        return

    name = get_user_field("name", "User") or "User"
    email = get_user_field("email", "") or ""
    avatar_url = get_user_field("picture", None)

    render_sidebar(email)
    render_header(name=name, email=email, avatar_url=avatar_url)
    render_dashboard()
    render_debug_panel()


if __name__ == "__main__":
    main()
