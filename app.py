from __future__ import annotations

from dataclasses import dataclass

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
    action_label: str


DASHBOARD_CARDS: tuple[DashboardCard, ...] = (
    DashboardCard(
        title="Start Simulation",
        description="Launch a new role-play session with scenario goals, challenge level, and guided facilitation.",
        action_label="Start",
    ),
    DashboardCard(
        title="Review Performance",
        description="Explore score trends, transcript highlights, and coaching recommendations for each learner.",
        action_label="Review",
    ),
    DashboardCard(
        title="Admin / Settings",
        description="Manage workspace policies, evaluation rubrics, team permissions, and platform defaults.",
        action_label="Open",
    ),
)


def inject_ui_css() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background: #f4f6f8;
                color: #0f172a;
            }

            .main > div {
                padding-top: 0.8rem;
                padding-bottom: 1.5rem;
            }

            [data-testid="stSidebar"] {
                background: #ffffff;
                border-right: 1px solid #e2e8f0;
            }

            .topbar {
                background: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 14px;
                padding: 0.95rem 1.15rem;
                margin-bottom: 1rem;
                box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            }

            .app-title {
                margin: 0;
                font-size: 1.28rem;
                font-weight: 700;
                letter-spacing: -0.01em;
                color: #0f172a;
            }

            .app-subtitle {
                margin-top: 0.2rem;
                font-size: 0.88rem;
                color: #64748b;
            }

            .profile-chip {
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 999px;
                padding: 0.33rem 0.72rem;
                font-size: 0.84rem;
                color: #334155;
            }

            .avatar-circle {
                width: 26px;
                height: 26px;
                border-radius: 999px;
                background: #cbd5e1;
                color: #0f172a;
                display: inline-flex;
                justify-content: center;
                align-items: center;
                font-size: 0.8rem;
                font-weight: 700;
            }

            .welcome-card {
                background: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 14px;
                padding: 1.35rem;
                margin-bottom: 1rem;
                box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            }

            .welcome-title {
                margin: 0 0 0.25rem 0;
                font-size: 1.3rem;
                font-weight: 700;
                color: #0f172a;
            }

            .muted {
                color: #475569;
                margin: 0;
                line-height: 1.5;
            }

            .dashboard-card {
                background: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 14px;
                padding: 1rem;
                min-height: 195px;
                box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            }

            .card-title {
                margin: 0 0 0.4rem 0;
                font-size: 1.02rem;
                font-weight: 650;
                color: #0f172a;
            }

            .settings-panel {
                background: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 14px;
                padding: 1rem 1.1rem;
                margin-bottom: 1rem;
                box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            }

            .section-label {
                margin-bottom: 0.2rem;
                font-size: 0.74rem;
                font-weight: 700;
                color: #64748b;
                text-transform: uppercase;
                letter-spacing: 0.08em;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _user_attr(field: str) -> str:
    user = getattr(st, "user", None)
    if user is None:
        return ""

    try:
        value = user.get(field, "")
    except Exception:
        value = getattr(user, field, "")

    return str(value).strip() if value else ""


def get_user_email() -> str:
    email = _user_attr("email")
    return email or "Signed-in user"


def get_user_display_name() -> str:
    name = _user_attr("name")
    if name:
        return name

    email = _user_attr("email")
    return email or "Signed-in user"


def get_user_initial() -> str:
    source = get_user_display_name()
    return source[0].upper() if source else "U"


def render_user_settings_panel() -> None:
    if not st.session_state.get("show_user_settings", False):
        return

    st.markdown('<div class="settings-panel">', unsafe_allow_html=True)
    st.markdown("#### User Settings")

    info_col, prefs_col = st.columns(2)
    with info_col:
        st.markdown('<p class="section-label">Account</p>', unsafe_allow_html=True)
        st.write(f"**Name:** {get_user_display_name()}")
        st.write(f"**Email:** {get_user_email()}")
        st.write("**Role:** Learner / Admin (placeholder)")

    with prefs_col:
        st.markdown('<p class="section-label">Preferences</p>', unsafe_allow_html=True)
        st.write("**Theme:** System default (placeholder)")
        st.write("**Notifications:** Weekly summary (placeholder)")
        st.caption("More profile options can be added later.")

    if st.button("Close Settings", key="close_user_settings"):
        st.session_state["show_user_settings"] = False
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def render_topbar() -> None:
    left, right = st.columns([6, 2.6], vertical_alignment="center")

    with left:
        st.markdown('<div class="topbar">', unsafe_allow_html=True)
        st.markdown('<h1 class="app-title">Training Simulator</h1>', unsafe_allow_html=True)
        st.markdown(
            '<div class="app-subtitle">Internal enablement workspace for realistic training, scoring, and coaching.</div>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown(
            (
                f'<div class="profile-chip"><span class="avatar-circle">{get_user_initial()}</span>'
                f'<span>{get_user_display_name()}</span></div>'
            ),
            unsafe_allow_html=True,
        )
        with st.popover("Profile", use_container_width=True):
            st.caption(get_user_email())
            if st.button("User Settings", use_container_width=True, key="menu_user_settings"):
                st.session_state["show_user_settings"] = True
                st.rerun()
            if st.button("Log Out", use_container_width=True, key="menu_logout"):
                st.logout()


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### Training Simulator")
        st.markdown('<p class="section-label">App Section</p>', unsafe_allow_html=True)
        st.radio(
            "Navigation",
            ["Dashboard", "Simulations", "Performance", "Team Management"],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("**Signed in as**")
        st.caption(get_user_email())


def render_welcome_card() -> None:
    st.markdown(
        f"""
        <div class="welcome-card">
            <h2 class="welcome-title">Welcome back, {get_user_display_name()}.</h2>
            <p class="muted">
                Continue building high-quality training experiences with simulation workflows,
                structured feedback, and measurable outcomes.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_cards() -> None:
    columns = st.columns(3)
    for column, card in zip(columns, DASHBOARD_CARDS):
        with column:
            st.markdown(
                f"""
                <div class="dashboard-card">
                    <h3 class="card-title">{card.title}</h3>
                    <p class="muted">{card.description}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.button(card.action_label, key=f"cta_{card.title}", use_container_width=True)


def main() -> None:
    inject_ui_css()

    if not bool(getattr(st.user, "is_logged_in", False)):
        st.markdown("## Training Simulator")
        st.info("Please sign in with Google to access your dashboard.")
        if st.button("Sign in with Google", type="primary"):
            st.login("google")
        return

    render_sidebar()
    render_topbar()
    render_user_settings_panel()
    render_welcome_card()
    render_dashboard_cards()


if __name__ == "__main__":
    main()
