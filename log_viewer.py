from __future__ import annotations

from pathlib import Path

import streamlit as st


LOG_LEVEL_OPTIONS = ["ALL", "DEBUG", "INFO", "WARNING", "ERROR"]


def is_admin_session() -> bool:
    """Temporary admin check based on session state."""
    current_user = st.session_state.get("current_user") or {}
    return bool(
        st.session_state.get("is_admin")
        or current_user.get("role") == "admin"
        or st.session_state.get("selected_role") == "admin"
    )


def read_log_lines(log_path: str) -> tuple[list[str], str | None]:
    """Read log file as lines and return an optional error message."""
    file_path = Path(log_path)
    if not file_path.exists():
        return [], f"Log file not found: {log_path}"

    try:
        content = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return [], f"Unable to read {log_path}: {exc}"

    return content.splitlines(), None


def filter_log_lines(lines: list[str], search_text: str, level: str) -> list[str]:
    """Filter in memory by level and free-text search."""
    filtered = lines

    if level != "ALL":
        level_token = level.upper()
        filtered = [line for line in filtered if level_token in line.upper()]

    query = (search_text or "").strip().lower()
    if query:
        filtered = [line for line in filtered if query in line.lower()]

    return filtered


def get_recent_lines(lines: list[str], max_lines: int) -> list[str]:
    if max_lines <= 0:
        return []
    return lines[-max_lines:]


def read_full_file_for_download(log_path: str) -> tuple[bytes | None, str | None]:
    file_path = Path(log_path)
    if not file_path.exists():
        return None, f"Log file not found: {log_path}"

    try:
        return file_path.read_bytes(), None
    except OSError as exc:
        return None, f"Unable to load {log_path} for download: {exc}"
