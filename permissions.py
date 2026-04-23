from __future__ import annotations

from typing import Any


DEV_EMAILS = {
    "domingo.jesse@gmail.com",
}


def normalize_email(email: str | None) -> str:
    normalized = (email or "").strip().lower()
    if "@" not in normalized:
        return normalized

    local_part, domain = normalized.split("@", 1)
    if domain in {"gmail.com", "googlemail.com"}:
        local_part = local_part.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    return f"{local_part}@{domain}"


def is_dev_account(user: dict[str, Any] | None) -> bool:
    if not user:
        return False

    normalized_dev_emails = {normalize_email(email) for email in DEV_EMAILS}
    return normalize_email(user.get("email")) in normalized_dev_emails
