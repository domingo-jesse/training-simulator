from __future__ import annotations

from typing import Any


DEV_EMAILS = {
    "domingo.jesse@gmail.com",
}


def normalize_email(email: str | None) -> str:
    return (email or "").strip().lower()


def is_dev_account(user: dict[str, Any] | None) -> bool:
    if not user:
        return False
    return normalize_email(user.get("email")) in DEV_EMAILS
