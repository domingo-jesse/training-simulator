# Flicker Reduction Opportunities

This document tracks UI changes that reduce perceived flicker during Streamlit reruns.

## Implemented in this change

### 1) Remove redundant explicit reruns for account-page navigation

**Problem**
- The `Settings` and `Back to Dashboard` actions are triggered by Streamlit buttons.
- Streamlit buttons already trigger a rerun after click handling.
- Calling `st.rerun()` again inside the navigation helpers can cause an extra immediate rerun cycle, which increases visible flicker.

**Implementation**
- Updated `_navigate_to_account_page(...)` and `_navigate_back_to_main_app(...)` in `app.py` to update state/query params without calling `st.rerun()`.

**Why this is safe**
- Navigation state (`current_page`, `page`, `nav`, and role-specific page keys) is still written before the normal button-triggered rerun completes.
- The app still lands on the same destination page, with fewer rerender passes.
