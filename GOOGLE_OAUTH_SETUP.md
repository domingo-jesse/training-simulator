# Google OAuth setup (Streamlit)

This project uses Streamlit's OIDC support with the Google identity provider.

## 1) Add app config (`.streamlit/secrets.toml`)

1. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`.
2. Fill in:
   - `auth.redirect_uri`
   - `auth.cookie_secret`
   - `auth.google.client_id`
   - `auth.google.client_secret`
   - `auth.google.server_metadata_url`

> Do not commit real secrets.

## 2) Wire up Google Sign-In

Google Sign-In is already wired in `app.py`:
- `st.login("google")` starts OAuth.
- `st.user` fields are used after login.
- `st.logout()` is supported.
- Domain allow-listing is controlled by `auth.allowed_domains`.

## 3) Validate OAuth setup

The login screen includes an **"OAuth setup & troubleshooting"** panel that checks:
- missing redirect URI
- missing cookie secret
- missing client ID/secret
- metadata URL issues
- common redirect URI formatting mistakes

## 4) Troubleshoot common auth issues

- `redirect_uri_mismatch`
  - ensure Google Cloud "Authorized redirect URIs" exactly matches `auth.redirect_uri`
- `invalid_client`
  - verify client ID + client secret are from the same Google OAuth app
- login loop / no session
  - clear app cookies and restart Streamlit
- post-login access denied
  - verify your email domain appears in `auth.allowed_domains`
