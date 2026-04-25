# Oura OAuth (V2)

This project uses Oura OAuth2 as documented at:

- `https://cloud.ouraring.com/docs/authentication`
- Authorize URL: `https://cloud.ouraring.com/oauth/authorize`
- Token URL: `https://api.ouraring.com/oauth/token`
- Revoke URL: `https://api.ouraring.com/oauth/revoke`

## 1) Configure `.env`

Required for OAuth flow:

- `OURA_CLIENT_ID`
- `OURA_CLIENT_SECRET`
- `OURA_REDIRECT_URI`

Recommended:

- `OURA_SCOPES=daily spo2 tag session workout heartrate personal email`
- `OURA_TOKEN_STORE_PATH=data/oura_tokens.json`
- `OURA_AUTO_REFRESH=true`

Optional direct token variables:

- `OURA_ACCESS_TOKEN`
- `OURA_REFRESH_TOKEN`

## 2) Get authorization URL

```bash
uv run life oura-oauth-url --state "life-init"
```

Open the returned URL, authorize app access, and copy the redirect URL.

## 3) Exchange code for tokens

Using full redirect URL:

```bash
uv run life oura-oauth-exchange --redirect-url "https://your-redirect?code=...&scope=...&state=..." --save
```

Or using just code:

```bash
uv run life oura-oauth-exchange --code "<code>" --save
```

`--save` stores tokens in `OURA_TOKEN_STORE_PATH`.

## 4) Refresh token manually

```bash
uv run life oura-oauth-refresh --refresh-token "<refresh_token>" --save
```

If omitted, the command attempts refresh token from token store.

## 5) Automatic refresh in pipeline

Before non-OAuth commands (`backfill-legacy`, `bridge-to-today`, `sync-incremental`), the CLI now:

1. Uses `OURA_ACCESS_TOKEN` if present
2. Else uses stored access token from `OURA_TOKEN_STORE_PATH`
3. Else, if `OURA_AUTO_REFRESH=true` and refresh token is available, refreshes automatically and stores new tokens

Notes:

- Oura refresh tokens are single-use; each refresh returns a new refresh token. Keep the latest one.
- Keep token store out of git (`data/` is ignored).

## 6) OAuth status command

To inspect token availability and effective token source (without printing secrets):

```bash
uv run life oura-oauth-status
```

Returns JSON fields like:

- `has_env_access_token`
- `has_stored_access_token`
- `effective_access_token_source` (`env`, `token_store`, `refresh`, `none`)
- `refresh_possible`
