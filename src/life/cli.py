from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from life.config import load_settings
from life.logging import configure_logging, get_logger
from life.oauth import OuraOAuthClient, OuraTokenStore
from life.pipeline import backfill_legacy, bridge_to_today, sync_incremental
from life.storage.duckdb import DuckDBStorage


@dataclass
class OAuthContext:
    oauth: OuraOAuthClient
    token_store: OuraTokenStore


def _resolve_oura_access_token_source(
    *,
    env_access_token: str | None,
    stored_access_token: str | None,
    auto_refresh: bool,
    refresh_token: str | None,
    oauth_configured: bool,
) -> str:
    if env_access_token:
        return "env"
    if stored_access_token:
        return "token_store"
    if auto_refresh and refresh_token and oauth_configured:
        return "refresh"
    return "none"


def _extract_code_from_redirect_url(redirect_url: str) -> str | None:
    cleaned = (
        redirect_url.replace("\\?", "?")
        .replace("\\&", "&")
        .replace("\\=", "=")
        .replace("&amp;", "&")
    )
    parsed = urlparse(cleaned)
    code = parse_qs(parsed.query).get("code", [None])[0]
    if code:
        return code

    if parsed.fragment:
        return parse_qs(parsed.fragment).get("code", [None])[0]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Life data engineering pipeline")
    parser.add_argument(
        "command",
        choices=[
            "backfill-legacy",
            "bridge-to-today",
            "sync-incremental",
            "oura-oauth-url",
            "oura-oauth-exchange",
            "oura-oauth-refresh",
            "oura-oauth-status",
        ],
        help="Pipeline command",
    )
    parser.add_argument("--state", default="life-oauth", help="OAuth state")
    parser.add_argument("--code", default=None, help="OAuth authorization code")
    parser.add_argument(
        "--redirect-url",
        default=None,
        help="Full redirect URL containing code query parameter",
    )
    parser.add_argument("--refresh-token", default=None, help="Oura refresh token")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save returned tokens to token store",
    )
    args = parser.parse_args()

    configure_logging()
    logger = get_logger(__name__)
    settings = load_settings()
    storage = DuckDBStorage(settings.duckdb_path)

    logger.info("pipeline start", command=args.command, duckdb_path=str(settings.duckdb_path))

    oauth_context = _build_oauth_context(settings)

    if args.command == "oura-oauth-status":
        _run_oura_oauth_status(settings, oauth_context)
        logger.info("pipeline done", command=args.command)
        return

    if args.command.startswith("oura-oauth"):
        if oauth_context is None:
            raise ValueError(
                "Missing required OAuth env vars: OURA_CLIENT_ID, "
                "OURA_CLIENT_SECRET, OURA_REDIRECT_URI"
            )
        _run_oura_oauth_command(args, oauth_context)
        logger.info("pipeline done", command=args.command)
        return

    _ensure_oura_access_token(settings, oauth_context, logger)

    if args.command == "backfill-legacy":
        backfill_legacy.run(storage, settings)
    elif args.command == "bridge-to-today":
        bridge_to_today.run(storage, settings)
    else:
        sync_incremental.run(storage, settings)

    logger.info("pipeline done", command=args.command)


def _build_oauth_context(settings) -> OAuthContext | None:
    missing_client_id = not settings.oura_client_id
    missing_client_secret = not settings.oura_client_secret
    missing_redirect_uri = not settings.oura_redirect_uri
    if missing_client_id or missing_client_secret or missing_redirect_uri:
        return None

    oauth = OuraOAuthClient(
        authorize_url=settings.oura_oauth_authorize_url,
        token_url=settings.oura_oauth_token_url,
        revoke_url=settings.oura_oauth_revoke_url,
        client_id=settings.oura_client_id or "",
        client_secret=settings.oura_client_secret or "",
        redirect_uri=settings.oura_redirect_uri or "",
        scope=settings.oura_scopes,
    )
    token_store = OuraTokenStore(settings.oura_token_store_path)
    return OAuthContext(oauth=oauth, token_store=token_store)


def _run_oura_oauth_command(args, context: OAuthContext) -> None:
    oauth = context.oauth
    token_store = context.token_store

    if args.command == "oura-oauth-url":
        print(oauth.build_authorize_url(state=args.state, response_type="code"))
        return

    if args.command == "oura-oauth-exchange":
        code = args.code
        if args.redirect_url and not code:
            code = _extract_code_from_redirect_url(args.redirect_url)
        if not code:
            raise ValueError("Provide --code or --redirect-url containing code=")
        token_data = oauth.exchange_code(code)
        if args.save:
            token_store.set_tokens(
                access_token=token_data.get("access_token", ""),
                refresh_token=token_data.get("refresh_token"),
                expires_in=token_data.get("expires_in"),
                token_type=token_data.get("token_type"),
                scope=token_data.get("scope"),
            )
        print(json.dumps(token_data, indent=2, ensure_ascii=True))
        return

    if args.command == "oura-oauth-refresh":
        refresh_token = args.refresh_token or token_store.get_refresh_token()
        if not refresh_token:
            raise ValueError("Provide --refresh-token or save one in token store")
        token_data = oauth.refresh_token(refresh_token)
        if args.save:
            token_store.set_tokens(
                access_token=token_data.get("access_token", ""),
                refresh_token=token_data.get("refresh_token"),
                expires_in=token_data.get("expires_in"),
                token_type=token_data.get("token_type"),
                scope=token_data.get("scope"),
            )
        print(json.dumps(token_data, indent=2, ensure_ascii=True))
        return


def _ensure_oura_access_token(settings, oauth_context: OAuthContext | None, logger) -> None:
    token_store = OuraTokenStore(settings.oura_token_store_path)

    if settings.oura_access_token:
        token_store.set_tokens(
            access_token=settings.oura_access_token,
            refresh_token=settings.oura_refresh_token,
            expires_in=None,
            token_type="bearer",
            scope=settings.oura_scopes,
        )
        return

    persisted_access = token_store.get_access_token()
    if persisted_access:
        settings.oura_access_token = persisted_access
        return

    if not settings.oura_auto_refresh:
        return

    refresh_token = settings.oura_refresh_token or token_store.get_refresh_token()
    if not refresh_token:
        return

    if oauth_context is None:
        logger.warning("cannot auto refresh oura token", reason="missing oauth client settings")
        return

    logger.info("refreshing oura access token")
    token_data = oauth_context.oauth.refresh_token(refresh_token)
    access = token_data.get("access_token")
    if not access:
        raise ValueError("Oura token refresh did not return access_token")
    settings.oura_access_token = access
    oauth_context.token_store.set_tokens(
        access_token=access,
        refresh_token=token_data.get("refresh_token"),
        expires_in=token_data.get("expires_in"),
        token_type=token_data.get("token_type"),
        scope=token_data.get("scope"),
    )


def _run_oura_oauth_status(settings, oauth_context: OAuthContext | None) -> None:
    token_store = OuraTokenStore(settings.oura_token_store_path)
    stored_access = token_store.get_access_token()
    stored_refresh = token_store.get_refresh_token()
    env_refresh = settings.oura_refresh_token

    refresh_candidate = env_refresh or stored_refresh
    source = _resolve_oura_access_token_source(
        env_access_token=settings.oura_access_token,
        stored_access_token=stored_access,
        auto_refresh=settings.oura_auto_refresh,
        refresh_token=refresh_candidate,
        oauth_configured=oauth_context is not None,
    )

    status = {
        "token_store_path": str(settings.oura_token_store_path),
        "token_store_exists": settings.oura_token_store_path.exists(),
        "has_env_access_token": bool(settings.oura_access_token),
        "has_env_refresh_token": bool(env_refresh),
        "has_stored_access_token": bool(stored_access),
        "has_stored_refresh_token": bool(stored_refresh),
        "auto_refresh_enabled": settings.oura_auto_refresh,
        "oauth_client_configured": oauth_context is not None,
        "effective_access_token_source": source,
        "refresh_possible": bool(
            settings.oura_auto_refresh and refresh_candidate and oauth_context is not None
        ),
    }
    print(json.dumps(status, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
