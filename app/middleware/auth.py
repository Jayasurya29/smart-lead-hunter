"""
SMART LEAD HUNTER — Auth Middleware
Pure ASGI implementation — avoids BaseHTTPMiddleware's streaming cancellation bug.
"""

import logging
import os
import secrets

from dotenv import load_dotenv
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Simple Dependency (per-endpoint) ───

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _get_valid_api_key() -> str:
    key = os.getenv("API_AUTH_KEY", "")
    if not key:
        raise RuntimeError(
            "API_AUTH_KEY not set in environment. Add API_AUTH_KEY=<your-key> to .env"
        )
    return key


async def api_key_auth(api_key: str = Security(_api_key_header)) -> str:
    valid_key = _get_valid_api_key()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )
    if not secrets.compare_digest(api_key, valid_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )
    return api_key


# ─── Pure ASGI Middleware ───
# Does NOT extend BaseHTTPMiddleware — safe for SSE / StreamingResponse.


class APIKeyMiddleware:
    """
    Pure ASGI auth middleware. Validates X-API-Key header or JWT cookie.
    Does not buffer responses — safe for SSE streaming endpoints.
    """

    # Exact paths that skip auth entirely
    EXCLUDE_EXACT: frozenset[str] = frozenset(
        {
            "/",
            "/health",
            "/docs",
            "/redoc",
            "/openapi.json",
        }
    )

    # Prefix matches — all sub-routes under these are public
    EXCLUDE_PREFIXES: tuple[str, ...] = (
        "/static/",
        "/auth/",
        "/assets/",
        # SSE streams: gated by one-time scrape_id/extract_id tokens
        "/api/dashboard/scrape/stream",
        "/api/dashboard/extract-url/stream",
        "/api/dashboard/discovery/stream",
        # Read-only public endpoints
        "/api/dashboard/stats",
        "/api/dashboard/sources/list",
        "/api/auth/verify",
    )

    # Only these prefixes require auth
    PROTECTED_PREFIXES: tuple[str, ...] = (
        "/api/",
        "/leads",
        "/sources",
        "/stats",
        "/scrape",
    )

    def __init__(self, app: ASGIApp, exclude_paths: list[str] | None = None):
        self.app = app
        # Allow callers to extend exclude_exact at init time
        if exclude_paths:
            self._exclude_exact = self.EXCLUDE_EXACT | set(exclude_paths)
        else:
            self._exclude_exact = self.EXCLUDE_EXACT

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Only intercept HTTP requests
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        path = request.url.path

        # ── Public paths — pass straight through ──
        if path in self._exclude_exact:
            await self.app(scope, receive, send)
            return

        if path.startswith(self.EXCLUDE_PREFIXES):
            await self.app(scope, receive, send)
            return

        # ── Only protect designated prefixes ──
        if not path.startswith(self.PROTECTED_PREFIXES):
            await self.app(scope, receive, send)
            return

        # ── AUTH CHECK ──
        # Method 1: X-API-Key header
        api_key = request.headers.get("X-API-Key", "")
        # Method 2: api_key query param (EventSource fallback)
        if not api_key:
            api_key = request.query_params.get("api_key", "")

        if api_key:
            try:
                valid_key = _get_valid_api_key()
                if secrets.compare_digest(api_key, valid_key):
                    await self.app(scope, receive, send)
                    return
            except RuntimeError:
                pass

        # Method 3: JWT cookie
        jwt_cookie = request.cookies.get("slh_session", "")
        if jwt_cookie:
            try:
                from jose import jwt as jose_jwt

                jwt_secret = os.getenv("JWT_SECRET_KEY", "")
                _insecure_keys = {
                    "",
                    "CHANGE_ME_32_CHARS_MINIMUM_SECRET",
                    "dev-only-insecure-key-do-not-use-in-production",
                }
                env = os.getenv("ENVIRONMENT", "development")

                if env == "production" and jwt_secret in _insecure_keys:
                    pass  # Reject JWT auth in production with no real secret
                elif jwt_secret and jwt_secret not in _insecure_keys:
                    payload = jose_jwt.decode(
                        jwt_cookie, jwt_secret, algorithms=["HS256"]
                    )
                    if payload.get("sub"):
                        await self.app(scope, receive, send)
                        return
                elif env != "production":
                    fallback = (
                        jwt_secret or "dev-only-insecure-key-do-not-use-in-production"
                    )
                    payload = jose_jwt.decode(
                        jwt_cookie, fallback, algorithms=["HS256"]
                    )
                    if payload.get("sub"):
                        await self.app(scope, receive, send)
                        return
            except Exception:
                pass

        # Dev bypass when no auth configured at all
        if not api_key and not jwt_cookie:
            try:
                _get_valid_api_key()
            except RuntimeError:
                env = os.getenv("ENVIRONMENT", "development")
                if env == "production":
                    resp = JSONResponse(
                        status_code=500,
                        content={"detail": "Server misconfigured: no auth configured"},
                    )
                    await resp(scope, receive, send)
                    return
                logger.warning("No auth configured — allowing request (dev mode only)")
                await self.app(scope, receive, send)
                return

        # ── Reject ──
        resp = JSONResponse(
            status_code=401,
            content={
                "detail": "Authentication required. Provide X-API-Key header or sign in."
            },
        )
        await resp(scope, receive, send)
