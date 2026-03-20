"""
SMART LEAD HUNTER — API Key Authentication Middleware
Fix: SH4 (no authentication on endpoints)

Usage in main.py:
    from app.middleware.auth import api_key_auth

    # Protect individual endpoints:
    @app.get("/api/leads", dependencies=[Depends(api_key_auth)])

    # Or protect all /api/ routes via middleware (add to main.py startup):
    from app.middleware.auth import APIKeyMiddleware
    app.add_middleware(APIKeyMiddleware, exclude_paths=["/health", "/dashboard", "/docs", "/openapi.json"])

Environment:
    API_AUTH_KEY=your-secret-key-here   # Add to .env
"""

import os
import secrets
from dotenv import load_dotenv


from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

load_dotenv()

# ─── Simple Dependency (per-endpoint) ───

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _get_valid_api_key() -> str:
    """Get the configured API key from environment."""
    key = os.getenv("API_AUTH_KEY", "")
    if not key:
        raise RuntimeError(
            "API_AUTH_KEY not set in environment. Add API_AUTH_KEY=<your-key> to .env"
        )
    return key


async def api_key_auth(api_key: str = Security(_api_key_header)) -> str:
    """
    FastAPI dependency — validates X-API-Key header.

    Usage:
        @app.get("/api/leads", dependencies=[Depends(api_key_auth)])
    """
    valid_key = _get_valid_api_key()

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    # Constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(api_key, valid_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

    return api_key


# ─── Middleware (global route protection) ───


class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    Middleware that protects all API and REST routes with API key auth.

    FIX C-01: Removed "/api/dashboard/leads" from exclude list — it was
    a prefix match that bypassed auth for ALL lead sub-routes including
    edit, approve, reject, enrich, contacts, etc.

    FIX C-02: Now also protects /leads, /sources, /stats, /scrape routes
    (previously only /api/* was protected).

    Usage:
        app.add_middleware(APIKeyMiddleware)
    """

    def __init__(self, app, exclude_paths: list[str] | None = None):
        super().__init__(app)
        # Exact path matches — only these specific paths skip auth
        self.exclude_exact = set(
            exclude_paths
            or [
                "/health",
                "/docs",
                "/redoc",
                "/openapi.json",
            ]
        )
        # Prefix matches — ALL sub-routes under these are public
        self.exclude_prefixes = [
            "/static",  # Static files
            "/auth/",  # Login/register/verify (must work without auth)
            # SSE streams: EventSource can't send custom headers;
            # gated by one-time scrape_id/discovery_id tokens
            "/api/dashboard/scrape/stream",
            "/api/dashboard/extract-url/stream",
            "/api/dashboard/discovery/stream",
            # Read-only endpoints
            "/api/dashboard/stats",
            "/api/dashboard/sources/list",
            # Auth verification endpoint (must work without auth to verify keys)
            "/api/auth/verify",
        ]
        # NOTE: "/api/dashboard/leads" is intentionally NOT excluded.
        # The old exclude was a prefix match that bypassed auth for
        # /api/dashboard/leads/{id}/edit, /approve, /reject, /enrich,
        # /contacts, etc.  All of those now require auth.

        # FIX C-02: Routes that MUST be protected (not just /api/)
        self.protected_prefixes = [
            "/api/",
            "/leads",
            "/sources",
            "/stats",
            "/scrape",
        ]

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Exact path exclusions (root, health, docs)
        if path in self.exclude_exact or path == "/":
            return await call_next(request)

        # Prefix exclusions (for genuinely public route trees)
        if any(path.startswith(p) for p in self.exclude_prefixes):
            return await call_next(request)

        # FIX C-02: Check if this path actually needs protection.
        # Only paths matching protected_prefixes require auth.
        # Everything else (favicon, robots.txt, etc.) passes through.
        needs_auth = any(path.startswith(p) for p in self.protected_prefixes)
        if not needs_auth:
            return await call_next(request)

        # ── AUTH CHECK: API Key OR JWT Cookie ──
        # Method 1: X-API-Key header (for scripts, Celery, automation)
        api_key = request.headers.get("X-API-Key", "")
        # Method 2: api_key query param (for EventSource which can't set headers)
        if not api_key:
            api_key = request.query_params.get("api_key", "")

        # Check API key first (fast path for automation)
        if api_key:
            try:
                valid_key = _get_valid_api_key()
                if secrets.compare_digest(api_key, valid_key):
                    return await call_next(request)
            except RuntimeError:
                pass  # API_AUTH_KEY not set — try JWT below

        # Method 3: JWT cookie (for browser sessions)
        jwt_cookie = request.cookies.get("slh_session", "")
        if jwt_cookie:
            try:
                from jose import jwt as jose_jwt, JWTError

                jwt_secret = os.getenv("JWT_SECRET_KEY", "")
                # FIX C-02: Block insecure dev key in production
                _insecure_keys = {
                    "",
                    "CHANGE_ME_32_CHARS_MINIMUM_SECRET",
                    "dev-only-insecure-key-do-not-use-in-production",
                }
                env = os.getenv("ENVIRONMENT", "development")
                if env == "production" and jwt_secret in _insecure_keys:
                    # Production with no real secret — reject JWT auth entirely
                    pass
                elif jwt_secret and jwt_secret not in _insecure_keys:
                    payload = jose_jwt.decode(
                        jwt_cookie, jwt_secret, algorithms=["HS256"]
                    )
                    if payload.get("sub"):
                        return await call_next(request)
                elif env != "production":
                    # Dev mode — allow the insecure key
                    fallback = (
                        jwt_secret or "dev-only-insecure-key-do-not-use-in-production"
                    )
                    payload = jose_jwt.decode(
                        jwt_cookie, fallback, algorithms=["HS256"]
                    )
                    if payload.get("sub"):
                        return await call_next(request)
            except (JWTError, Exception):
                pass  # Invalid/expired token — fall through to reject

        # Neither method worked — check if we should allow dev bypass
        if not api_key and not jwt_cookie:
            try:
                _get_valid_api_key()
            except RuntimeError:
                env = os.getenv("ENVIRONMENT", "development")
                if env == "production":
                    return JSONResponse(
                        status_code=500,
                        content={"detail": "Server misconfigured: no auth configured"},
                    )
                import logging as _logging

                _logging.getLogger(__name__).warning(
                    "No auth configured — allowing request (dev mode only)"
                )
                return await call_next(request)

        return JSONResponse(
            status_code=401,
            content={
                "detail": "Authentication required. Provide X-API-Key header or sign in."
            },
        )
