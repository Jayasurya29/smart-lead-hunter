"""
SMART LEAD HUNTER — Auth Middleware
Pure ASGI implementation — avoids BaseHTTPMiddleware's streaming cancellation bug.
"""

import logging
import os
import secrets
import time

from dotenv import load_dotenv
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

load_dotenv()

logger = logging.getLogger(__name__)

# AUDIT 2026-05-05 (bug #17): TTL cache for User.is_active lookups so the
# middleware doesn't run a DB query on every authed request. 60-second TTL
# means a deactivated user keeps access for at most 60s after deactivation —
# acceptable for an internal LAN-only sales tool. Manual invalidation via
# the public `_clear_user_active_cache()` helper from auth.py would shorten
# this to zero if needed.
_USER_ACTIVE_TTL_SECONDS = 60
_user_active_cache: dict[str, tuple[bool, float]] = {}


async def _is_user_active(user_id: str) -> bool:
    """Return True if the user exists and User.is_active is True.

    Cached for 60s per user_id. On any DB error, returns False
    conservatively — better to reject one good request than to wave
    through a deactivated user during a transient DB blip.
    """
    if not user_id:
        return False
    now = time.monotonic()
    cached = _user_active_cache.get(user_id)
    if cached is not None:
        is_active, ts = cached
        if now - ts < _USER_ACTIVE_TTL_SECONDS:
            return is_active

    try:
        from app.database import async_session
        from app.models.user import User
        from sqlalchemy import select

        # user_id from JWT 'sub' claim is a string; coerce to int for query
        try:
            uid_int = int(user_id)
        except (TypeError, ValueError):
            return False

        async with async_session() as session:
            result = await session.execute(
                select(User.is_active).where(User.id == uid_int)
            )
            row = result.scalar_one_or_none()
            is_active = bool(row) if row is not None else False
    except Exception as e:
        logger.warning(f"_is_user_active DB lookup failed for {user_id!r}: {e}")
        return False

    _user_active_cache[user_id] = (is_active, now)
    return is_active


def _clear_user_active_cache(user_id: str | None = None) -> None:
    """Force re-fetch on next request. Call from auth.py on deactivate."""
    if user_id is None:
        _user_active_cache.clear()
    else:
        _user_active_cache.pop(user_id, None)


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
        "/stats",
        "/api/dashboard/sources/list",
        "/api/auth/verify",
    )

    # Only these prefixes require auth
    # AUDIT 2026-05-05 (bug #22): /stats was in both EXCLUDE_PREFIXES and
    # PROTECTED_PREFIXES — exclude check fires first, so /stats has been
    # publicly accessible all along. Removing the duplicate from PROTECTED
    # so the policy is consistent. /stats stays public (matches today's
    # behavior — sales dashboard widget polls it without auth headers).
    # If sales policy later requires auth, remove it from EXCLUDE_PREFIXES
    # above instead.
    #
    # AUDIT 2026-05-06 (CRIT-1): Added /revenue and /discovery. The
    # revenue router (mounted at prefix /revenue) had two POST endpoints
    # that mutate DB state — bulk-update and update/{lead_id} — and was
    # reachable without any auth at all because /revenue matched none of
    # the prefixes below. Same problem for /discovery/queries (read-only
    # but leaks operational query intelligence). Per-route deps were also
    # missing on these routes; lifting auth into the prefix list keeps the
    # protection symmetric with /leads, /sources, /scrape and avoids
    # depending on every future route adding its own require_admin.
    PROTECTED_PREFIXES: tuple[str, ...] = (
        "/api/",
        "/leads",
        "/sources",
        "/scrape",
        "/revenue",
        "/discovery",
    )

    def __init__(self, app: ASGIApp, exclude_paths: list[str] | None = None):
        self.app = app
        # Allow callers to extend exclude_exact at init time
        if exclude_paths:
            self._exclude_exact = self.EXCLUDE_EXACT | set(exclude_paths)
        else:
            self._exclude_exact = self.EXCLUDE_EXACT
        self.exclude_exact = self._exclude_exact
        self.exclude_prefixes = list(self.EXCLUDE_PREFIXES)
        self.protected_prefixes = list(self.PROTECTED_PREFIXES)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Only intercept HTTP requests
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        path = request.url.path

        # ── CORS preflight — always pass through so CORSMiddleware can respond ──
        if request.method == "OPTIONS":
            await self.app(scope, receive, send)
            return

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
            _jwt_authed = False
            try:
                from jose import jwt as jose_jwt

                jwt_secret = os.getenv("JWT_SECRET_KEY", "")
                _insecure_keys = {
                    "",
                    "CHANGE_ME_32_CHARS_MINIMUM_SECRET",
                    "dev-only-insecure-key-do-not-use-in-production",
                }
                env = os.getenv("ENVIRONMENT", "development")

                payload = None
                if env == "production" and jwt_secret in _insecure_keys:
                    pass  # Reject JWT auth in production with no real secret
                elif jwt_secret and jwt_secret not in _insecure_keys:
                    payload = jose_jwt.decode(
                        jwt_cookie, jwt_secret, algorithms=["HS256"]
                    )
                elif env != "production":
                    fallback = (
                        jwt_secret or "dev-only-insecure-key-do-not-use-in-production"
                    )
                    payload = jose_jwt.decode(
                        jwt_cookie, fallback, algorithms=["HS256"]
                    )

                # AUDIT 2026-05-05 (bug #17): Verify User.is_active before
                # accepting the JWT. Previously the middleware accepted any
                # decoded JWT with a `sub` claim — a deactivated user kept
                # full access until the token expired (8h or 30 days for
                # "remember me"). Now we look up the user and check
                # is_active. The result is cached for 60 seconds keyed by
                # user_id to keep the hot path fast (single DB lookup per
                # user per minute, not per request).
                if payload and payload.get("sub"):
                    user_id_str = str(payload["sub"])
                    if await _is_user_active(user_id_str):
                        _jwt_authed = True
                    else:
                        # Deactivated user — fall through to deny
                        logger.info(
                            f"Auth middleware rejected JWT for deactivated "
                            f"user_id={user_id_str}"
                        )
            except Exception:
                pass

            # FIX 2026-06-01: Call the downstream app OUTSIDE the JWT-decode
            # try/except above. Previously `await self.app(...)` lived INSIDE
            # that try, so ANY route exception (e.g. one bad lead row raising
            # a ResponseValidationError in /leads) was swallowed by
            # `except Exception: pass` and execution fell through to the 401
            # reject block — surfacing a real 500 as a fake "session expired".
            # That masked the actual error and bounced authenticated users to
            # /login. The try/except must guard JWT decoding only, never the
            # request handling itself.
            if _jwt_authed:
                await self.app(scope, receive, send)
                return

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
