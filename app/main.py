"""
Smart Lead Hunter - Main Application
------------------------------------
FastAPI entry point. Route handlers live in app/routes/.

Run with:
    uvicorn app.main:app --reload --port 8000
"""

import logging
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.types import ASGIApp, Receive, Scope, Send

from app.config import settings
from app.logging_config import setup_logging
from app.database import init_db
from app.middleware.auth import APIKeyMiddleware

# Route modules
from app.routes.health import router as health_router
from app.routes.leads import router as leads_router
from app.routes.sources import router as sources_router
from app.routes.dashboard import router as dashboard_router
from app.routes.scraping import router as scraping_router
from app.routes.contacts import router as contacts_router
from app.routes.auth import router as auth_router
from app.routes.existing_hotels import router as existing_hotels_router

from app.routes.sap import router as sap_router, legacy_router as sap_legacy_router

from app.routes.revenue import router as revenue_router


# NOTE: Do NOT set WindowsProactorEventLoopPolicy here.
# Uvicorn overrides the policy and creates its own SelectorEventLoop anyway.
# Playwright/Crawl4AI need ProactorEventLoop for subprocesses — they work
# in Celery workers and CLI (run_pipeline.py) but not under uvicorn.
# Dashboard scraping uses httpx with gold URL fallback.


logger = logging.getLogger(__name__)

# SSE streaming paths — must bypass ALL BaseHTTPMiddleware wrapping
_SSE_PATHS = frozenset(
    {
        "/api/dashboard/scrape/stream",
        "/api/dashboard/extract-url/stream",
        "/api/dashboard/discovery/stream",
    }
)


# -----------------------------------------------------------------------------
# Application Lifecycle
# -----------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    logger.info("=" * 50)
    logger.info("Starting Smart Lead Hunter...")
    logger.info("=" * 50)

    await init_db()
    logger.info("Database tables verified")
    logger.info("Smart Lead Hunter is ready!")
    logger.info("API Docs: http://localhost:8000/docs")

    yield

    logger.info("Shutting down Smart Lead Hunter...")

    try:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        await crm.close()
        logger.info("Insightly client closed")
    except Exception:
        pass

    try:
        from app.services.contact_enrichment import _shared_client

        if _shared_client and not _shared_client.is_closed:
            await _shared_client.aclose()
            logger.info("Enrichment HTTP client closed")
    except Exception:
        pass

    try:
        import app.shared as _shared

        if _shared._stats_redis:
            await _shared._stats_redis.aclose()
        if _shared._health_redis:
            await _shared._health_redis.aclose()
        logger.info("Redis connections closed")
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Pure ASGI middlewares — safe for SSE / StreamingResponse
# NOTE: BaseHTTPMiddleware buffers responses which cancels long-running streams.
#       All middlewares here use raw ASGI __call__ instead.
# -----------------------------------------------------------------------------


class RequestIDMiddleware:
    """Attach X-Request-ID to every non-SSE response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        from starlette.requests import Request as _Req

        path = _Req(scope).url.path
        if path in _SSE_PATHS:
            await self.app(scope, receive, send)
            return

        headers_dict = dict(scope.get("headers", []))
        request_id = (
            headers_dict.get(b"x-request-id", b"").decode() or str(uuid.uuid4())[:8]
        )

        async def send_with_header(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_header)


_rate_limit_store: dict = defaultdict(lambda: {"count": 0, "reset": 0.0})
_RATE_LIMIT_MAX = 200
_RATE_LIMIT_WINDOW = 60.0
_RATE_LIMIT_MAX_ENTRIES = 10000
_rate_limit_last_cleanup = 0.0
_RATE_LIMITED_PREFIXES = ("/api/", "/leads", "/sources", "/scrape")


def _cleanup_rate_limit_store(now: float) -> None:
    global _rate_limit_last_cleanup
    if now - _rate_limit_last_cleanup < _RATE_LIMIT_WINDOW:
        return
    _rate_limit_last_cleanup = now
    expired = [
        ip
        for ip, bucket in _rate_limit_store.items()
        if now > bucket["reset"] + _RATE_LIMIT_WINDOW
    ]
    for ip in expired:
        del _rate_limit_store[ip]
    if len(_rate_limit_store) > _RATE_LIMIT_MAX_ENTRIES:
        sorted_ips = sorted(
            _rate_limit_store.keys(), key=lambda ip: _rate_limit_store[ip]["reset"]
        )
        for ip in sorted_ips[: len(_rate_limit_store) - _RATE_LIMIT_MAX_ENTRIES // 2]:
            del _rate_limit_store[ip]


class RateLimitMiddleware:
    """Per-IP rate limiter. SSE paths are fully exempt."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        from starlette.requests import Request as _Req

        request = _Req(scope, receive)
        path = request.url.path

        if path in _SSE_PATHS or not path.startswith(_RATE_LIMITED_PREFIXES):
            await self.app(scope, receive, send)
            return

        forwarded = request.headers.get("X-Forwarded-For", "")
        client_ip = (
            forwarded.split(",")[0].strip()
            if forwarded
            else (request.client.host if request.client else "unknown")
        )
        now = time.monotonic()
        _cleanup_rate_limit_store(now)

        bucket = _rate_limit_store[client_ip]
        if now > bucket["reset"]:
            bucket["count"] = 0
            bucket["reset"] = now + _RATE_LIMIT_WINDOW
        bucket["count"] += 1

        if bucket["count"] > _RATE_LIMIT_MAX:
            resp = JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Please slow down."},
                headers={"Retry-After": str(int(bucket["reset"] - now))},
            )
            await resp(scope, receive, send)
            return

        await self.app(scope, receive, send)


# -----------------------------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------------------------
setup_logging()

app = FastAPI(
    title="Smart Lead Hunter",
    description="Automated hotel lead generation system for J.A. Uniforms",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-API-Key"],
)

# Pure ASGI middlewares (added in reverse — last added = outermost wrapper)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(APIKeyMiddleware)


# -----------------------------------------------------------------------------
# Include Route Modules
# -----------------------------------------------------------------------------
app.include_router(auth_router)
app.include_router(health_router)
app.include_router(leads_router)
app.include_router(sources_router)
app.include_router(dashboard_router)
app.include_router(scraping_router)
app.include_router(contacts_router)
app.include_router(existing_hotels_router)
app.include_router(sap_router)
app.include_router(sap_legacy_router)
app.include_router(revenue_router)


# -----------------------------------------------------------------------------
# Production Frontend
# -----------------------------------------------------------------------------
_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _FRONTEND_DIR.is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_FRONTEND_DIR / "assets")),
        name="frontend_assets",
    )

    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/vite.svg", include_in_schema=False)
    async def frontend_static_files(request: Request):
        file_path = _FRONTEND_DIR / request.url.path.lstrip("/")
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_FRONTEND_DIR / "index.html"))

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        file_path = _FRONTEND_DIR / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_FRONTEND_DIR / "index.html"))

    logger.info(f"Frontend: serving production build from {_FRONTEND_DIR}")
else:
    logger.info("Frontend: no dist/ found — run 'npm run build' for production mode")
