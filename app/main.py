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

logger = logging.getLogger(__name__)


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

    # FIX H-06: Gracefully close shared HTTP clients and Redis connections
    logger.info("Shutting down Smart Lead Hunter...")

    # Close Insightly shared httpx client
    try:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        await crm.close()
        logger.info("Insightly client closed")
    except Exception:
        pass

    # Close enrichment shared httpx client
    try:
        from app.services.contact_enrichment import _shared_client

        if _shared_client and not _shared_client.is_closed:
            await _shared_client.aclose()
            logger.info("Enrichment HTTP client closed")
    except Exception:
        pass

    # Close Redis connections
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
    allow_origins=(
        [
            "https://leads.jauniforms.com",
        ]
        if getattr(settings, "environment", "development") == "production"
        else [
            "http://localhost:8000",
            "http://localhost:3000",
            "http://127.0.0.1:8000",
            "http://192.168.30.59:8000",
            "http://192.168.30.59:3000",
        ]
    ),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-API-Key"],
)

# Auth
app.add_middleware(APIKeyMiddleware)


# ── L-06: Request ID middleware — adds X-Request-ID to every response ──
# Enables tracing a single user action through scraping → extraction → CRM push.


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach a unique request ID to each request for log tracing."""
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]
    # Store on request state so route handlers can access it
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# Rate Limiter
# ── H-03 NOTE: This is per-process memory. If you ever run uvicorn with
# --workers 2+, each worker has its own store, effectively doubling the limit.
# For single-worker (current setup) this is fine. For multi-worker, move to
# Redis INCR/EXPIRE. Same applies to auth rate limiter in routes/auth.py.
_rate_limit_store: dict = defaultdict(lambda: {"count": 0, "reset": 0.0})
_RATE_LIMIT_MAX = 60
_RATE_LIMIT_WINDOW = 60.0
_RATE_LIMIT_MAX_ENTRIES = 10000
_rate_limit_last_cleanup = 0.0


def _cleanup_rate_limit_store(now: float) -> None:
    """Evict expired rate limit entries."""
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


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Rate limit API requests per client IP."""
    path = request.url.path

    if not any(path.startswith(p) for p in ["/leads", "/sources", "/scrape", "/api"]):
        return await call_next(request)

    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    else:
        client_ip = request.client.host if request.client else "unknown"
    now = time.monotonic()

    _cleanup_rate_limit_store(now)

    bucket = _rate_limit_store[client_ip]

    if now > bucket["reset"]:
        bucket["count"] = 0
        bucket["reset"] = now + _RATE_LIMIT_WINDOW

    bucket["count"] += 1

    if bucket["count"] > _RATE_LIMIT_MAX:
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."},
            headers={"Retry-After": str(int(bucket["reset"] - now))},
        )

    response = await call_next(request)
    return response


# -----------------------------------------------------------------------------
# Include Route Modules
# -----------------------------------------------------------------------------
app.include_router(auth_router)  # /auth/* — must be before other routes
app.include_router(health_router)
app.include_router(leads_router)
app.include_router(sources_router)
app.include_router(dashboard_router)
app.include_router(scraping_router)
app.include_router(contacts_router)


# -----------------------------------------------------------------------------
# Production Frontend — serve React build from FastAPI (single port)
# -----------------------------------------------------------------------------
# After `npm run build`, the dist/ folder is served here.
# All API routes above take priority. Unknown paths get index.html (SPA routing).
# In dev mode (Vite on :3000), this section is harmlessly skipped.
# -----------------------------------------------------------------------------

_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _FRONTEND_DIR.is_dir():
    # Serve static assets (JS, CSS, images) from dist/assets/
    app.mount(
        "/assets",
        StaticFiles(directory=str(_FRONTEND_DIR / "assets")),
        name="frontend_assets",
    )

    # Serve other static files at root (favicon, manifest, etc.)
    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/vite.svg", include_in_schema=False)
    async def frontend_static_files(request: Request):
        file_path = _FRONTEND_DIR / request.url.path.lstrip("/")
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_FRONTEND_DIR / "index.html"))

    # SPA catch-all: any non-API path serves index.html so React Router works
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        # Don't serve index.html for API/auth/health paths (those already handled above)
        file_path = _FRONTEND_DIR / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_FRONTEND_DIR / "index.html"))

    logger.info(f"Frontend: serving production build from {_FRONTEND_DIR}")
else:
    logger.info("Frontend: no dist/ found — run 'npm run build' for production mode")
