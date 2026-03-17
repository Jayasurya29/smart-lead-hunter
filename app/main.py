"""
Smart Lead Hunter - Main Application
------------------------------------
FastAPI entry point. Route handlers live in app/routes/.

Run with:
    uvicorn app.main:app --reload --port 8000
"""

import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

    logger.info("Shutting down Smart Lead Hunter...")


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
        ]
    ),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-API-Key"],
)

# Auth
app.add_middleware(APIKeyMiddleware)


# Rate Limiter
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
app.include_router(health_router)
app.include_router(leads_router)
app.include_router(sources_router)
app.include_router(dashboard_router)
app.include_router(scraping_router)
app.include_router(contacts_router)
