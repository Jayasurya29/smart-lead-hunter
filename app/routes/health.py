"""Health, auth verification, and stats endpoints."""

import logging

from fastapi import APIRouter, Depends, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.schemas import StatsResponse
from app.services.utils import local_now
from app.shared import safe_error, get_dashboard_stats

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", tags=["Health"])
async def root():
    """Root endpoint — serves React app if built, otherwise API info."""
    from pathlib import Path
    from fastapi.responses import FileResponse

    index = (
        Path(__file__).resolve().parent.parent.parent
        / "frontend"
        / "dist"
        / "index.html"
    )
    if index.is_file():
        return FileResponse(str(index))
    return {
        "name": "Smart Lead Hunter",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }


@router.get("/api/auth/verify")
async def verify_auth(request: Request):
    """Verifies API key is valid — used by frontend login."""
    return {"status": "ok"}


@router.get("/health", tags=["Health"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """Health check endpoint — verifies DB, Gemini API, and Redis."""
    import app.shared as _shared

    components = {}

    # 1. Database
    try:
        await db.execute(text("SELECT 1"))
        components["database"] = "healthy"
    except Exception as e:
        components["database"] = f"unhealthy: {safe_error(e)}"

    # 2. Gemini API
    try:
        import httpx

        gemini_key = (
            settings.gemini_api_key if hasattr(settings, "gemini_api_key") else None
        )
        if not gemini_key:
            components["gemini"] = "not configured"
        else:
            gemini_model = getattr(settings, "gemini_model", "gemini-2.5-flash")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}?key={gemini_key}"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                components["gemini"] = "healthy"
            else:
                components["gemini"] = f"error: HTTP {resp.status_code}"
    except Exception as e:
        components["gemini"] = f"unhealthy: {safe_error(e)}"

    # 3. Redis
    try:
        redis_url = getattr(settings, "redis_url", None)
        if not redis_url:
            components["redis"] = "not configured"
        else:
            import redis.asyncio as aioredis

            if _shared._health_redis is None:
                _shared._health_redis = aioredis.from_url(
                    redis_url, socket_connect_timeout=3
                )
            try:
                await _shared._health_redis.ping()
                components["redis"] = "healthy"
            except Exception:
                try:
                    await _shared._health_redis.aclose()
                except Exception:
                    pass
                _shared._health_redis = aioredis.from_url(
                    redis_url, socket_connect_timeout=3
                )
                await _shared._health_redis.ping()
                components["redis"] = "healthy"
    except Exception as e:
        components["redis"] = f"unhealthy: {safe_error(e)}"

    # 4. Insightly CRM (FIX L-11: was missing from health check)
    try:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if not crm.enabled:
            components["insightly"] = "not configured"
        else:
            crm_result = await crm.test_connection()
            if crm_result.get("connected"):
                components["insightly"] = "healthy"
            else:
                components["insightly"] = f"error: {crm_result.get('error', 'unknown')}"
    except Exception as e:
        components["insightly"] = f"unhealthy: {safe_error(e)}"

    # Overall status
    healthy_count = sum(1 for v in components.values() if v == "healthy")
    if healthy_count == len(components):
        overall = "healthy"
    elif components.get("database") == "healthy":
        overall = "degraded"
    else:
        overall = "unhealthy"

    return {
        "status": overall,
        "timestamp": local_now().isoformat(),
        "components": components,
    }


@router.get("/stats", response_model=StatsResponse, tags=["Dashboard"])
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Get dashboard statistics"""
    stats = await get_dashboard_stats(db)
    return StatsResponse(**stats)
