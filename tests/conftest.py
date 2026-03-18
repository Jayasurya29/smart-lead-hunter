"""
Smart Lead Hunter — Test Configuration
========================================
Two fixture layers:
  1. MOCK layer (no DB needed) — httpx client with dependency overrides
  2. DB layer (requires PostgreSQL) — real session fixtures

Run unit tests only:   pytest tests/ -v -k "not db_"
Run everything:        pytest tests/ -v
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, str(Path(__file__).parent.parent))

# Ensure dev JWT secret is set for tests
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-testing-only-32chars!")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_AUTH_KEY", "test-api-key-12345")


# =====================================================================
# MOCK DB SESSION — no real database needed
# =====================================================================

def _make_mock_session():
    """Create a mock AsyncSession with common patterns."""
    session = AsyncMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.close = AsyncMock()
    session.rollback = AsyncMock()
    session.delete = AsyncMock()
    return session


@pytest.fixture
def mock_db():
    """Mock database session for unit tests."""
    return _make_mock_session()


# =====================================================================
# HTTP CLIENTS — use the REAL app.main (not main_old)
# =====================================================================

@pytest_asyncio.fixture
async def client():
    """Unauthenticated HTTP client against the real FastAPI app."""
    from app.main import app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def authed_client():
    """HTTP client with a valid API key header for protected routes."""
    from app.main import app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"X-API-Key": os.environ["API_AUTH_KEY"]},
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def jwt_client():
    """HTTP client with a valid JWT cookie (simulates logged-in browser)."""
    from app.main import app
    from app.routes.auth import create_token, COOKIE_NAME

    token = create_token(user_id=1, email="test@jauniforms.com", role="admin")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        cookies={COOKIE_NAME: token},
    ) as ac:
        yield ac


# =====================================================================
# AUTH HELPERS
# =====================================================================

@pytest.fixture
def valid_register_data():
    """Valid registration payload."""
    uid = uuid.uuid4().hex[:6]
    return {
        "first_name": "Test",
        "last_name": "User",
        "email": f"test.{uid}@jauniforms.com",
        "password": "StrongPass1!",
        "role": "sales",
    }


@pytest.fixture
def weak_passwords():
    """Collection of passwords that should fail validation."""
    return [
        ("short1A", "at least 8"),
        ("alllowercase1", "uppercase"),
        ("ALLUPPERCASE1", "lowercase"),
        ("NoNumbersHere", "number"),
    ]


# =====================================================================
# SAMPLE DATA FACTORIES (in-memory, no DB)
# =====================================================================

@pytest.fixture
def sample_lead_dict():
    """Minimal lead dict for scorer/factory tests."""
    return {
        "hotel_name": "Rosewood Miami Beach",
        "brand": "Rosewood Hotels",
        "city": "Miami Beach",
        "state": "Florida",
        "country": "USA",
        "opening_date": "Q3 2027",
        "room_count": 200,
        "source_url": "https://test.example.com/article",
        "source_site": "test.example.com",
    }


@pytest.fixture
def sample_contact_dict():
    """Sample enriched contact."""
    return {
        "name": "Jane Smith",
        "title": "Director of Housekeeping",
        "email": "jane@rosewood.com",
        "phone": "+1-305-555-0100",
        "linkedin": "https://linkedin.com/in/janesmith",
        "organization": "Rosewood Miami Beach",
        "scope": "hotel_specific",
        "confidence": "high",
    }


# =====================================================================
# DB-BACKED FIXTURES (require real PostgreSQL — skip in CI)
# =====================================================================

@pytest_asyncio.fixture(scope="session", autouse=True)
async def _patch_engine():
    """Replace app's connection pool with NullPool for test compatibility.
    Silently skips if DB is unreachable."""
    try:
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        from sqlalchemy.pool import NullPool
        from sqlalchemy import text
        from app.config import settings
        from app import database

        db_url = settings.database_url.replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        test_engine = create_async_engine(db_url, poolclass=NullPool, echo=False)
        # Quick connectivity check
        async with test_engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        database.engine = test_engine
        database.async_session = async_sessionmaker(
            test_engine, class_=AsyncSession, expire_on_commit=False
        )
        yield
        await test_engine.dispose()
    except Exception:
        # DB not available — unit tests still work
        yield


@pytest_asyncio.fixture
async def db_session():
    """Real DB session — tests using this are skipped if DB is down."""
    try:
        from app.database import async_session
        session = async_session()
        yield session
        await session.close()
    except Exception:
        pytest.skip("Database not available")
