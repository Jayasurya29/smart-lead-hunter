"""
Smart Lead Hunter - Test Configuration (SDLC-Compliant)
======================================================
- Patches app engine with NullPool to avoid event loop conflicts
- Unique test data names (UUID) prevent duplicate key errors
- Auto-cleanup after each test
Run: pytest tests/ -v
"""

import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

import pytest_asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import delete, text

sys.path.insert(0, str(Path(__file__).parent.parent))


# =====================================================================
# PATCH APP ENGINE (NullPool = no connection reuse = no event loop clash)
# =====================================================================


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _patch_engine():
    """Replace app's connection pool with NullPool for test compatibility."""
    from app.config import settings
    from app import database

    db_url = settings.database_url.replace("postgresql://", "postgresql+asyncpg://")
    database.engine = create_async_engine(db_url, poolclass=NullPool, echo=False)
    database.async_session = async_sessionmaker(
        database.engine, class_=AsyncSession, expire_on_commit=False
    )
    yield
    await database.engine.dispose()


# =====================================================================
# CLEANUP stale __TEST__ data from previous failed runs
# =====================================================================


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _cleanup_stale_data(_patch_engine):
    """Create tables if needed, then remove leftover __TEST__ data."""
    from app.database import engine, async_session, Base
    from app.models.potential_lead import PotentialLead
    from app.models.source import Source

    # Ensure pgvector extension + tables exist (needed in CI)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)

    # Clean stale test data
    try:
        async with async_session() as session:
            await session.execute(
                delete(PotentialLead).where(PotentialLead.hotel_name.like("__TEST__%"))
            )
            await session.execute(delete(Source).where(Source.name.like("__TEST__%")))
            await session.commit()
    except Exception:
        pass  # Tables may be empty
    yield


# =====================================================================
# HTTP CLIENT
# =====================================================================


@pytest_asyncio.fixture
async def client():
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as ac:
        yield ac


# =====================================================================
# DB SESSION (separate engine, no conflicts)
# =====================================================================


@pytest_asyncio.fixture
async def db_session():
    from app.config import settings

    db_url = settings.database_url.replace("postgresql://", "postgresql+asyncpg://")
    test_engine = create_async_engine(db_url, poolclass=NullPool)
    factory = async_sessionmaker(
        test_engine, class_=AsyncSession, expire_on_commit=False
    )

    session = factory()
    try:
        yield session
    finally:
        await session.close()
        await test_engine.dispose()


# =====================================================================
# TEST DATA FACTORIES (unique names, auto-cleanup)
# =====================================================================


@pytest_asyncio.fixture
async def sample_source(db_session):
    from app.models.source import Source

    uid = uuid.uuid4().hex[:8]
    source = Source(
        name=f"__TEST_{uid}__ Hospitality News",
        base_url=f"https://test-{uid}.example.com",
        source_type="news",
        priority=5,
        is_active=True,
        health_status="healthy",
        leads_found=0,
        consecutive_failures=0,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(source)
    await db_session.commit()
    await db_session.refresh(source)

    yield source

    try:
        await db_session.delete(source)
        await db_session.commit()
    except Exception:
        await db_session.rollback()


@pytest_asyncio.fixture
async def sample_lead(db_session, sample_source):
    from app.models.potential_lead import PotentialLead

    uid = uuid.uuid4().hex[:8]
    lead = PotentialLead(
        hotel_name=f"__TEST_{uid}__ Four Seasons Miami Beach",
        brand="Four Seasons",
        brand_tier="2",
        city="Miami Beach",
        state="Florida",
        country="USA",
        opening_date="Q3 2027",
        opening_year=2027,
        room_count=200,
        lead_score=78,
        status="new",
        source_id=sample_source.id,
        source_url=f"https://test.example.com/article/{uid}",
        source_site="test.example.com",
        score_breakdown={
            "brand": 20,
            "timing": 18,
            "location": 20,
            "size": 10,
            "extras": 10,
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(lead)
    await db_session.commit()
    await db_session.refresh(lead)

    yield lead

    try:
        await db_session.refresh(lead)
        await db_session.delete(lead)
        await db_session.commit()
    except Exception:
        await db_session.rollback()


@pytest_asyncio.fixture
async def sample_leads_batch(db_session, sample_source):
    from app.models.potential_lead import PotentialLead

    uid = uuid.uuid4().hex[:8]
    leads = []
    test_data = [
        {
            "hotel_name": f"__TEST_{uid}__ W Miami Beach",
            "brand": "W Hotels",
            "brand_tier": "2",
            "city": "Miami Beach",
            "state": "Florida",
            "country": "USA",
            "lead_score": 82,
            "status": "new",
            "opening_year": 2027,
        },
        {
            "hotel_name": f"__TEST_{uid}__ Hyatt San Juan",
            "brand": "Hyatt",
            "brand_tier": "3",
            "city": "San Juan",
            "state": "PR",
            "country": "USA",
            "lead_score": 70,
            "status": "new",
            "opening_year": 2027,
        },
        {
            "hotel_name": f"__TEST_{uid}__ Ritz Nassau",
            "brand": "Ritz-Carlton",
            "brand_tier": "1",
            "city": "Nassau",
            "state": "New Providence",
            "country": "Bahamas",
            "lead_score": 90,
            "status": "approved",
            "opening_year": 2026,
        },
        {
            "hotel_name": f"__TEST_{uid}__ Marriott Orlando",
            "brand": "Marriott",
            "brand_tier": "3",
            "city": "Orlando",
            "state": "Florida",
            "country": "USA",
            "lead_score": 55,
            "status": "rejected",
            "opening_year": 2027,
        },
        {
            "hotel_name": f"__TEST_{uid}__ Motel 6 Skip",
            "brand": "Motel 6",
            "brand_tier": "5",
            "city": "Nowhere",
            "state": "TX",
            "country": "USA",
            "lead_score": 12,
            "status": "rejected",
            "opening_year": 2027,
        },
    ]

    for d in test_data:
        lead = PotentialLead(
            hotel_name=d["hotel_name"],
            brand=d.get("brand"),
            brand_tier=d.get("brand_tier"),
            city=d["city"],
            state=d["state"],
            country=d["country"],
            lead_score=d["lead_score"],
            status=d["status"],
            opening_year=d.get("opening_year"),
            source_id=sample_source.id,
            source_url=f"https://test.example.com/{d['hotel_name'][:20]}",
            source_site="test.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(lead)
        leads.append(lead)

    await db_session.commit()
    for lead in leads:
        await db_session.refresh(lead)

    yield leads

    try:
        for lead in leads:
            await db_session.refresh(lead)
            await db_session.delete(lead)
        await db_session.commit()
    except Exception:
        await db_session.rollback()
