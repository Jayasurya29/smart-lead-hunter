"""
Database connection and session management
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from collections.abc import AsyncGenerator
from app.config import settings

# Convert postgres:// to postgresql+asyncpg:// for async support
database_url = settings.database_url.replace("postgresql://", "postgresql+asyncpg://")

# Create async engine
# M6: echo=False always (never log SQL in production — leaks data)
# PM1: pool_size 5→10 for concurrent scraping
# R2: max_overflow 10→20 to handle traffic spikes
engine = create_async_engine(
    database_url,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True,  # FIX: Detect dead connections before checkout
)

# Create async session factory
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Base class for models
Base = declarative_base()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Get database session for dependency injection"""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize database tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
