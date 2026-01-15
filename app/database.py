"""
Database connection and session management
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from collections.abc import AsyncGenerator
from app.config import settings

# Convert postgres:// to postgresql+asyncpg:// for async support
database_url = settings.database_url.replace(
    "postgresql://", "postgresql+asyncpg://"
)

# Create async engine
engine = create_async_engine(
    database_url,
    echo=settings.debug,  # Log SQL queries in debug mode
    pool_size=5,
    max_overflow=10
)

# Create async session factory
async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

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