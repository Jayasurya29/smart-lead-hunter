from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from datetime import timedelta
from app.database import Base
from app.services.utils import local_now


class FailedDomain(Base):
    __tablename__ = "failed_domains"

    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(500), unique=True, nullable=False, index=True)
    reason = Column(Text, nullable=True)
    fail_count = Column(Integer, default=1, nullable=False)
    first_failed = Column(DateTime(timezone=True), server_default=func.now())
    last_failed = Column(DateTime(timezone=True), server_default=func.now())
    retry_after = Column(DateTime(timezone=True), nullable=True)
    failed_at = Column(
        DateTime(timezone=True), server_default=func.now()
    )  # kept for backward compat

    def should_skip(self) -> bool:
        """Check if this domain should be skipped (still in cooldown)."""
        if self.retry_after is None:
            return True
        now = local_now()
        return now < self.retry_after

    def record_failure(self, reason: str = None):
        """Record another failure for this domain."""
        now = local_now()
        self.fail_count = (self.fail_count or 0) + 1
        self.last_failed = now
        self.failed_at = now
        if reason:
            self.reason = reason
        # Exponential backoff: 7, 14, 28, 56 days (max 56)
        days = min(7 * (2 ** (self.fail_count - 1)), 56)
        self.retry_after = now + timedelta(days=days)
