from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from app.database import Base


class FailedDomain(Base):
    __tablename__ = "failed_domains"

    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(500), unique=True, nullable=False, index=True)
    reason = Column(Text, nullable=True)
    failed_at = Column(DateTime(timezone=True), server_default=func.now())
