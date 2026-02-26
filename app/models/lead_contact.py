"""
SMART LEAD HUNTER — LeadContact Model
======================================
Stores contacts linked to leads. Supports:
- Multiple contacts per lead (not just one in the notes field)
- Save/pin contacts to persist across re-enrichment
- Track enrichment history (which cycle found/updated each contact)
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.database import Base


class LeadContact(Base):
    __tablename__ = "lead_contacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_id = Column(
        Integer,
        ForeignKey("potential_leads.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Contact info
    name = Column(String(200), nullable=False)
    title = Column(String(200))
    email = Column(String(255))
    phone = Column(String(50))
    linkedin = Column(String(500))
    organization = Column(String(300))

    # Classification
    scope = Column(
        String(50), default="unknown"
    )  # hotel_specific, chain_area, chain_corporate
    confidence = Column(String(20), default="medium")  # high, medium, low
    tier = Column(String(50))  # TIER1_UNIFORM_DIRECT, TIER2_PURCHASING, etc.
    score = Column(Integer, default=0)

    # Persistence
    is_saved = Column(
        Boolean, default=False, index=True
    )  # Pinned by user — survives re-enrichment
    is_primary = Column(Boolean, default=False)  # Best contact for this lead

    # Source tracking
    found_via = Column(
        String(100)
    )  # "linkedin_search", "apollo", "press_release", "manual"
    source_detail = Column(
        Text
    )  # e.g., "LinkedIn profile mentions Six Senses Napa Valley"
    evidence_url = Column(Text)  # URL where this contact was found (proof of relevance)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_enriched_at = Column(DateTime)  # When enrichment last touched this contact

    # Relationship
    lead = relationship("PotentialLead", backref="contacts")

    def to_dict(self):
        return {
            "id": self.id,
            "lead_id": self.lead_id,
            "name": self.name,
            "title": self.title,
            "email": self.email,
            "phone": self.phone,
            "linkedin": self.linkedin,
            "organization": self.organization,
            "scope": self.scope,
            "confidence": self.confidence,
            "tier": self.tier,
            "score": self.score,
            "is_saved": self.is_saved,
            "is_primary": self.is_primary,
            "found_via": self.found_via,
            "source_detail": self.source_detail,
            "evidence_url": self.evidence_url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        pin = " [SAVED]" if self.is_saved else ""
        return f"<LeadContact {self.name} - {self.title or 'No title'}{pin}>"
