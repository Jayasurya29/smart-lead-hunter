"""
SMART LEAD HUNTER — LeadContact Model
======================================
Stores contacts linked to leads. Supports:
- Multiple contacts per lead (not just one in the notes field)
- Save/pin contacts to persist across re-enrichment
- Track enrichment history (which cycle found/updated each contact)
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.database import Base
from datetime import datetime, timezone


class LeadContact(Base):
    __tablename__ = "lead_contacts"

    # M-08: Composite index for duplicate checks during enrichment
    __table_args__ = (Index("ix_lead_contacts_lead_name", "lead_id", "name"),)

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

    # Strategist priority (from Iter 6 reasoning pass) — overrides algorithmic priority
    # when present. Values: "P1", "P2", "P3", "P4", or NULL when not yet reasoned.
    strategist_priority = Column(String(4))
    strategist_reasoning = Column(Text)

    # Persistence
    is_saved = Column(
        Boolean, default=False, index=True
    )  # Pinned by user — survives re-enrichment
    is_primary = Column(Boolean, default=False)  # Best contact for this lead

    # Source tracking
    found_via = Column(String(100))  # "linkedin_search", "press_release", "manual"
    source_detail = Column(
        Text
    )  # e.g., "LinkedIn profile mentions Six Senses Napa Valley"
    evidence_url = Column(Text)  # URL where this contact was found (proof of relevance)

    # Timestamps — use UTC for storage consistency; convert at presentation layer
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    last_enriched_at = Column(
        DateTime(timezone=True)
    )  # When enrichment last touched this contact

    # Relationship
    lead = relationship("PotentialLead", backref="contacts")

    def _compute_priority(self) -> tuple[str, str]:
        """
        Compute (priority_label, priority_reason) for sales prioritization.

        If the strategist (Iter 6 reasoning pass) has set a priority, USE IT.
        It has full context — timeline, project stage, company verification,
        role verification — that the simple algorithm doesn't see.

        Otherwise fall back to the algorithmic rules below:
          P1 — CALL FIRST       on-property + uniform/purchasing/GM
          P2 — STRONG FIT       regional decision-maker OR property F&B/HR
          P3 — USEFUL           corporate procurement OR area F&B/HR
          P4 — ESCALATION ONLY  C-suite or low-relevance roles
        """
        # Strategist verdict wins when present
        sp = (self.strategist_priority or "").upper().strip()
        if sp in ("P1", "P2", "P3", "P4"):
            return (sp, self.strategist_reasoning or "Strategist priority")

        scope = (self.scope or "unknown").lower()
        tier = (self.tier or "UNKNOWN").upper()

        # Tier groupings
        is_uniform_buyer = tier in ("TIER1_UNIFORM_DIRECT", "TIER2_PURCHASING")
        is_gm_ops = tier == "TIER3_GM_OPS"
        is_fb_hr = tier in ("TIER4_FB", "TIER5_HR")
        is_low_value = tier in ("TIER6_FINANCE", "TIER7_IRRELEVANT", "UNKNOWN")

        # ── P1: On-property buyer with right role ──
        if scope == "hotel_specific" and (is_uniform_buyer or is_gm_ops):
            if is_uniform_buyer:
                return ("P1", "On-property uniform/purchasing — call first")
            return ("P1", "On-property GM/Operations — call first")

        # ── P2: Regional decision-maker OR property F&B/HR ──
        if scope == "chain_area" and (is_uniform_buyer or is_gm_ops):
            return ("P2", "Regional decision-maker — strong fit")
        if scope == "hotel_specific" and is_fb_hr:
            return ("P2", "On-property F&B/HR — strong fit")

        # ── P3: Corporate procurement / area F&B/HR / hotel unknown ──
        if scope == "chain_corporate" and (is_uniform_buyer or is_gm_ops):
            return ("P3", "Corporate procurement — useful for owner relations")
        if scope == "chain_area" and is_fb_hr:
            return ("P3", "Regional F&B/HR — useful")
        if scope == "hotel_specific" and is_low_value:
            return ("P3", "On-property but low-relevance role")

        # ── P4: Escalation only / low fit ──
        if scope == "chain_corporate":
            return ("P4", "Corporate executive — escalation only")
        if is_low_value:
            return ("P4", "Low-relevance role")

        return ("P4", "Insufficient signal — review manually")

    def to_dict(self):
        priority_label, priority_reason = self._compute_priority()
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
            # Priority for sales team UI (computed from tier + scope,
            # or overridden by strategist verdict when available)
            "priority_label": priority_label,
            "priority_reason": priority_reason,
            "strategist_priority": self.strategist_priority,
            "strategist_reasoning": self.strategist_reasoning,
            "has_strategist_verdict": bool(self.strategist_priority),
        }

    def __repr__(self):
        pin = " [SAVED]" if self.is_saved else ""
        return f"<LeadContact {self.name} - {self.title or 'No title'}{pin}>"
