"""
Smart Lead Hunter — Existing Hotels Model
==========================================
Stores existing/operating hotels for prospecting map.
Hotels marked is_client=True are current JA Uniforms customers (from SAP).
The rest are prospects for the sales team.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    Text,
)
from datetime import datetime, timezone

from app.database import Base


class ExistingHotel(Base):
    """Existing/operating hotels — for prospecting map."""

    __tablename__ = "existing_hotels"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Hotel identity
    name = Column(String(300), nullable=False, index=True)
    brand = Column(String(150))
    chain = Column(String(150))  # Parent company (Marriott International, Hilton, etc.)
    brand_tier = Column(String(50))  # tier1_ultra_luxury, tier2_luxury, etc.

    # Location
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(100), index=True)
    country = Column(String(100), default="US")
    zip_code = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)

    # Property details
    room_count = Column(Integer)
    phone = Column(String(50))
    website = Column(String(500))
    property_type = Column(String(50))  # hotel, resort, boutique, etc.

    # Contact info
    gm_name = Column(String(200))
    gm_title = Column(String(200))
    gm_email = Column(String(255))
    gm_phone = Column(String(50))
    gm_linkedin = Column(String(500))

    # Client status
    is_client = Column(Boolean, default=False, nullable=False, index=True)
    sap_bp_code = Column(String(20))  # SAP Business Partner code
    client_notes = Column(Text)

    # Data tracking
    data_source = Column(
        String(50)
    )  # sap_import, google_places, chain_directory, manual
    source_url = Column(String(500))
    last_verified_at = Column(DateTime(timezone=True))
    status = Column(String(20), default="active")  # active, closed, unknown

    # Atlist integration
    atlist_marker_id = Column(String(100))
    pushed_to_map = Column(Boolean, default=False)
    pushed_at = Column(DateTime(timezone=True))

    # Timestamps
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        client_tag = " [CLIENT]" if self.is_client else ""
        return f"<ExistingHotel(id={self.id}, name='{self.name}', city='{self.city}'{client_tag})>"

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "brand": self.brand,
            "chain": self.chain,
            "brand_tier": self.brand_tier,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "zip_code": self.zip_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "room_count": self.room_count,
            "phone": self.phone,
            "website": self.website,
            "property_type": self.property_type,
            "gm_name": self.gm_name,
            "gm_title": self.gm_title,
            "gm_email": self.gm_email,
            "gm_phone": self.gm_phone,
            "gm_linkedin": self.gm_linkedin,
            "is_client": self.is_client,
            "sap_bp_code": self.sap_bp_code,
            "client_notes": self.client_notes,
            "data_source": self.data_source,
            "source_url": self.source_url,
            "status": self.status,
            "pushed_to_map": self.pushed_to_map,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
