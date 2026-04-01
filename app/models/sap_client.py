"""
SMART LEAD HUNTER — SAP Client Model
=====================================
Stores client data imported from SAP Business One CSV exports.
Powers the Client Intelligence and Market Expansion modules.
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB

from app.database import Base
from app.services.utils import local_now


class SAPClient(Base):
    """Imported SAP Business One client record."""

    __tablename__ = "sap_clients"

    __table_args__ = (
        Index("ix_sap_clients_customer_code", "customer_code", unique=True),
        Index("ix_sap_clients_customer_group", "customer_group"),
        Index("ix_sap_clients_state", "state"),
        Index("ix_sap_clients_city_state", "city", "state"),
        Index("ix_sap_clients_revenue_lifetime", "revenue_lifetime"),
        Index("ix_sap_clients_days_since_last_order", "days_since_last_order"),
        Index("ix_sap_clients_sales_rep", "sales_rep"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    # SAP identifiers
    customer_code = Column(String(50), nullable=False, unique=True)
    customer_name = Column(String(300), nullable=False)
    customer_name_normalized = Column(String(300))

    # Classification
    customer_group = Column(String(100))
    customer_type = Column(String(50), default="unknown")
    is_hotel = Column(Boolean, default=False, index=True)

    # Contact info
    phone = Column(String(100))
    email = Column(String(255))
    contact_person = Column(String(200))

    # Address
    street = Column(String(300))
    city = Column(String(100))
    state = Column(String(10))
    zip_code = Column(String(20))
    country = Column(String(10), default="US")

    # Revenue
    revenue_current_year = Column(Float, default=0.0)
    revenue_last_year = Column(Float, default=0.0)
    revenue_lifetime = Column(Float, default=0.0)
    total_invoices = Column(Integer, default=0)

    # Dates
    customer_since = Column(String(20))
    last_order_date = Column(String(20))
    days_since_last_order = Column(Integer)

    # Sales
    sales_rep = Column(String(100))

    # Enrichment (filled later by SLH)
    brand = Column(String(100))
    brand_tier = Column(String(50))
    room_count = Column(Integer)
    hotel_website = Column(String(500))

    # Geo (for map)
    latitude = Column(Float)
    longitude = Column(Float)

    # Cross-reference
    matched_lead_id = Column(Integer)

    # Import tracking
    import_batch = Column(String(50))
    last_imported_at = Column(DateTime(timezone=True), default=lambda: local_now())

    # Metadata
    notes = Column(Text)
    extra_data = Column(JSONB, default=dict)
    created_at = Column(DateTime(timezone=True), default=lambda: local_now())
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: local_now(),
        onupdate=lambda: local_now(),
    )

    @property
    def churn_risk(self) -> str:
        if self.days_since_last_order is None:
            return "unknown"
        if self.days_since_last_order <= 30:
            return "active"
        if self.days_since_last_order <= 90:
            return "healthy"
        if self.days_since_last_order <= 180:
            return "watch"
        if self.days_since_last_order <= 365:
            return "at_risk"
        return "churned"

    @property
    def revenue_trend(self) -> str:
        if not self.revenue_last_year or self.revenue_last_year <= 0:
            if self.revenue_current_year and self.revenue_current_year > 0:
                return "new"
            return "inactive"
        from datetime import datetime

        current_month = datetime.now().month or 1
        annualized = (self.revenue_current_year or 0) * (12 / current_month)
        ratio = annualized / self.revenue_last_year
        if ratio >= 1.15:
            return "growing"
        if ratio >= 0.85:
            return "stable"
        return "declining"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "customer_code": self.customer_code,
            "customer_name": self.customer_name,
            "customer_group": self.customer_group,
            "customer_type": self.customer_type,
            "is_hotel": self.is_hotel,
            "phone": self.phone,
            "email": self.email,
            "contact_person": self.contact_person,
            "street": self.street,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "country": self.country,
            "revenue_current_year": self.revenue_current_year,
            "revenue_last_year": self.revenue_last_year,
            "revenue_lifetime": self.revenue_lifetime,
            "total_invoices": self.total_invoices,
            "customer_since": self.customer_since,
            "last_order_date": self.last_order_date,
            "days_since_last_order": self.days_since_last_order,
            "churn_risk": self.churn_risk,
            "revenue_trend": self.revenue_trend,
            "sales_rep": self.sales_rep,
            "brand": self.brand,
            "brand_tier": self.brand_tier,
            "room_count": self.room_count,
            "hotel_website": self.hotel_website,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "matched_lead_id": self.matched_lead_id,
            "import_batch": self.import_batch,
            "last_imported_at": (
                self.last_imported_at.isoformat() if self.last_imported_at else None
            ),
            "notes": self.notes,
            "created_at": (self.created_at.isoformat() if self.created_at else None),
            "updated_at": (self.updated_at.isoformat() if self.updated_at else None),
        }

    def __repr__(self) -> str:
        risk = self.churn_risk
        return (
            f"<SAPClient {self.customer_code} '{self.customer_name}' "
            f"rev=${self.revenue_lifetime:,.0f} risk={risk}>"
        )
