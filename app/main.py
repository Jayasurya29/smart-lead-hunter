"""
Smart Lead Hunter - Main Application
------------------------------------
FastAPI entry point with REST API endpoints

Features:
- Lead management endpoints (CRUD)
- Manual scrape triggers
- Health checks and status
- Dashboard stats

Run with:
    uvicorn app.main:app --reload --port 8000
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from sqlalchemy.orm import selectinload

from .config import settings
from .database import get_db, init_db
from .models import PotentialLead, Source, ScrapeLog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Pydantic Models (Request/Response Schemas)
# -----------------------------------------------------------------------------

class LeadBase(BaseModel):
    """Base lead schema"""
    hotel_name: str
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = "USA"
    opening_date: Optional[str] = None
    room_count: Optional[int] = None
    hotel_type: Optional[str] = None
    brand: Optional[str] = None
    notes: Optional[str] = None


class LeadCreate(LeadBase):
    """Schema for creating a lead"""
    source_url: Optional[str] = None


class LeadUpdate(BaseModel):
    """Schema for updating a lead"""
    status: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    notes: Optional[str] = None
    

class LeadResponse(LeadBase):
    """Schema for lead response"""
    id: int
    lead_score: Optional[int] = None
    status: str
    source_url: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class LeadListResponse(BaseModel):
    """Paginated lead list response"""
    leads: List[LeadResponse]
    total: int
    page: int
    per_page: int
    pages: int


class SourceBase(BaseModel):
    """Base source schema"""
    name: str
    base_url: str
    source_type: Optional[str] = "aggregator"
    priority: Optional[int] = 5
    is_active: Optional[bool] = True


class SourceCreate(SourceBase):
    """Schema for creating a source"""
    pass


class SourceResponse(BaseModel):
    """Schema for source response"""
    id: str
    name: str
    url: str
    scrape_frequency: Optional[str] = None
    is_active: bool
    last_scraped_at: Optional[datetime] = None
    leads_found: Optional[int] = 0
    
    class Config:
        from_attributes = True


class ScrapeLogResponse(BaseModel):
    """Schema for scrape log response"""
    id: int
    source_id: Optional[int] = None
    source_name: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    urls_scraped: int = 0
    leads_found: int = 0
    
    class Config:
        from_attributes = True


class StatsResponse(BaseModel):
    """Schema for dashboard stats"""
    total_leads: int
    new_leads: int
    approved_leads: int
    pending_leads: int
    bad_leads: int
    total_sources: int
    active_sources: int
    leads_today: int
    leads_this_week: int


# -----------------------------------------------------------------------------
# Application Lifecycle
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    # Startup
    logger.info("=" * 50)
    logger.info("Starting Smart Lead Hunter...")
    logger.info("=" * 50)
    
    # Initialize database tables
    await init_db()
    logger.info("Database tables verified")
    
    logger.info("Smart Lead Hunter is ready!")
    logger.info("API Docs: http://localhost:8000/docs")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Smart Lead Hunter...")


# -----------------------------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------------------------

app = FastAPI(
    title="Smart Lead Hunter",
    description="Automated hotel lead generation system for J.A. Uniforms",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Health & Status Endpoints
# -----------------------------------------------------------------------------

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API info"""
    return {
        "name": "Smart Lead Hunter",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "company": "J.A. Uniforms"
    }


@app.get("/health", tags=["Health"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """Health check endpoint"""
    # Check database
    try:
        await db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {e}"
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "database": db_status,
            "insightly": "disabled (for later)"
        }
    }


@app.get("/stats", response_model=StatsResponse, tags=["Dashboard"])
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Get dashboard statistics"""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    
    # Total leads
    result = await db.execute(select(func.count(PotentialLead.id)))
    total_leads = result.scalar() or 0
    
    # New leads
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.status == "new")
    )
    new_leads = result.scalar() or 0
    
    # Approved leads
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.status == "approved")
    )
    approved_leads = result.scalar() or 0
    
    # Pending leads
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.status == "pending")
    )
    pending_leads = result.scalar() or 0
    
    # Bad leads
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.status == "bad")
    )
    bad_leads = result.scalar() or 0
    
    # Total sources
    result = await db.execute(select(func.count(Source.id)))
    total_sources = result.scalar() or 0
    
    # Active sources
    result = await db.execute(
        select(func.count(Source.id)).where(Source.is_active == True)
    )
    active_sources = result.scalar() or 0
    
    # Leads today
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.created_at >= today_start)
    )
    leads_today = result.scalar() or 0
    
    # Leads this week
    result = await db.execute(
        select(func.count(PotentialLead.id)).where(PotentialLead.created_at >= week_start)
    )
    leads_this_week = result.scalar() or 0
    
    return StatsResponse(
        total_leads=total_leads,
        new_leads=new_leads,
        approved_leads=approved_leads,
        pending_leads=pending_leads,
        bad_leads=bad_leads,
        total_sources=total_sources,
        active_sources=active_sources,
        leads_today=leads_today,
        leads_this_week=leads_this_week
    )


# -----------------------------------------------------------------------------
# Lead Endpoints
# -----------------------------------------------------------------------------

@app.get("/leads", response_model=LeadListResponse, tags=["Leads"])
async def list_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    state: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """List leads with filtering and pagination"""
    # Build base query
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))
    
    # Apply filters
    if status:
        query = query.where(PotentialLead.status == status)
        count_query = count_query.where(PotentialLead.status == status)
    if min_score:
        query = query.where(PotentialLead.lead_score >= min_score)
        count_query = count_query.where(PotentialLead.lead_score >= min_score)
    if state:
        query = query.where(PotentialLead.state.ilike(f"%{state}%"))
        count_query = count_query.where(PotentialLead.state.ilike(f"%{state}%"))
    if search:
        search_filter = (
            PotentialLead.hotel_name.ilike(f"%{search}%") |
            PotentialLead.city.ilike(f"%{search}%") |
            PotentialLead.brand.ilike(f"%{search}%")
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)
    
    # Get total count
    result = await db.execute(count_query)
    total = result.scalar() or 0
    
    # Apply pagination and ordering
    offset = (page - 1) * per_page
    query = query.order_by(
        PotentialLead.lead_score.desc().nullsfirst(),
        PotentialLead.created_at.desc()
    ).offset(offset).limit(per_page)
    
    result = await db.execute(query)
    leads = result.scalars().all()
    
    # Calculate total pages
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    return LeadListResponse(
        leads=[LeadResponse.model_validate(lead) for lead in leads],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages
    )


@app.get("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def get_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single lead by ID"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads", response_model=LeadResponse, tags=["Leads"])
async def create_lead(
    lead_data: LeadCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new lead manually"""
    # Normalize hotel name
    normalized_name = lead_data.hotel_name.lower().strip()
    
    # Create lead
    lead = PotentialLead(
        hotel_name=lead_data.hotel_name,
        hotel_name_normalized=normalized_name,
        contact_email=lead_data.contact_email,
        contact_phone=lead_data.contact_phone,
        contact_name=lead_data.contact_name,
        city=lead_data.city,
        state=lead_data.state,
        country=lead_data.country,
        opening_date=lead_data.opening_date,
        room_count=lead_data.room_count,
        hotel_type=lead_data.hotel_type,
        brand=lead_data.brand,
        notes=lead_data.notes,
        source_url=lead_data.source_url,
        status="new"
    )
    
    db.add(lead)
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Created lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.patch("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def update_lead(
    lead_id: int,
    updates: LeadUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a lead"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    # Apply updates
    update_data = updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(lead, field, value)
    
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Updated lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads/{lead_id}/approve", response_model=LeadResponse, tags=["Leads"])
async def approve_lead(
    lead_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Approve a lead (marks as ready for CRM sync when enabled)"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    if lead.status == "approved":
        raise HTTPException(status_code=400, detail="Lead already approved")
    
    lead.status = "approved"
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Approved lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.post("/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def reject_lead(
    lead_id: int,
    reason: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Reject/mark a lead as bad"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    lead.status = "bad"
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    await db.refresh(lead)
    
    logger.info(f"Rejected lead: {lead.hotel_name} (ID: {lead.id})")
    
    return LeadResponse.model_validate(lead)


@app.delete("/leads/{lead_id}", tags=["Leads"])
async def delete_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a lead"""
    result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    hotel_name = lead.hotel_name
    await db.delete(lead)
    await db.commit()
    
    logger.info(f"Deleted lead: {hotel_name} (ID: {lead_id})")
    
    return {"message": "Lead deleted", "id": lead_id}


# -----------------------------------------------------------------------------
# Source Endpoints
# -----------------------------------------------------------------------------

@app.get("/sources", response_model=List[SourceResponse], tags=["Sources"])
async def list_sources(
    active_only: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """List all scraping sources"""
    query = select(Source)
    
    if active_only:
        query = query.where(Source.is_active == True)
    
    query = query.order_by(Source.name)
    
    result = await db.execute(query)
    sources = result.scalars().all()
    
    return [SourceResponse.model_validate(source) for source in sources]


@app.get("/sources/{source_id}", response_model=SourceResponse, tags=["Sources"])
async def get_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single source by ID"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    return SourceResponse.model_validate(source)


@app.post("/sources", response_model=SourceResponse, tags=["Sources"])
async def create_source(
    source_data: SourceCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new scraping source"""
    # Check for duplicate URL
    result = await db.execute(
        select(Source).where(Source.base_url == source_data.base_url)
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(status_code=409, detail="Source with this URL already exists")
    
    source = Source(
        name=source_data.name,
        base_url=source_data.base_url,
        source_type=source_data.source_type,
        priority=source_data.priority,
        is_active=source_data.is_active
    )
    
    db.add(source)
    await db.commit()
    await db.refresh(source)
    
    logger.info(f"Created source: {source.name} (ID: {source.id})")
    
    return SourceResponse.model_validate(source)


@app.post("/sources/{source_id}/toggle", response_model=SourceResponse, tags=["Sources"])
async def toggle_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Toggle source active/inactive"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    source.is_active = not source.is_active
    await db.commit()
    await db.refresh(source)
    
    status = "activated" if source.is_active else "deactivated"
    logger.info(f"Source {status}: {source.name} (ID: {source.id})")
    
    return SourceResponse.model_validate(source)


@app.delete("/sources/{source_id}", tags=["Sources"])
async def delete_source(source_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a source"""
    result = await db.execute(
        select(Source).where(Source.id == source_id)
    )
    source = result.scalar_one_or_none()
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    name = source.name
    await db.delete(source)
    await db.commit()
    
    logger.info(f"Deleted source: {name} (ID: {source_id})")
    
    return {"message": "Source deleted", "id": source_id}


# -----------------------------------------------------------------------------
# Scrape Logs Endpoint
# -----------------------------------------------------------------------------

@app.get("/scrape/logs", response_model=List[ScrapeLogResponse], tags=["Scraping"])
async def get_scrape_logs(
    limit: int = Query(20, ge=1, le=100),
    source_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get recent scrape logs"""
    query = select(ScrapeLog)
    
    if source_id:
        query = query.where(ScrapeLog.source_id == source_id)
    
    query = query.order_by(ScrapeLog.started_at.desc()).limit(limit)
    
    result = await db.execute(query)
    logs = result.scalars().all()
    
    # Add source names
    response_list = []
    for log in logs:
        log_response = ScrapeLogResponse.model_validate(log)
        if log.source_id:
            source_result = await db.execute(
                select(Source).where(Source.id == log.source_id)
            )
            source = source_result.scalar_one_or_none()
            log_response.source_name = source.name if source else None
        response_list.append(log_response)
    
    return response_list


# -----------------------------------------------------------------------------
# Placeholder for future scraping endpoints
# -----------------------------------------------------------------------------

@app.post("/scrape/test", tags=["Scraping"])
async def test_scrape_endpoint():
    """
    Placeholder for scraping functionality.
    Will be enabled once scraping services are ready.
    """
    return {
        "message": "Scraping endpoints coming soon!",
        "status": "not_implemented",
        "note": "Use Celery worker for background scraping"
    }


# -----------------------------------------------------------------------------
# Run with: uvicorn app.main:app --reload --port 8000
# -----------------------------------------------------------------------------