"""
SMART LEAD HUNTER — SAP Client Intelligence API Routes
========================================================
Add to main.py:
    from app.routes.sap import router as sap_router, legacy_router as sap_legacy_router
    app.include_router(sap_router)
    app.include_router(sap_legacy_router)
"""

import logging
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import asc, desc, func, or_, select

from app.database import async_session
from app.models.sap_client import SAPClient
from app.services.sap_import import get_import_summary, import_sap_csv

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sap", tags=["sap"])


# ─── Import ──────────────────────────────────────────────────────────────────


@router.post("/import")
async def upload_sap_csv(file: UploadFile = File(...)):
    """Upload and import a SAP Business One CSV or XLSX export."""
    fname = (file.filename or "").lower()
    if not fname.endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Only .csv, .xlsx, or .xls files accepted",
        )

    try:
        raw_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {e}")

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="File is empty")

    # XLSX → pass bytes; CSV → decode then pass content
    if fname.endswith((".xlsx", ".xls")):
        result = await import_sap_csv(
            file_bytes=raw_bytes,
            filename=file.filename,
        )
    else:
        try:
            decoded = raw_bytes.decode("utf-8-sig")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CSV decode error: {e}")
        result = await import_sap_csv(file_content=decoded)

    if result.get("error") and result.get("processed", 0) == 0:
        raise HTTPException(status_code=400, detail=result["error"])

    return JSONResponse(content=result)


@router.get("/import/summary")
async def import_summary():
    """Get summary stats of all imported SAP data."""
    summary = await get_import_summary()
    return JSONResponse(content=summary)


# ─── Client Intelligence ─────────────────────────────────────────────────────


@router.get("/clients")
async def list_clients(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    search: Optional[str] = Query(None, max_length=200),
    group: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    sales_rep: Optional[str] = Query(None),
    is_hotel: Optional[bool] = Query(None),
    customer_type: Optional[str] = Query(None),
    churn_risk: Optional[str] = Query(None),
    sort_by: str = Query("revenue_lifetime", max_length=50),
    sort_dir: str = Query("desc", pattern="^(asc|desc)$"),
    min_revenue: Optional[float] = Query(None),
):
    """List SAP clients with filtering, sorting, and pagination."""
    async with async_session() as session:
        query = select(SAPClient)
        count_query = select(func.count(SAPClient.id))

        filters = []

        if search:
            search_pattern = f"%{search}%"
            filters.append(
                or_(
                    SAPClient.customer_name.ilike(search_pattern),
                    SAPClient.customer_code.ilike(search_pattern),
                    SAPClient.city.ilike(search_pattern),
                    SAPClient.contact_person.ilike(search_pattern),
                )
            )

        if group:
            filters.append(SAPClient.customer_group == group)
        if state:
            filters.append(SAPClient.state == state)
        if sales_rep:
            filters.append(SAPClient.sales_rep == sales_rep)
        if is_hotel is not None:
            filters.append(SAPClient.is_hotel == is_hotel)
        if customer_type:
            filters.append(SAPClient.customer_type == customer_type)
        if min_revenue is not None:
            filters.append(SAPClient.revenue_lifetime >= min_revenue)

        if churn_risk:
            risk_filters = {
                "active": (None, 30),
                "healthy": (31, 90),
                "watch": (91, 180),
                "at_risk": (181, 365),
                "churned": (366, None),
            }
            if churn_risk in risk_filters:
                low, high = risk_filters[churn_risk]
                if low is not None:
                    filters.append(SAPClient.days_since_last_order >= low)
                if high is not None:
                    filters.append(SAPClient.days_since_last_order <= high)

        for f in filters:
            query = query.where(f)
            count_query = count_query.where(f)

        allowed_sort_fields = {
            "customer_name": SAPClient.customer_name,
            "revenue_lifetime": SAPClient.revenue_lifetime,
            "revenue_current_year": SAPClient.revenue_current_year,
            "revenue_last_year": SAPClient.revenue_last_year,
            "days_since_last_order": SAPClient.days_since_last_order,
            "total_invoices": SAPClient.total_invoices,
            "customer_since": SAPClient.customer_since,
            "state": SAPClient.state,
            "customer_group": SAPClient.customer_group,
        }
        sort_col = allowed_sort_fields.get(sort_by, SAPClient.revenue_lifetime)
        order_fn = desc if sort_dir == "desc" else asc
        query = query.order_by(order_fn(sort_col))

        total_result = await session.execute(count_query)
        total = total_result.scalar()

        offset = (page - 1) * per_page
        query = query.offset(offset).limit(per_page)

        result = await session.execute(query)
        clients = result.scalars().all()

        return JSONResponse(
            content={
                "clients": [c.to_dict() for c in clients],
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page,
            }
        )


@router.get("/clients/{client_id}")
async def get_client(client_id: int):
    """Get a single SAP client by ID."""
    async with async_session() as session:
        result = await session.execute(
            select(SAPClient).where(SAPClient.id == client_id)
        )
        client = result.scalar_one_or_none()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        return JSONResponse(content=client.to_dict())


# ─── Analytics ────────────────────────────────────────────────────────────────


@router.get("/analytics/pareto")
async def pareto_analysis(
    is_hotel: Optional[bool] = Query(None),
    state: Optional[str] = Query(None),
):
    """Pareto analysis — clients ranked by revenue with cumulative %."""
    async with async_session() as session:
        query = select(
            SAPClient.id,
            SAPClient.customer_code,
            SAPClient.customer_name,
            SAPClient.customer_group,
            SAPClient.revenue_lifetime,
            SAPClient.revenue_current_year,
            SAPClient.revenue_last_year,
            SAPClient.sales_rep,
            SAPClient.state,
            SAPClient.is_hotel,
        ).order_by(desc(SAPClient.revenue_lifetime))

        if is_hotel is not None:
            query = query.where(SAPClient.is_hotel == is_hotel)
        if state:
            query = query.where(SAPClient.state == state)

        result = await session.execute(query)
        rows = result.all()

        total_revenue = sum(r.revenue_lifetime or 0 for r in rows)
        cumulative = 0
        pareto_data = []

        for i, r in enumerate(rows, 1):
            rev = r.revenue_lifetime or 0
            cumulative += rev
            pareto_data.append(
                {
                    "rank": i,
                    "id": r.id,
                    "customer_code": r.customer_code,
                    "customer_name": r.customer_name,
                    "customer_group": r.customer_group,
                    "revenue_lifetime": rev,
                    "revenue_current_year": r.revenue_current_year or 0,
                    "revenue_last_year": r.revenue_last_year or 0,
                    "sales_rep": r.sales_rep,
                    "state": r.state,
                    "is_hotel": r.is_hotel,
                    "pct_of_total": round(rev / total_revenue * 100, 2)
                    if total_revenue
                    else 0,
                    "cumulative_pct": round(cumulative / total_revenue * 100, 2)
                    if total_revenue
                    else 0,
                }
            )

        return JSONResponse(
            content={
                "total_revenue": total_revenue,
                "total_clients": len(rows),
                "pareto": pareto_data,
            }
        )


@router.get("/analytics/brand-penetration")
async def brand_penetration():
    """Brand penetration — clients per group with revenue."""
    async with async_session() as session:
        result = await session.execute(
            select(
                SAPClient.customer_group,
                func.count().label("client_count"),
                func.sum(SAPClient.revenue_lifetime).label("total_revenue"),
                func.avg(SAPClient.revenue_lifetime).label("avg_revenue"),
                func.sum(SAPClient.revenue_current_year).label("revenue_ytd"),
                func.array_agg(SAPClient.state).label("states"),
            )
            .where(SAPClient.is_hotel.is_(True))
            .group_by(SAPClient.customer_group)
            .order_by(desc(func.sum(SAPClient.revenue_lifetime)))
        )
        rows = result.all()

        brands = []
        for r in rows:
            states = list(set(s for s in (r.states or []) if s))
            brands.append(
                {
                    "group": r.customer_group,
                    "client_count": r.client_count,
                    "total_revenue": float(r.total_revenue or 0),
                    "avg_revenue": round(float(r.avg_revenue or 0), 2),
                    "revenue_ytd": float(r.revenue_ytd or 0),
                    "states": sorted(states),
                    "state_count": len(states),
                }
            )

        return JSONResponse(content={"brands": brands})


@router.get("/analytics/churn-risk")
async def churn_risk_report(
    is_hotel: Optional[bool] = Query(None),
    min_revenue: float = Query(0),
):
    """Churn risk — clients at risk sorted by revenue."""
    async with async_session() as session:
        query = (
            select(SAPClient)
            .where(SAPClient.days_since_last_order > 90)
            .where(SAPClient.revenue_lifetime > min_revenue)
            .order_by(desc(SAPClient.revenue_lifetime))
        )

        if is_hotel is not None:
            query = query.where(SAPClient.is_hotel == is_hotel)

        result = await session.execute(query)
        clients = result.scalars().all()

        return JSONResponse(
            content={
                "at_risk_clients": [c.to_dict() for c in clients],
                "total_at_risk": len(clients),
                "total_revenue_at_risk": sum(c.revenue_lifetime or 0 for c in clients),
            }
        )


@router.get("/analytics/geo")
async def geo_distribution():
    """Geographic distribution for map feature."""
    async with async_session() as session:
        result = await session.execute(
            select(
                SAPClient.state,
                func.count().label("client_count"),
                func.sum(SAPClient.revenue_lifetime).label("total_revenue"),
                func.sum(SAPClient.revenue_current_year).label("revenue_ytd"),
            )
            .where(SAPClient.state.isnot(None))
            .where(SAPClient.state != "")
            .group_by(SAPClient.state)
            .order_by(desc(func.sum(SAPClient.revenue_lifetime)))
        )
        rows = result.all()

        states = [
            {
                "state": r.state,
                "client_count": r.client_count,
                "total_revenue": float(r.total_revenue or 0),
                "revenue_ytd": float(r.revenue_ytd or 0),
            }
            for r in rows
        ]

        return JSONResponse(content={"states": states})


@router.get("/filters")
async def get_filter_options():
    """Returns available filter values for dropdowns."""
    async with async_session() as session:
        groups = await session.execute(
            select(SAPClient.customer_group)
            .where(SAPClient.customer_group.isnot(None))
            .distinct()
            .order_by(SAPClient.customer_group)
        )
        states = await session.execute(
            select(SAPClient.state)
            .where(SAPClient.state.isnot(None))
            .where(SAPClient.state != "")
            .distinct()
            .order_by(SAPClient.state)
        )
        reps = await session.execute(
            select(SAPClient.sales_rep)
            .where(SAPClient.sales_rep.isnot(None))
            .distinct()
            .order_by(SAPClient.sales_rep)
        )
        types = await session.execute(
            select(SAPClient.customer_type)
            .where(SAPClient.customer_type.isnot(None))
            .distinct()
            .order_by(SAPClient.customer_type)
        )

        return JSONResponse(
            content={
                "groups": [r[0] for r in groups],
                "states": [r[0] for r in states],
                "sales_reps": [r[0] for r in reps],
                "customer_types": [r[0] for r in types],
            }
        )


# ─── Legacy alias routes (for frontend calling /sap/* without /api prefix) ───

legacy_router = APIRouter(prefix="/sap", tags=["sap-legacy"])


@legacy_router.post("/import")
async def upload_sap_csv_legacy(file: UploadFile = File(...)):
    """Legacy alias for /api/sap/import (used by frontend)."""
    return await upload_sap_csv(file)


@legacy_router.get("/import/summary")
async def import_summary_legacy():
    """Legacy alias for /api/sap/import/summary."""
    return await import_summary()
