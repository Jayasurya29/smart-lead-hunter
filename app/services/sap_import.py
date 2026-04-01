"""
SMART LEAD HUNTER — SAP CSV Import Service
============================================
Parses SAP Business One CSV exports and upserts into sap_clients table.
"""

import csv
import logging
import re
import uuid
from datetime import datetime
from io import StringIO
from typing import Optional

from sqlalchemy import select, func, case
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import async_session
from app.models.sap_client import SAPClient

logger = logging.getLogger(__name__)

# ─── Column mapping: SAP CSV header → SAPClient field ────────────────────────
DEFAULT_COLUMN_MAP = {
    "customer_code": "customer_code",
    "customer_name": "customer_name",
    "customer_group": "customer_group",
    "phone": "phone",
    "email": "email",
    "contact_person": "contact_person",
    "street": "street",
    "city": "city",
    "state": "state",
    "zip_code": "zip_code",
    "country": "country",
    "sales_rep": "sales_rep",
    "customer_since": "customer_since",
    "revenue_2026": "revenue_current_year",
    "revenue_2025": "revenue_last_year",
    "revenue_lifetime": "revenue_lifetime",
    "total_invoices": "total_invoices",
    "last_order_date": "last_order_date",
    "days_since_last_order": "days_since_last_order",
}

# ─── Hotel keyword detection ─────────────────────────────────────────────────
HOTEL_KEYWORDS = [
    "hotel",
    "resort",
    "inn",
    "suites",
    "lodge",
    "motel",
    "hospitality",
    "marriott",
    "hilton",
    "hyatt",
    "loews",
    "ritz",
    "four seasons",
    "mandarin",
    "peninsula",
    "fairmont",
    "intercontinental",
    "sheraton",
    "westin",
    "w hotel",
    "st. regis",
    "jw marriott",
    "rosen",
    "grand beach",
    "bungalows",
    "hard rock",
    "ocean reef",
    "club med",
    "sandals",
    "beaches",
    "montage",
    "auberge",
    "rosewood",
    "aman",
    "six senses",
]

PARKING_KEYWORDS = [
    "parking",
    "valet",
    "towne park",
    "park one",
    "laz parking",
    "ace parking",
    "denison parking",
    "reimagined parking",
    "unified parking",
]

RESTAURANT_KEYWORDS = [
    "restaurant",
    "steak",
    "grill",
    "cafe",
    "bistro",
    "kitchen",
    "dining",
    "enzo",
    "prime steak",
]

CONDO_KEYWORDS = [
    "condo",
    "condominium",
    "hoa",
    "association",
    "residential",
]

HOTEL_GROUPS = {
    "HILTON",
    "MARRIOTT",
    "LOEWS HOTELS",
    "ROSEN",
    "HYATT",
    "HHOA",
    "OTHER HOTEL",
    "FOUR SEASONS",
    "MONTAGE",
    "GRAND BEACH",
    "FSR",
}


def _normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"\s+", " ", n)
    n = re.sub(r"\b(llc|inc|corp|ltd|co)\b\.?", "", n)
    return n.strip()


def _parse_float(value: str) -> float:
    if not value or not value.strip():
        return 0.0
    cleaned = value.strip().replace('"', "").replace(",", "")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def _parse_int(value: str) -> Optional[int]:
    if not value or not value.strip():
        return None
    cleaned = value.strip().replace('"', "").replace(",", "")
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _classify_customer(name: str, group: str) -> tuple[str, bool]:
    """Returns (customer_type, is_hotel)."""
    name_lower = (name or "").lower()
    group_upper = (group or "").upper().strip()

    if group_upper in HOTEL_GROUPS:
        return "hotel", True

    for kw in HOTEL_KEYWORDS:
        if kw in name_lower:
            return "hotel", True

    for kw in PARKING_KEYWORDS:
        if kw in name_lower:
            return "parking", False

    for kw in RESTAURANT_KEYWORDS:
        if kw in name_lower:
            return "restaurant", False

    for kw in CONDO_KEYWORDS:
        if kw in name_lower:
            return "condo", False

    return "other", False


def _parse_row(row: dict, column_map: dict, batch_id: str) -> dict:
    mapped = {}

    for csv_col, model_field in column_map.items():
        value = row.get(csv_col, "").strip() if row.get(csv_col) else ""
        mapped[model_field] = value

    # Parse numeric fields
    mapped["revenue_current_year"] = _parse_float(
        mapped.get("revenue_current_year", "0")
    )
    mapped["revenue_last_year"] = _parse_float(mapped.get("revenue_last_year", "0"))
    mapped["revenue_lifetime"] = _parse_float(mapped.get("revenue_lifetime", "0"))
    mapped["total_invoices"] = _parse_int(mapped.get("total_invoices", "0")) or 0
    mapped["days_since_last_order"] = _parse_int(
        mapped.get("days_since_last_order", "")
    )

    # Normalize name
    mapped["customer_name_normalized"] = _normalize_name(
        mapped.get("customer_name", "")
    )

    # Auto-classify
    ctype, is_hotel = _classify_customer(
        mapped.get("customer_name", ""),
        mapped.get("customer_group", ""),
    )
    mapped["customer_type"] = ctype
    mapped["is_hotel"] = is_hotel

    mapped["import_batch"] = batch_id

    return mapped


async def import_sap_csv(
    file_content: str = "",
    file_path: str = "",
    column_map: Optional[dict] = None,
) -> dict:
    """
    Import SAP CSV data into sap_clients table.
    Uses upsert (INSERT ON CONFLICT UPDATE) keyed on customer_code.
    """
    if not column_map:
        column_map = DEFAULT_COLUMN_MAP

    batch_id = f"sap_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # Read CSV
    if file_path:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
    elif file_content:
        content = file_content
    else:
        return {"error": "No file content or path provided"}

    reader = csv.DictReader(StringIO(content))

    # Validate headers
    csv_headers = set(reader.fieldnames or [])
    expected_headers = set(column_map.keys())
    missing = expected_headers - csv_headers
    if missing:
        logger.warning(f"Missing CSV columns: {missing}")
        column_map = {k: v for k, v in column_map.items() if k in csv_headers}

    stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0, "total": 0}
    rows_to_upsert = []

    for row in reader:
        stats["total"] += 1
        try:
            parsed = _parse_row(row, column_map, batch_id)
            if not parsed.get("customer_code"):
                stats["skipped"] += 1
                continue
            rows_to_upsert.append(parsed)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Error parsing row {stats['total']}: {e}")

    if not rows_to_upsert:
        return {**stats, "batch_id": batch_id, "error": "No valid rows to import"}

    # Batch upsert
    async with async_session() as session:
        try:
            for row_data in rows_to_upsert:
                stmt = pg_insert(SAPClient).values(**row_data)

                update_dict = {
                    k: v for k, v in row_data.items() if k != "customer_code"
                }
                update_dict["last_imported_at"] = datetime.now()
                update_dict["updated_at"] = datetime.now()

                stmt = stmt.on_conflict_do_update(
                    index_elements=["customer_code"],
                    set_=update_dict,
                )

                await session.execute(stmt)

            await session.commit()

            # Count records from this batch
            count_result = await session.execute(
                select(func.count(SAPClient.id)).where(
                    SAPClient.import_batch == batch_id
                )
            )
            new_count = count_result.scalar() or 0

            stats["processed"] = len(rows_to_upsert)
            stats["created"] = new_count
            stats["updated"] = len(rows_to_upsert) - new_count

            logger.info(
                f"SAP import batch {batch_id}: "
                f"{stats['processed']} processed, "
                f"{stats['skipped']} skipped, "
                f"{stats['errors']} errors"
            )

        except Exception as e:
            await session.rollback()
            logger.error(f"SAP import failed: {e}")
            return {**stats, "batch_id": batch_id, "error": str(e)}

    return {**stats, "batch_id": batch_id}


async def get_import_summary() -> dict:
    """Get summary stats of imported SAP data."""
    async with async_session() as session:
        total = await session.execute(select(func.count(SAPClient.id)))
        total_count = total.scalar() or 0

        hotels = await session.execute(
            select(func.count(SAPClient.id)).where(SAPClient.is_hotel.is_(True))
        )
        hotel_count = hotels.scalar() or 0

        # Churn breakdown
        churn_query = await session.execute(
            select(
                func.count().label("count"),
                case(
                    (SAPClient.days_since_last_order <= 30, "active"),
                    (SAPClient.days_since_last_order <= 90, "healthy"),
                    (SAPClient.days_since_last_order <= 180, "watch"),
                    (SAPClient.days_since_last_order <= 365, "at_risk"),
                    (SAPClient.days_since_last_order > 365, "churned"),
                    else_="unknown",
                ).label("risk"),
            ).group_by("risk")
        )
        churn_breakdown = {row.risk: row.count for row in churn_query}

        # Revenue totals
        rev_query = await session.execute(
            select(
                func.sum(SAPClient.revenue_lifetime).label("lifetime"),
                func.sum(SAPClient.revenue_current_year).label("current_year"),
                func.sum(SAPClient.revenue_last_year).label("last_year"),
            )
        )
        rev = rev_query.one()

        # Top groups
        group_query = await session.execute(
            select(
                SAPClient.customer_group,
                func.count().label("client_count"),
                func.sum(SAPClient.revenue_lifetime).label("total_revenue"),
            )
            .group_by(SAPClient.customer_group)
            .order_by(func.sum(SAPClient.revenue_lifetime).desc())
            .limit(15)
        )
        top_groups = [
            {
                "group": row.customer_group,
                "client_count": row.client_count,
                "total_revenue": float(row.total_revenue or 0),
            }
            for row in group_query
        ]

        return {
            "total_clients": total_count,
            "hotel_clients": hotel_count,
            "non_hotel_clients": total_count - hotel_count,
            "churn_breakdown": churn_breakdown,
            "revenue": {
                "lifetime": float(rev.lifetime or 0),
                "current_year": float(rev.current_year or 0),
                "last_year": float(rev.last_year or 0),
            },
            "top_groups": top_groups,
        }
