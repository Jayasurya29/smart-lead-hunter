"""
Bulk Upload Service — Parse Excel, dedup, insert, queue enrichment.

Flow:
  1. parse_upload() — reads Excel/CSV, auto-maps columns, returns structured rows
  2. dedup_check() — checks each row against existing_hotels + potential_leads
  3. import_hotels() — inserts clean rows, queues Smart Fill + contact enrichment
"""

import io
import logging
import re
from datetime import datetime
from typing import Optional

from sqlalchemy import select

from app.database import async_session
from app.models.existing_hotel import ExistingHotel
from app.models.potential_lead import PotentialLead
from app.services.utils import normalize_hotel_name

logger = logging.getLogger(__name__)

# ── Column header auto-detection ──
# Maps common Excel header variations to our internal field names.
# First match wins — order by specificity.
_COLUMN_MAP = {
    "hotel_name": [
        "hotel name",
        "hotel",
        "property name",
        "property",
        "name",
        "hotel_name",
        "property_name",
        "hotelname",
    ],
    "brand": [
        "brand",
        "flag",
        "brand name",
        "hotel brand",
        "chain",
    ],
    "city": [
        "city",
        "town",
        "municipality",
    ],
    "state": [
        "state",
        "province",
        "state/province",
        "region",
        "st",
    ],
    "country": [
        "country",
        "nation",
        "country code",
        "country_code",
    ],
    "opening_date": [
        "opening date",
        "open date",
        "opening",
        "expected opening",
        "opening_date",
        "open_date",
        "projected opening",
    ],
    "room_count": [
        "rooms",
        "room count",
        "room_count",
        "# rooms",
        "num rooms",
        "number of rooms",
        "keys",
        "key count",
    ],
    "address": [
        "address",
        "street address",
        "street",
        "location address",
    ],
    "management_company": [
        "management company",
        "operator",
        "management",
        "managed by",
        "management_company",
        "mgmt company",
        "operating company",
    ],
    "owner": [
        "owner",
        "ownership",
        "owned by",
        "owner name",
        "property owner",
    ],
    "developer": [
        "developer",
        "development company",
        "developed by",
    ],
    "hotel_type": [
        "type",
        "hotel type",
        "property type",
        "category",
        "hotel_type",
        "property_type",
    ],
    "target_table": [
        "target",
        "table",
        "destination",
        "import to",
        "import_to",
        "status",
        "hotel status",
    ],
    "contact_name": [
        "contact",
        "contact name",
        "gm",
        "general manager",
        "contact_name",
    ],
    "contact_email": [
        "email",
        "contact email",
        "e-mail",
        "contact_email",
    ],
    "contact_phone": [
        "phone",
        "contact phone",
        "telephone",
        "contact_phone",
    ],
    "notes": [
        "notes",
        "comments",
        "remarks",
        "note",
    ],
}


def _auto_map_columns(headers: list[str]) -> dict[str, int]:
    """Auto-detect which Excel column maps to which internal field.

    Returns {field_name: column_index} for every matched column.
    """
    mapping = {}
    headers_lower = [h.strip().lower() for h in headers]

    for field, variants in _COLUMN_MAP.items():
        for variant in variants:
            for idx, header in enumerate(headers_lower):
                if header == variant and field not in mapping:
                    mapping[field] = idx
                    break
            if field in mapping:
                break

    return mapping


async def parse_upload(
    file_bytes: bytes,
    filename: str,
) -> dict:
    """Parse an Excel or CSV file into structured rows with auto-mapped columns.

    Returns:
        {
            "rows": [{field: value, ...}, ...],
            "column_mapping": {field: col_index},
            "headers": ["Hotel Name", "City", ...],
            "total_rows": 120,
            "parse_errors": ["Row 5: missing hotel name"],
        }
    """
    fname = (filename or "").lower()
    rows = []
    headers = []
    parse_errors = []

    if fname.endswith((".xlsx", ".xls")):
        try:
            import openpyxl

            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
            ws = wb.active
            all_rows = list(ws.iter_rows(values_only=True))
            wb.close()

            if not all_rows:
                return {"error": "Excel file is empty", "rows": []}

            # First row = headers
            headers = [str(h or "").strip() for h in all_rows[0]]
            data_rows = all_rows[1:]

        except Exception as e:
            return {"error": f"Failed to read Excel file: {e}", "rows": []}

    elif fname.endswith(".csv"):
        import csv

        try:
            decoded = file_bytes.decode("utf-8-sig")
            reader = csv.reader(io.StringIO(decoded))
            all_rows = list(reader)

            if not all_rows:
                return {"error": "CSV file is empty", "rows": []}

            headers = [str(h or "").strip() for h in all_rows[0]]
            data_rows = all_rows[1:]

        except Exception as e:
            return {"error": f"Failed to read CSV file: {e}", "rows": []}
    else:
        return {"error": f"Unsupported file type: {filename}", "rows": []}

    # Auto-detect column mapping
    col_map = _auto_map_columns(headers)

    if "hotel_name" not in col_map:
        return {
            "error": (
                "Could not find a 'Hotel Name' column. "
                f"Headers found: {', '.join(headers)}"
            ),
            "rows": [],
        }

    # Parse each row
    for row_idx, row in enumerate(data_rows, start=2):  # row 2 = first data row
        row_values = list(row) if not isinstance(row, list) else row

        def _get(field: str) -> Optional[str]:
            idx = col_map.get(field)
            if idx is None or idx >= len(row_values):
                return None
            val = row_values[idx]
            if val is None:
                return None
            return str(val).strip() or None

        hotel_name = _get("hotel_name")
        if not hotel_name:
            parse_errors.append(f"Row {row_idx}: missing hotel name — skipped")
            continue

        # Parse room count as int
        room_count = None
        rc_raw = _get("room_count")
        if rc_raw:
            try:
                room_count = int(float(re.sub(r"[^\d.]", "", rc_raw)))
            except (ValueError, TypeError):
                pass

        # Determine target table from column or from opening_date
        target = (_get("target_table") or "").lower()
        if target in ("existing", "open", "operating", "existing_hotel"):
            target_table = "existing"
        elif target in ("potential", "new", "pre-opening", "lead", "potential_lead"):
            target_table = "potential"
        else:
            # Auto-detect: if opening_date is in the past or empty → existing
            target_table = "auto"

        parsed_row = {
            "row_number": row_idx,
            "hotel_name": hotel_name,
            "hotel_name_normalized": normalize_hotel_name(hotel_name),
            "brand": _get("brand"),
            "city": _get("city"),
            "state": _get("state"),
            "country": _get("country") or "USA",
            "opening_date": _get("opening_date"),
            "room_count": room_count,
            "address": _get("address"),
            "management_company": _get("management_company"),
            "owner": _get("owner"),
            "developer": _get("developer"),
            "hotel_type": _get("hotel_type"),
            "contact_name": _get("contact_name"),
            "contact_email": _get("contact_email"),
            "contact_phone": _get("contact_phone"),
            "notes": _get("notes"),
            "target_table": target_table,
        }
        rows.append(parsed_row)

    return {
        "rows": rows,
        "column_mapping": col_map,
        "headers": headers,
        "total_rows": len(rows),
        "parse_errors": parse_errors,
    }


async def dedup_check(rows: list[dict]) -> list[dict]:
    """Check each parsed row against existing_hotels and potential_leads.

    Returns rows enriched with dedup status:
        row["dedup"] = {
            "status": "new" | "duplicate_existing" | "duplicate_lead" | "duplicate_upload",
            "match_id": 3042,
            "match_name": "The Ritz-Carlton South Beach",
            "match_table": "existing_hotels" | "potential_leads",
            "match_status": "new" | "approved" | etc.,
            "similarity": "exact" | "fuzzy",
        }
    """
    if not rows:
        return rows

    async with async_session() as session:
        # Pre-load all normalized names from both tables for fast matching
        existing_result = await session.execute(
            select(
                ExistingHotel.id,
                ExistingHotel.hotel_name,
                ExistingHotel.hotel_name_normalized,
                ExistingHotel.city,
                ExistingHotel.state,
                ExistingHotel.status,
            )
        )
        existing_hotels = existing_result.all()

        leads_result = await session.execute(
            select(
                PotentialLead.id,
                PotentialLead.hotel_name,
                PotentialLead.hotel_name_normalized,
                PotentialLead.city,
                PotentialLead.state,
                PotentialLead.status,
            )
        )
        potential_leads = leads_result.all()

    # Build lookup dicts: normalized_name → list of (id, name, city, state, status, table)
    db_hotels = []
    for h in existing_hotels:
        norm = h.hotel_name_normalized or normalize_hotel_name(h.hotel_name)
        db_hotels.append(
            {
                "id": h.id,
                "name": h.hotel_name,
                "normalized": norm,
                "city": (h.city or "").lower().strip(),
                "state": (h.state or "").lower().strip(),
                "status": h.status,
                "table": "existing_hotels",
            }
        )
    for lead in potential_leads:
        norm = lead.hotel_name_normalized or normalize_hotel_name(lead.hotel_name)
        db_hotels.append(
            {
                "id": lead.id,
                "name": lead.hotel_name,
                "normalized": norm,
                "city": (lead.city or "").lower().strip(),
                "state": (lead.state or "").lower().strip(),
                "status": lead.status,
                "table": "potential_leads",
            }
        )

    # Also track upload-internal duplicates
    seen_in_upload = {}  # normalized_name+city → row_number

    for row in rows:
        norm = row["hotel_name_normalized"]
        row_city = (row.get("city") or "").lower().strip()
        row_state = (row.get("state") or "").lower().strip()

        # Check upload-internal duplicates
        upload_key = f"{norm}|{row_city}|{row_state}"
        if upload_key in seen_in_upload:
            row["dedup"] = {
                "status": "duplicate_upload",
                "match_row": seen_in_upload[upload_key],
                "similarity": "exact",
            }
            continue
        seen_in_upload[upload_key] = row["row_number"]

        # Check against DB
        best_match = None
        best_score = 0

        for db in db_hotels:
            score = 0

            # Exact normalized name match
            if db["normalized"] == norm:
                score = 100
            # Fuzzy: one contains the other
            elif norm in db["normalized"] or db["normalized"] in norm:
                score = 80
            # Fuzzy: word overlap (Jaccard)
            else:
                words_a = set(norm.split())
                words_b = set(db["normalized"].split())
                if words_a and words_b:
                    intersection = words_a & words_b
                    union = words_a | words_b
                    jaccard = len(intersection) / len(union)
                    if jaccard >= 0.6:
                        score = int(jaccard * 70)

            if score < 50:
                continue

            # Boost if city/state match
            if row_city and db["city"] and row_city == db["city"]:
                score += 10
            if row_state and db["state"] and row_state == db["state"]:
                score += 5

            if score > best_score:
                best_score = score
                best_match = db

        if best_match and best_score >= 60:
            table_label = (
                "duplicate_existing"
                if best_match["table"] == "existing_hotels"
                else "duplicate_lead"
            )
            row["dedup"] = {
                "status": table_label,
                "match_id": best_match["id"],
                "match_name": best_match["name"],
                "match_table": best_match["table"],
                "match_status": best_match["status"],
                "similarity": "exact" if best_score >= 90 else "fuzzy",
                "score": best_score,
            }
        else:
            row["dedup"] = {"status": "new"}

    return rows


async def import_hotels(
    rows: list[dict],
    skip_duplicates: bool = True,
) -> dict:
    """Insert approved rows into the database and queue enrichment.

    Args:
        rows: Parsed rows with dedup info. Only rows where dedup.status=="new"
              (or all if skip_duplicates=False) are imported.
        skip_duplicates: If True, skip rows flagged as duplicates.

    Returns:
        {
            "imported": 42,
            "skipped_duplicates": 8,
            "skipped_errors": 2,
            "hotels": [{"id": 3200, "name": "...", "table": "existing_hotels"}, ...],
            "errors": ["Row 15: ..."],
        }
    """
    imported = []
    skipped_dupes = 0
    skipped_errors = 0
    errors = []

    async with async_session() as session:
        for row in rows:
            dedup = row.get("dedup", {})

            # Skip duplicates if requested
            if skip_duplicates and dedup.get("status") != "new":
                skipped_dupes += 1
                continue

            hotel_name = row["hotel_name"]
            try:
                # Determine target table
                target = row.get("target_table", "auto")
                if target == "auto":
                    # If opening_date looks like a future date → potential
                    # Otherwise → existing
                    target = _guess_target_table(row.get("opening_date"))

                if target == "existing":
                    hotel = ExistingHotel(
                        hotel_name=hotel_name,
                        hotel_name_normalized=row["hotel_name_normalized"],
                        brand=row.get("brand"),
                        city=row.get("city"),
                        state=row.get("state"),
                        country=row.get("country", "USA"),
                        opening_date=row.get("opening_date"),
                        room_count=row.get("room_count"),
                        address=row.get("address"),
                        management_company=row.get("management_company"),
                        owner=row.get("owner"),
                        developer=row.get("developer"),
                        hotel_type=row.get("hotel_type"),
                        contact_name=row.get("contact_name"),
                        contact_email=row.get("contact_email"),
                        contact_phone=row.get("contact_phone"),
                        notes=row.get("notes"),
                        data_source="bulk_upload",
                        status="new",
                    )
                    session.add(hotel)
                    await session.flush()
                    imported.append(
                        {
                            "id": hotel.id,
                            "name": hotel_name,
                            "table": "existing_hotels",
                            "row_number": row.get("row_number"),
                        }
                    )
                else:
                    # Potential lead
                    from app.services.utils import get_timeline_label

                    timeline = None
                    if row.get("opening_date"):
                        try:
                            timeline = get_timeline_label(row["opening_date"])
                        except Exception:
                            pass

                    lead = PotentialLead(
                        hotel_name=hotel_name,
                        hotel_name_normalized=row["hotel_name_normalized"],
                        brand=row.get("brand"),
                        city=row.get("city"),
                        state=row.get("state"),
                        country=row.get("country", "USA"),
                        opening_date=row.get("opening_date"),
                        timeline_label=timeline,
                        room_count=row.get("room_count"),
                        address=row.get("address"),
                        management_company=row.get("management_company"),
                        owner=row.get("owner"),
                        developer=row.get("developer"),
                        hotel_type=row.get("hotel_type"),
                        contact_name=row.get("contact_name"),
                        contact_email=row.get("contact_email"),
                        contact_phone=row.get("contact_phone"),
                        notes=row.get("notes"),
                        source_site="bulk_upload",
                        status="new",
                    )
                    session.add(lead)
                    await session.flush()
                    imported.append(
                        {
                            "id": lead.id,
                            "name": hotel_name,
                            "table": "potential_leads",
                            "row_number": row.get("row_number"),
                        }
                    )

            except Exception as e:
                skipped_errors += 1
                errors.append(f"Row {row.get('row_number', '?')}: {hotel_name} — {e}")
                logger.warning(f"Bulk upload error for '{hotel_name}': {e}")

        await session.commit()

    logger.info(
        f"Bulk upload complete: {len(imported)} imported, "
        f"{skipped_dupes} duplicates skipped, {skipped_errors} errors"
    )

    return {
        "imported": len(imported),
        "skipped_duplicates": skipped_dupes,
        "skipped_errors": skipped_errors,
        "hotels": imported,
        "errors": errors,
    }


def _guess_target_table(opening_date: Optional[str]) -> str:
    """Guess whether a hotel is existing or pre-opening based on opening_date."""
    if not opening_date:
        return "existing"  # No date → assume already open

    od = str(opening_date).lower().strip()

    # Extract year
    year_match = re.search(r"20(\d{2})", od)
    if not year_match:
        return "existing"

    year = 2000 + int(year_match.group(1))
    now = datetime.now()

    if year < now.year:
        return "existing"  # Past year → already open
    elif year == now.year:
        # Same year — check month if available
        month_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
            "q1": 3,
            "q2": 6,
            "q3": 9,
            "q4": 12,
        }
        for kw, month in month_map.items():
            if kw in od:
                if month <= now.month:
                    return "existing"
                else:
                    return "potential"
        # Same year, no month info → assume existing (conservative)
        return "existing"
    else:
        return "potential"  # Future year
