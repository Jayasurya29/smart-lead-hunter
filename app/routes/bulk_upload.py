"""
Bulk Upload Routes — Excel/CSV hotel list import with dedup + enrichment.

Endpoints:
  POST /api/bulk-upload/parse    — parse file + dedup check → preview
  POST /api/bulk-upload/confirm  — import approved rows + queue enrichment
"""

import logging

from fastapi import APIRouter, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.services.bulk_upload import parse_upload, dedup_check, import_hotels

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/bulk-upload", tags=["Bulk Upload"])


@router.post("/parse")
async def parse_and_dedup(file: UploadFile = File(...)):
    """Upload an Excel/CSV file, parse it, and check for duplicates.

    Returns structured rows with dedup status so the frontend can show
    a preview table with duplicate flags before the user confirms import.
    """
    fname = (file.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(
            status_code=400,
            detail="Only .xlsx, .xls, or .csv files accepted",
        )

    try:
        raw_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {e}")

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="File is empty")

    # Step 1: Parse
    parsed = await parse_upload(raw_bytes, file.filename or "upload.xlsx")

    if parsed.get("error"):
        raise HTTPException(status_code=400, detail=parsed["error"])

    if not parsed.get("rows"):
        raise HTTPException(
            status_code=400,
            detail="No valid rows found in file",
        )

    # Step 2: Dedup check
    rows_with_dedup = await dedup_check(parsed["rows"])

    # Compute summary stats
    new_count = sum(
        1 for r in rows_with_dedup if r.get("dedup", {}).get("status") == "new"
    )
    dup_existing = sum(
        1
        for r in rows_with_dedup
        if r.get("dedup", {}).get("status") == "duplicate_existing"
    )
    dup_lead = sum(
        1
        for r in rows_with_dedup
        if r.get("dedup", {}).get("status") == "duplicate_lead"
    )
    dup_upload = sum(
        1
        for r in rows_with_dedup
        if r.get("dedup", {}).get("status") == "duplicate_upload"
    )

    return JSONResponse(
        content={
            "rows": rows_with_dedup,
            "summary": {
                "total": len(rows_with_dedup),
                "new": new_count,
                "duplicate_existing": dup_existing,
                "duplicate_lead": dup_lead,
                "duplicate_upload": dup_upload,
            },
            "column_mapping": parsed.get("column_mapping", {}),
            "headers": parsed.get("headers", []),
            "parse_errors": parsed.get("parse_errors", []),
        }
    )


class ConfirmRequest(BaseModel):
    """Request body for confirming import."""

    rows: list[dict]
    skip_duplicates: bool = True


@router.post("/confirm")
async def confirm_import(req: ConfirmRequest):
    """Import approved rows into the database.

    The frontend sends back the rows (possibly filtered by the user)
    and the skip_duplicates flag. New rows are inserted and queued
    for Smart Fill + contact enrichment.
    """
    if not req.rows:
        raise HTTPException(status_code=400, detail="No rows to import")

    result = await import_hotels(
        rows=req.rows,
        skip_duplicates=req.skip_duplicates,
    )

    if result.get("imported", 0) == 0 and result.get("errors"):
        raise HTTPException(
            status_code=400,
            detail=f"Import failed: {result['errors'][0]}",
        )

    return JSONResponse(content=result)
