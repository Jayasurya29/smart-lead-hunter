"""Affiliation / coverage read endpoints (Phase 2).

Thin wrappers over app.services.affiliations:

  GET /api/affiliations/person/{person_type}/{person_id}
      employer + resolved coverage (explicit covers + derived portfolio)
  GET /api/affiliations/account/{account_type}/{account_id}
      everyone who covers a hotel/lead (direct edges + portfolio buyers)
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.affiliations import (
    get_affiliations_for_person,
    get_coverage_for_account,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Affiliations"])

_PERSON_TYPES = {"contact", "lead_contact"}
_ACCOUNT_TYPES = {"existing_hotel", "potential_lead"}


@router.get("/api/affiliations/person/{person_type}/{person_id}")
async def person_affiliations(
    person_type: str,
    person_id: int,
    db: AsyncSession = Depends(get_db),
):
    if person_type not in _PERSON_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"person_type must be one of {sorted(_PERSON_TYPES)}",
        )
    return await get_affiliations_for_person(db, person_type, person_id)


@router.get("/api/affiliations/account/{account_type}/{account_id}")
async def account_coverage(
    account_type: str,
    account_id: int,
    db: AsyncSession = Depends(get_db),
):
    if account_type not in _ACCOUNT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"account_type must be one of {sorted(_ACCOUNT_TYPES)}",
        )
    return await get_coverage_for_account(db, account_type, account_id)
