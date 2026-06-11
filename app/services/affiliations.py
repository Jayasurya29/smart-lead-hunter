"""Coverage resolver over contact_affiliations (Phase 2).

Reads the typed edges from migration 034 and answers the two questions the UI
needs once the single matched_* link is no longer the source of truth:

  PERSON -> who employs them, and which accounts do they cover?
    coverage = explicit `covers` / `stationed_at` edges
               UNION
               (if employed_by a management company with scope='portfolio')
               every hotel/lead managed by that company, DERIVED from
               management_company — never stored, so 1 VP != 20 rows.

  ACCOUNT (hotel/lead) -> who covers it?
    direct edges pointing at it
    UNION
    the mgmt-co portfolio buyers whose employer name matches this account's
    management_company (they cover it by derivation).

Read-only: no writes, no migrations. Backs the /api/affiliations/* endpoints.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_ACCOUNT_TABLE = {
    "existing_hotel": "existing_hotels",
    "potential_lead": "potential_leads",
}
_COVERAGE_CAP = 500  # don't ship an unbounded list to the UI


async def _account_name(session: AsyncSession, account_type: str, account_id: int) -> Optional[str]:
    tbl = _ACCOUNT_TABLE.get(account_type)
    if not tbl or account_id is None:
        return None
    return (
        await session.execute(
            text(f"SELECT hotel_name FROM {tbl} WHERE id = :id"), {"id": account_id}
        )
    ).scalar()


async def _account_mgmt(session: AsyncSession, account_type: str, account_id: int) -> Optional[str]:
    tbl = _ACCOUNT_TABLE.get(account_type)
    if not tbl or account_id is None:
        return None
    return (
        await session.execute(
            text(f"SELECT management_company FROM {tbl} WHERE id = :id"), {"id": account_id}
        )
    ).scalar()


async def _managed_properties(session: AsyncSession, mgmt_name: Optional[str]) -> list[dict]:
    """The derived portfolio: every hotel + lead managed by this company."""
    if not mgmt_name or not mgmt_name.strip():
        return []
    out: list[dict] = []
    for account_type, tbl in _ACCOUNT_TABLE.items():
        rows = (
            await session.execute(
                text(
                    f"SELECT id, hotel_name FROM {tbl} "
                    "WHERE lower(management_company) = lower(:m)"
                ),
                {"m": mgmt_name.strip()},
            )
        ).all()
        out += [
            {
                "account_type": account_type,
                "account_id": r.id,
                "name": r.hotel_name,
                "via": "portfolio",
            }
            for r in rows
        ]
    return out


async def _person_identity(
    session: AsyncSession, person_type: str, person_id: int
) -> Optional[dict]:
    if person_type == "contact":
        r = (
            (
                await session.execute(
                    text(
                        "SELECT first_name, last_name, display_name, email, title, "
                        "organization, contact_category, is_decision_maker "
                        "FROM contacts WHERE id = :id"
                    ),
                    {"id": person_id},
                )
            )
            .mappings()
            .first()
        )
        if not r:
            return None
        name = (
            " ".join(p for p in (r["first_name"], r["last_name"]) if p)
            or r["display_name"]
            or r["email"]
        )
        return {
            "person_type": "contact",
            "person_id": person_id,
            "name": name,
            "title": r["title"],
            "organization": r["organization"],
            "email": r["email"],
            "contact_category": r["contact_category"],
            "is_decision_maker": r["is_decision_maker"],
            "is_saved": True,
        }
    if person_type == "lead_contact":
        r = (
            (
                await session.execute(
                    text(
                        "SELECT name, email, title, organization, is_saved FROM lead_contacts WHERE id = :id"
                    ),
                    {"id": person_id},
                )
            )
            .mappings()
            .first()
        )
        if not r:
            return None
        return {
            "person_type": "lead_contact",
            "person_id": person_id,
            "name": r["name"],
            "title": r["title"],
            "organization": r["organization"],
            "email": r["email"],
            "contact_category": "buyer",
            "is_decision_maker": None,
            "is_saved": r["is_saved"],
        }
    return None


async def get_affiliations_for_person(
    session: AsyncSession, person_type: str, person_id: int
) -> dict[str, Any]:
    """Employer(s) + resolved coverage for one person. If the person has been
    resolved (lead_contacts.person_id grouping key), gathers edges from ALL their
    rows, so a chain / career person shows every affiliation — incl. multiple
    companies (job changes) — from one drawer."""
    # expand to every row resolved to the same human
    row_ids = [person_id]
    if person_type == "lead_contact":
        grp = (
            await session.execute(
                text("SELECT person_id FROM lead_contacts WHERE id = :id"),
                {"id": person_id},
            )
        ).scalar()
        if grp is not None:
            sib = (
                await session.execute(
                    text("SELECT id FROM lead_contacts WHERE person_id = :g"),
                    {"g": grp},
                )
            ).all()
            row_ids = [r.id for r in sib] or [person_id]

    edges = (
        (
            await session.execute(
                text(
                    "SELECT account_type, account_id, account_name, relationship, scope "
                    "FROM contact_affiliations "
                    "WHERE person_type = :pt AND person_id = ANY(:ids)"
                ),
                {"pt": person_type, "ids": row_ids},
            )
        )
        .mappings()
        .all()
    )

    employers_raw: list[dict] = []
    explicit: list[dict] = []
    for e in edges:
        if e["relationship"] == "employed_by":
            employers_raw.append(dict(e))
        elif e["relationship"] in ("covers", "stationed_at"):
            explicit.append(dict(e))

    coverage: list[dict] = []
    seen: set = set()

    for e in explicit:
        nm = e["account_name"] or await _account_name(session, e["account_type"], e["account_id"])
        key = (e["account_type"], e["account_id"], e["account_name"])
        if key in seen:
            continue
        seen.add(key)
        coverage.append(
            {
                "account_type": e["account_type"],
                "account_id": e["account_id"],
                "name": nm,
                "via": "explicit",
            }
        )

    # Each property-side employer IS a covered property; each mgmt-co portfolio
    # employer derives its managed book. A resolved person may have several.
    derived_count = 0
    employers_out: list[dict] = []
    seen_emp: set = set()
    for emp in employers_raw:
        at = emp["account_type"]
        if at in ("existing_hotel", "potential_lead"):
            ekey = (at, emp["account_id"])
            if ekey in seen_emp:
                continue
            seen_emp.add(ekey)
            nm = emp.get("account_name") or await _account_name(session, at, emp["account_id"])
            employers_out.append(
                {"type": at, "id": emp["account_id"], "name": nm, "scope": emp.get("scope")}
            )
            ckey = (at, emp["account_id"], None)
            if ckey not in seen:
                seen.add(ckey)
                coverage.append(
                    {
                        "account_type": at,
                        "account_id": emp["account_id"],
                        "name": nm,
                        "via": "employer",
                    }
                )
        elif at == "management_company":
            ekey = ("mgmt", (emp.get("account_name") or "").lower())
            if ekey in seen_emp:
                continue
            seen_emp.add(ekey)
            employers_out.append(
                {
                    "type": "management_company",
                    "name": emp.get("account_name"),
                    "scope": emp.get("scope"),
                }
            )
            if emp.get("scope") == "portfolio":
                for d in await _managed_properties(session, emp.get("account_name")):
                    dkey = (d["account_type"], d["account_id"], None)
                    if dkey in seen:
                        continue
                    seen.add(dkey)
                    coverage.append(d)
                    derived_count += 1

    is_portfolio = any(
        e["type"] == "management_company" and e.get("scope") == "portfolio" for e in employers_out
    )

    return {
        "person_type": person_type,
        "person_id": person_id,
        "employer": employers_out[0] if employers_out else None,
        "employers": employers_out,
        "is_portfolio_buyer": is_portfolio,
        "coverage_count": len(coverage),
        "derived_portfolio_count": derived_count,
        "coverage": coverage[:_COVERAGE_CAP],
    }


async def get_coverage_for_account(
    session: AsyncSession, account_type: str, account_id: int
) -> dict[str, Any]:
    """Everyone who covers one hotel/lead: direct edges + portfolio buyers."""
    direct = (
        (
            await session.execute(
                text(
                    "SELECT person_type, person_id, relationship, scope "
                    "FROM contact_affiliations "
                    "WHERE account_type = :at AND account_id = :aid "
                    "AND relationship IN ('employed_by','stationed_at','covers')"
                ),
                {"at": account_type, "aid": account_id},
            )
        )
        .mappings()
        .all()
    )

    people: dict[tuple, dict] = {}
    for d in direct:
        people[(d["person_type"], d["person_id"])] = {
            "via": "direct" if d["relationship"] != "covers" else "covers",
            "relationship": d["relationship"],
            "scope": d["scope"],
        }

    mgmt = await _account_mgmt(session, account_type, account_id)
    if mgmt and mgmt.strip():
        portfolio = (
            (
                await session.execute(
                    text(
                        "SELECT person_type, person_id FROM contact_affiliations "
                        "WHERE account_type = 'management_company' "
                        "AND relationship = 'employed_by' AND scope = 'portfolio' "
                        "AND lower(account_name) = lower(:m)"
                    ),
                    {"m": mgmt.strip()},
                )
            )
            .mappings()
            .all()
        )
        for p in portfolio:
            people.setdefault(
                (p["person_type"], p["person_id"]),
                {"via": "management_company", "relationship": "covers", "scope": "portfolio"},
            )

    resolved: list[dict] = []
    for (pt, pid), meta in people.items():
        info = await _person_identity(session, pt, pid)
        if info:
            resolved.append({**info, **meta})

    # decision-makers and portfolio (mgmt-co) buyers surface first
    resolved.sort(
        key=lambda p: (
            0 if p.get("is_decision_maker") else 1,
            0 if p.get("via") == "management_company" else 1,
            (p.get("name") or "").lower(),
        )
    )

    return {
        "account_type": account_type,
        "account_id": account_id,
        "management_company": mgmt,
        "people_count": len(resolved),
        "people": resolved[:_COVERAGE_CAP],
    }
