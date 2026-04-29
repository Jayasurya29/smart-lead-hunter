"""
Dedup expired potential_leads using Gemini (LLM clustering)
============================================================

Replaces the previous regex-based clustering. Regex was either too
conservative (missed obvious dups like "Vanderpump" + "Vanderpump
Cromwell") or too aggressive (false-positives like clustering Bally's
with W Hotel Las Vegas).

Gemini handles this far better in one shot — given a list of hotel
names + locations, it returns clusters of "same physical hotel" with
high precision because it actually understands "Hotel Indigo" and
"Kimpton" are different brands at the same location.

Three-stage operation:

  Stage 1 — call Gemini, write plan
      python scripts\\dedup_expired_leads.py --plan
      python scripts\\dedup_expired_leads.py --plan --force   # overwrite

      Sends all 113 expired leads to Gemini in ONE call. Gemini returns
      JSON: [{cluster_id, canonical_lead_id, member_lead_ids, reason}, ...]
      Writes scripts\\expired_dedup_plan.txt with KEEP/DELETE actions.

  Stage 2 — review the plan
      Open scripts\\expired_dedup_plan.txt. Each cluster shows the
      LLM's reasoning. Edit any KEEP/DELETE you disagree with.

  Stage 3 — apply
      python scripts\\dedup_expired_leads.py --apply --dry-run
      python scripts\\dedup_expired_leads.py --apply

      DELETE rows hard-delete the lead + its contacts.
      KEEP rows go through transfer_lead() which:
        - finds an existing_hotels match (cross-table dedup)
        - merges or creates a new EH row
        - re-parents contacts via FK flip
        - hard-deletes the source lead

Cost
----
~1 Gemini Flash call. About $0.005. Takes ~30 seconds for 113 leads.

Created: 2026-04-28 (replaces regex-based version)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import select, delete, func  # noqa: E402
from app.database import async_session  # noqa: E402
from app.models.potential_lead import PotentialLead  # noqa: E402
from app.models.lead_contact import LeadContact  # noqa: E402
from app.services.lead_transfer import transfer_leads_by_ids  # noqa: E402
from app.services.lead_data_enrichment import _call_gemini  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


PLAN_FILE = Path(__file__).resolve().parent / "expired_dedup_plan.txt"


# ─────────────────────────────────────────────────────────────────────────────
# Gemini prompt + schema
# ─────────────────────────────────────────────────────────────────────────────

_PROMPT_TEMPLATE = """You are deduplicating a list of hotel records.

Each record has an id, name, location, brand, rooms, and tier. Group
records that represent the SAME PHYSICAL HOTEL into clusters. Different
brands at the same address are NOT duplicates — Hotel Indigo and Kimpton
in Turks & Caicos are different properties even though both are IHG.

A duplicate cluster requires BOTH:
  - The names refer to the same property (small naming variations like
    "ABC Resort" vs "ABC Resort & Spa" vs "ABC" are fine)
  - Same physical location (same city OR same beach/area)

Conservative rule: if you're unsure, DON'T cluster. A few duplicates
making it through is better than wrongly merging two distinct hotels.

Return JSON with this exact shape:
{{
  "clusters": [
    {{
      "canonical_id": <integer>,
      "member_ids": [<integer>, <integer>, ...],
      "reason": "<one short sentence why these are the same hotel>"
    }}
  ]
}}

Rules:
- Only return clusters with 2+ members. Singletons are skipped.
- canonical_id must be the most-enriched member (most contacts, most
  fields populated). It MUST appear in member_ids too.
- Every id must appear in at most one cluster.

HOTEL RECORDS:
{records}

Return JSON only:"""


_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "clusters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "canonical_id": {"type": "integer"},
                    "member_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "reason": {"type": "string"},
                },
                "required": ["canonical_id", "member_ids", "reason"],
            },
        },
    },
    "required": ["clusters"],
}


def _format_record(lead: PotentialLead, contact_count: int) -> str:
    """One-line representation for the prompt."""
    loc_parts = [
        lead.city, lead.state, lead.country,
    ]
    loc = ", ".join([p for p in loc_parts if p]) or "?"
    bits = []
    if lead.brand:
        bits.append(f"brand={lead.brand}")
    if lead.brand_tier and lead.brand_tier != "unknown":
        bits.append(lead.brand_tier.replace("tier", "T"))
    if lead.room_count:
        bits.append(f"{lead.room_count}rm")
    if lead.hotel_type:
        bits.append(lead.hotel_type)
    if contact_count:
        bits.append(f"{contact_count} contacts")
    extra = f"  [{', '.join(bits)}]" if bits else ""
    return f"  id={lead.id:<5}  name={lead.hotel_name!r:<60}  loc={loc!r}{extra}"


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — generate plan via Gemini
# ─────────────────────────────────────────────────────────────────────────────

async def _generate_plan(force: bool) -> int:
    if PLAN_FILE.exists() and not force:
        print(f"⚠ Plan file already exists: {PLAN_FILE}")
        print("  Re-run with --force to overwrite.")
        return 1

    print()
    print("Querying expired leads + contact counts...")

    async with async_session() as session:
        r = await session.execute(
            select(PotentialLead)
            .where(PotentialLead.status == "expired")
            .order_by(PotentialLead.id)
        )
        leads = list(r.scalars().all())
        if not leads:
            print("No expired leads found.")
            return 0

        cc_q = await session.execute(
            select(LeadContact.lead_id, func.count(LeadContact.id))
            .group_by(LeadContact.lead_id)
        )
        contact_counts = {row[0]: row[1] for row in cc_q}

    n = len(leads)
    print(f"Found {n} expired lead(s). Calling Gemini for clustering...")

    # Build the prompt
    records_text = "\n".join(
        _format_record(lead, contact_counts.get(lead.id, 0))
        for lead in leads
    )
    prompt = _PROMPT_TEMPLATE.format(records=records_text)

    # Call Gemini
    raw = await _call_gemini(
        prompt, temperature=0.0, response_schema=_RESPONSE_SCHEMA,
    )
    if not raw:
        print("✗ Gemini call failed (returned None). Check Vertex AI credentials.")
        return 2

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"✗ Gemini returned invalid JSON: {e}")
        print(f"Raw response (first 500 chars): {raw[:500]}")
        return 2

    clusters = parsed.get("clusters", [])

    # ── Validate clusters ──
    valid_ids = {lead.id for lead in leads}
    cleaned: list[dict] = []
    seen_members: set[int] = set()
    rejected = 0

    for cluster in clusters:
        canon = cluster.get("canonical_id")
        members = cluster.get("member_ids") or []
        reason = cluster.get("reason") or ""

        # Sanity checks
        if not isinstance(canon, int) or not isinstance(members, list):
            rejected += 1
            continue
        if len(members) < 2:
            rejected += 1
            continue
        if canon not in members:
            members = [canon] + members
        # Drop unknown IDs
        members = [m for m in members if m in valid_ids]
        if len(members) < 2:
            rejected += 1
            continue
        # Drop overlap with already-seen members (each id one cluster)
        members = [m for m in members if m not in seen_members]
        if len(members) < 2:
            rejected += 1
            continue
        if canon not in members:
            canon = members[0]
        for m in members:
            seen_members.add(m)
        cleaned.append({
            "canonical_id": canon,
            "member_ids": members,
            "reason": reason,
        })

    if rejected:
        print(f"  ⚠ Rejected {rejected} malformed/conflicting cluster(s) from response.")

    # ── Build the plan ──
    leads_by_id = {lead.id: lead for lead in leads}
    contact_counts_by_id = contact_counts

    cluster_member_ids: set[int] = set()
    for c in cleaned:
        cluster_member_ids.update(c["member_ids"])

    singleton_leads = [
        lead for lead in leads if lead.id not in cluster_member_ids
    ]

    lines: list[str] = []
    lines.append("# Expired leads dedup plan (Gemini-generated)")
    lines.append(f"# {n} expired leads scanned, {len(cleaned)} duplicate cluster(s) found")
    lines.append("# ")
    lines.append("# For each lead one of these actions:")
    lines.append("#   KEEP    <id>  — will be transferred to existing_hotels")
    lines.append("#   DELETE  <id>  — will be hard-deleted (and its contacts)")
    lines.append("# ")
    lines.append("# Override any cluster decision by swapping KEEP and DELETE.")
    lines.append("# Then run: python scripts\\dedup_expired_leads.py --apply")
    lines.append("")
    lines.append("# ============================================================")
    lines.append(f"# DUPLICATE CLUSTERS ({len(cleaned)}) — Gemini-detected")
    lines.append("# ============================================================")
    lines.append("")

    keep_count = 0
    delete_count = 0

    for i, c in enumerate(cleaned, 1):
        canon = c["canonical_id"]
        members = c["member_ids"]
        reason = c["reason"]

        canon_lead = leads_by_id[canon]
        cluster_label = (canon_lead.hotel_name or "?")[:60]
        lines.append(f"# --- Cluster {i}: {cluster_label} ---")
        lines.append(f"# Reason: {reason}")

        # KEEP the canonical
        cc = contact_counts_by_id.get(canon, 0)
        ann = []
        if cc:
            ann.append(f"{cc} contact(s)")
        if canon_lead.room_count:
            ann.append(f"{canon_lead.room_count}rm")
        if canon_lead.hotel_type:
            ann.append(canon_lead.hotel_type)
        if canon_lead.brand_tier and canon_lead.brand_tier != "unknown":
            ann.append(canon_lead.brand_tier.replace("tier", "T").replace("_", " "))
        ann_str = f"[{', '.join(ann)}]" if ann else ""
        lines.append(
            f"KEEP   {canon:>4}    "
            f"# {(canon_lead.hotel_name or '?')[:50]}  {ann_str}"
        )
        keep_count += 1

        # DELETE the rest
        for m in members:
            if m == canon:
                continue
            ml = leads_by_id[m]
            mcc = contact_counts_by_id.get(m, 0)
            ann = []
            if mcc:
                ann.append(f"{mcc} contact(s)")
            if ml.room_count:
                ann.append(f"{ml.room_count}rm")
            if ml.hotel_type:
                ann.append(ml.hotel_type)
            if ml.brand_tier and ml.brand_tier != "unknown":
                ann.append(ml.brand_tier.replace("tier", "T").replace("_", " "))
            ann_str = f"[{', '.join(ann)}]" if ann else ""
            lines.append(
                f"DELETE {m:>4}    "
                f"# {(ml.hotel_name or '?')[:50]}  {ann_str}  ← dup of #{canon}"
            )
            delete_count += 1
        lines.append("")

    lines.append("# ============================================================")
    lines.append(f"# SINGLETONS — no cluster-mates ({len(singleton_leads)})")
    lines.append("# ============================================================")
    lines.append("")
    for lead in singleton_leads:
        cc = contact_counts_by_id.get(lead.id, 0)
        ann = []
        if cc:
            ann.append(f"{cc} contact(s)")
        if lead.room_count:
            ann.append(f"{lead.room_count}rm")
        if lead.hotel_type:
            ann.append(lead.hotel_type)
        if lead.brand_tier and lead.brand_tier != "unknown":
            ann.append(lead.brand_tier.replace("tier", "T").replace("_", " "))
        ann_str = f"[{', '.join(ann)}]" if ann else ""
        lines.append(
            f"KEEP   {lead.id:>4}    "
            f"# {(lead.hotel_name or '?')[:50]}  {ann_str}"
        )
        keep_count += 1

    PLAN_FILE.write_text("\n".join(lines), encoding="utf-8")

    print()
    print(f"✓ Plan written to: {PLAN_FILE}")
    print(f"  Clusters detected: {len(cleaned)}")
    print(f"  KEEP:    {keep_count}")
    print(f"  DELETE:  {delete_count}")
    print()
    print("Next steps:")
    print(f"  1. Open and review: notepad {PLAN_FILE}")
    print(f"  2. Preview: python scripts\\dedup_expired_leads.py --apply --dry-run")
    print(f"  3. Apply:   python scripts\\dedup_expired_leads.py --apply")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — apply plan
# ─────────────────────────────────────────────────────────────────────────────

def _parse_plan() -> tuple[list[int], list[int], list[str]]:
    """Read plan file. Returns (keep_ids, delete_ids, errors)."""
    if not PLAN_FILE.exists():
        return [], [], [f"Plan file not found: {PLAN_FILE}. Run --plan first."]

    keep_ids: list[int] = []
    delete_ids: list[int] = []
    errors: list[str] = []

    for lineno, raw in enumerate(
        PLAN_FILE.read_text(encoding="utf-8").splitlines(), 1
    ):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            errors.append(f"Line {lineno}: malformed: {raw!r}")
            continue
        action = parts[0].upper()
        try:
            lid = int(parts[1])
        except ValueError:
            errors.append(f"Line {lineno}: not an integer ID: {parts[1]!r}")
            continue
        if action == "KEEP":
            keep_ids.append(lid)
        elif action == "DELETE":
            delete_ids.append(lid)
        else:
            errors.append(
                f"Line {lineno}: unknown action {action!r} "
                f"(expected KEEP or DELETE)"
            )

    overlap = set(keep_ids) & set(delete_ids)
    if overlap:
        errors.append(
            f"IDs appear as both KEEP and DELETE: {sorted(overlap)}"
        )

    return keep_ids, delete_ids, errors


async def _apply_plan(dry_run: bool) -> int:
    keep_ids, delete_ids, errors = _parse_plan()

    if errors:
        print("Plan file has errors:")
        for e in errors:
            print(f"  ✗ {e}")
        return 2

    print()
    print(f"Plan loaded from: {PLAN_FILE}")
    print(f"  KEEP (will transfer): {len(keep_ids)}")
    print(f"  DELETE (will purge):  {len(delete_ids)}")
    print()

    if dry_run:
        print("DRY RUN — no changes will be written")
        print("=" * 70)
        async with async_session() as s:
            if delete_ids:
                print()
                print(f"WOULD DELETE {len(delete_ids)} lead(s):")
                for lid in delete_ids:
                    r = await s.execute(
                        select(PotentialLead)
                        .where(PotentialLead.id == lid)
                    )
                    lead = r.scalar_one_or_none()
                    if lead:
                        cc = await s.execute(
                            select(func.count(LeadContact.id))
                            .where(LeadContact.lead_id == lid)
                        )
                        nc = cc.scalar() or 0
                        print(
                            f"  ✗ #{lid:>4}  "
                            f"{(lead.hotel_name or '?')[:60]}  "
                            f"({nc} contact(s))"
                        )
                    else:
                        print(f"  ⏭ #{lid:>4}  not found in DB")

            if keep_ids:
                print()
                print(f"WOULD TRANSFER {len(keep_ids)} lead(s):")
                from app.services.lead_transfer import _find_existing_hotel_match
                for lid in keep_ids:
                    r = await s.execute(
                        select(PotentialLead)
                        .where(PotentialLead.id == lid)
                    )
                    lead = r.scalar_one_or_none()
                    if not lead:
                        print(f"  ⏭ #{lid:>4}  not found in DB")
                        continue
                    match = await _find_existing_hotel_match(lead, s)
                    verb = (
                        f"MERGE into EH#{match.id}"
                        if match else "CREATE new EH"
                    )
                    print(
                        f"  ✓ #{lid:>4}  "
                        f"{(lead.hotel_name or '?')[:50]:<50}  → {verb}"
                    )
        print()
        print("Re-run without --dry-run to apply.")
        return 0

    # ── REAL APPLY ──
    print("APPLYING — writing to database")
    print("=" * 70)

    deleted_leads = 0
    deleted_contacts = 0
    if delete_ids:
        async with async_session() as s:
            for lid in delete_ids:
                r = await s.execute(
                    delete(LeadContact)
                    .where(LeadContact.lead_id == lid)
                )
                deleted_contacts += r.rowcount or 0
            r2 = await s.execute(
                delete(PotentialLead)
                .where(PotentialLead.id.in_(delete_ids))
            )
            deleted_leads = r2.rowcount or 0
            await s.commit()
        print(
            f"  Deleted {deleted_leads} lead(s) and "
            f"{deleted_contacts} contact(s)"
        )

    transfer_result = {
        "transferred": 0, "merged": 0, "errors": 0,
        "not_found": 0, "contacts_migrated": 0,
    }
    if keep_ids:
        print()
        print(f"  Transferring {len(keep_ids)} lead(s)...")
        async with async_session() as s:
            transfer_result = await transfer_leads_by_ids(keep_ids, s)
        for r in transfer_result["results"]:
            status = r["status"]
            lid = r["lead_id"]
            eh_id = r.get("existing_hotel_id")
            score = r.get("score")
            if status == "transferred":
                print(f"  ✓ #{lid:>4} → EH#{eh_id:<5} score={score}")
            elif status == "merged":
                print(
                    f"  ⇄ #{lid:>4} merged into EH#{eh_id} score={score}"
                )
            elif status == "error":
                print(
                    f"  ✗ #{lid:>4} ERROR: {r.get('reason', '')}"
                )

    print()
    print("─" * 70)
    print(
        f"  Deleted (purged):              {deleted_leads:>4} leads, "
        f"{deleted_contacts} contact(s)"
    )
    print(f"  Transferred (new EH rows):     {transfer_result['transferred']:>4}")
    print(f"  Merged (into existing EH):     {transfer_result['merged']:>4}")
    print(f"  Errors:                        {transfer_result['errors']:>4}")
    print(f"  Contacts re-parented:          {transfer_result['contacts_migrated']:>4}")
    print("─" * 70)
    print()
    print("✓ Done. Review the Existing Hotels Pipeline tab.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--plan",  action="store_true",
                   help="Generate the dedup plan via Gemini")
    g.add_argument("--apply", action="store_true",
                   help="Execute the dedup plan")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing plan file (only for --plan)")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview without writing (only for --apply)")
    args = p.parse_args()

    if args.plan:
        return await _generate_plan(force=args.force)
    return await _apply_plan(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
