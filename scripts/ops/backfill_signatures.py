"""
backfill_signatures.py
======================
Recovers FULL signature data for active contacts whose signature was never
captured. Reuses the live extractor and the same safety gauntlet as the sync,
so nothing enters that the daily sync wouldn't accept.

    python -m scripts.ops.backfill_signatures                 # DRY RUN, 50
    python -m scripts.ops.backfill_signatures --limit 500     # DRY RUN, 500
    python -m scripts.ops.backfill_signatures --limit 500 --apply

TARGETS
  Contacts that emailed us 3+ times, have has_signature=false, no title, and a
  known mailbox. The probe showed ~77% of these have a recoverable signature.

WHAT IT WRITES (only on --apply, only into EMPTY fields)
  first_name, last_name, title, organization, phone (or mobile), address,
  linkedin_url — everything the signature block offers. Never overwrites an
  existing non-empty value. Sets has_signature=true so it is not re-scanned.

SAFETY (identical to the daily sync)
  - Full 6-layer rejection: JA-org, JA-team leak, JA-sig-on-external,
    email-domain mismatch, is_real_person=false, confidence < MIN_CONFIDENCE.
  - Only fills blank fields (fill-empty) — cannot corrupt good data.
  - Dry run writes nothing and reports the hit rate + a per-field fill count.

COST: Gmail reads + one Gemini flash-lite call per found signature. No
grounding, no Serper, no Wiza. ~500 contacts is a small flash-lite batch.
"""

import argparse
import asyncio
import re
import sys
from collections import Counter

import httpx
from sqlalchemy import text

from app.database import async_session
from app.services.mailbox_discovery import list_active_mailboxes
from app.services.name_validation import is_role_inbox
from app.services.inbox_sync import (
    MIN_CONFIDENCE,
    OWN_DOMAINS,
    _domain,
    _extract_plain,
    _extract_sig_block,
    _gmail,
    _is_ja_team_leak,
    _parse_sig,
    _split_segments,
    _validate_phone,
)

SQL = """
    SELECT id, email, first_name, last_name, title, organization,
           phone, address, linkedin_url
    FROM contacts
    WHERE COALESCE(manual_category, contact_category,'') != 'junk'
      AND NOT has_signature
      AND interaction_count >= 3
      AND COALESCE(NULLIF(TRIM(title),''),'') = ''
      AND COALESCE(manual_category, contact_category,'') = 'buyer'
    ORDER BY interaction_count DESC
    LIMIT :lim
"""

# fields we can fill, mapped from parsed-sig keys
FIELDS = ["first_name", "last_name", "title", "organization", "address", "linkedin_url"]

# Two patterns: (1) role word as its own token (bounded by . _ - or ends),
# and (2) an "ap"/"ar" accounts suffix GLUED to the end of the local part
# (umiamiap, ameriparkap, uhealthap, apm) — these are shared AP inboxes.
_SHARED_LOCAL = re.compile(
    r"(^|[._-])(ap|ar|apinvoice|apinvoices|invoice|invoices|reqlogic|billing|"
    r"payables|receivables|accounting|accountspayable|adminasst|adminassistant|"
    r"asstmanager|assistantmgr|assistantmanager|frontdesk|fd|housekeeping|hk|"
    r"reservations|pm|propertymanager|productiondept|production|purchasing|"
    r"procurement|orders|sales|info|office|admin|hr|payroll|support|"
    r"marketplace|clubevents)([._-]|$)"
)
_SHARED_GLUED = re.compile(r"(ap|ar)$")   # umiamiap, ameriparkap, uhealthap


def _is_shared_local(local: str) -> bool:
    return bool(_SHARED_LOCAL.search(local) or _SHARED_GLUED.search(local))


def _accept(parsed: dict, sig_owner: str) -> bool:
    """The sync's rejection gauntlet, condensed. True = safe to apply."""
    if not parsed:
        return False
    porg = (parsed.get("organization") or "").lower()
    if any(t in porg for t in ("jauniforms", "j.a. uniforms", "ja uniforms")):
        return False
    if _is_ja_team_leak(parsed, sig_owner):
        return False
    pemail = (parsed.get("email") or "").lower().strip()
    if pemail and _domain(pemail) in OWN_DOMAINS and _domain(sig_owner) not in OWN_DOMAINS:
        return False
    if pemail and pemail != sig_owner and _domain(pemail) != _domain(sig_owner):
        return False
    if parsed.get("is_real_person") is False:
        return False
    conf = parsed.get("confidence")
    if isinstance(conf, (int, float)) and conf < MIN_CONFIDENCE:
        return False
    return True


async def recover(gmail_clients, http, email):
    """Search every active mailbox for this sender; first signature wins."""
    for gmail in gmail_clients:
        try:
            resp = gmail.users().messages().list(
                userId="me", q=f"from:{email}", maxResults=3
            ).execute()
        except Exception:
            continue
        for m in resp.get("messages", []):
            try:
                msg = gmail.users().messages().get(
                    userId="me", id=m["id"], format="full"
                ).execute()
            except Exception:
                continue
            for seg in _split_segments(_extract_plain(msg.get("payload", {}))):
                sig = _extract_sig_block(seg)
                if not sig or len(sig) < 30:
                    continue
                parsed = await _parse_sig(http, sig)
                if _accept(parsed, email):
                    return parsed
    return None


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL), {"lim": args.limit})).all()]

    print(f"\nSIGNATURE BACKFILL  ({'APPLYING' if args.apply else 'DRY RUN'})  "
          f"{len(rows)} contacts\n" + "=" * 70)

    # Build Gmail clients for the mailboxes most likely to hold external mail,
    # ordered so the busiest inboxes are tried first and we stop at the first
    # hit. Searching all 29 per contact is far too slow at scale.
    PRIORITY = [
        "sales@jauniforms.com", "salesorders@jauniforms.com",
        "orderprocessing@jauniforms.com", "customerservice@jauniforms.com",
        "salessupport@jauniforms.com", "office@jauniforms.com",
        "operations@jauniforms.com", "support@jauniforms.com",
    ]
    all_mb = list_active_mailboxes()
    ordered = [m for m in PRIORITY if m in all_mb] + [m for m in all_mb if m not in PRIORITY]
    # Cap at the busiest MAX_MAILBOXES inboxes. Nearly all external mail to JA
    # lands in sales@/orderprocessing@/customerservice@; searching all 29 per
    # contact is what made this slow. First hit wins, so order matters more
    # than coverage.
    MAX_MAILBOXES = 6
    ordered = ordered[:MAX_MAILBOXES]
    gmail_clients = []
    for mb in ordered:
        try:
            gmail_clients.append(_gmail(mb))
        except Exception:
            pass
    print(f"  searching top {len(gmail_clients)} inboxes, first hit wins\n")

    hits = 0
    fill_counts = Counter()
    to_write = []
    skipped_role = 0
    async with httpx.AsyncClient(timeout=30) as http:
        for r in rows:
            # Role inboxes (accountspayable@, purchasing@, admin@) are shared by
            # several people. A signature found in one names WHOEVER wrote that
            # message, not the mailbox owner — so we must not attach a personal
            # identity to a shared address. Skip them, same rule as the sync.
            local = r["email"].split("@")[0].lower()
            # Broadened shared/role detection: is_role_inbox misses variants like
            # umiamiap@, apinvoice@, reqlogic@, adminasst@, asstmanager@, fd@,
            # hk@, pm@, productiondept@. For a shared box we may still learn the
            # property's address/phone, but must NOT attach one person's name.
            shared = is_role_inbox(r["email"]) or _is_shared_local(local)
            print(f"    ...checked {r['email'][:40]}", flush=True)
            parsed = await recover(gmail_clients, http, r["email"])
            if parsed and shared:
                parsed = dict(parsed)
                parsed["first_name"] = None
                parsed["last_name"] = None
            if not parsed:
                continue
            hits += 1

            updates = {}
            for f in FIELDS:
                if parsed.get(f) and not (r[f] or "").strip():
                    updates[f] = parsed[f]
                    fill_counts[f] += 1
            ph = _validate_phone(parsed.get("phone")) or _validate_phone(parsed.get("mobile"))
            if ph and not (r["phone"] or "").strip():
                updates["phone"] = ph
                fill_counts["phone"] += 1
            if updates:
                updates["has_signature"] = True
                to_write.append((r["id"], r["email"], updates))

    checked = len(rows) - skipped_role
    if skipped_role:
        print(f"  skipped {skipped_role} role inboxes (shared — no personal identity)")
    print(f"  signatures recovered : {hits}  ({100.0*hits/checked:.0f}% of {checked})" if checked else "")
    print(f"  contacts with new data: {len(to_write)}")
    print("\n  fields that would be filled:")
    for f, n in fill_counts.most_common():
        print(f"    {f:<16} {n:>5}")

    print("\n  sample:")
    for _id, em, u in to_write[:15]:
        got = ", ".join(f"{k}={str(v)[:18]}" for k, v in u.items() if k != "has_signature")
        print(f"    {em[:34]:<36} {got}")

    if not args.apply:
        print("\n  Dry run — nothing written. Re-run with --apply.\n")
        return 0

    async with async_session() as s:
        for cid, _em, u in to_write:
            sets, params = [], {"id": cid}
            for k, v in u.items():
                sets.append(f"{k} = :{k}")
                params[k] = v
            sets.append("updated_at = NOW()")
            await s.execute(text(f"UPDATE contacts SET {', '.join(sets)} WHERE id = :id"), params)
        await s.commit()
    print(f"\n  WROTE signature data to {len(to_write)} contacts.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
