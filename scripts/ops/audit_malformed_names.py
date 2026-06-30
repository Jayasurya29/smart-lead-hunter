"""
audit_malformed_names.py
======================
Cleans malformed contact display names, bucketed by type so each gets the right
action. A blunt "null all bad names" would destroy recoverable names and bury
real client inboxes -- so this sorts first, shows you every bucket, and acts
only on --apply.

Buckets + action:
  RECOVERABLE  - a real name buried in junk ('Charles Chen-12311',
                 'xx-Scott Williams Disabled 6.16.23') -> salvage the clean name
  STORE_INBOX  - client store/numbered inboxes (sedof23, Sedanos21) -> clear the
                 junk name, KEEP the contact (Sedano's is a top client)
  DOMAIN_NAME  - the name is just a website (STEAK48.COM, cbayresort.com)
                 -> clear name, keep contact (real inbox, bad name)
  ROLE_INBOX   - role/system inboxes (Purchasing2, '500 Brickell Manager',
                 '0323_AM_Email') -> clear name, keep contact
  EVENT_SPAM   - event/vendor blasts ('The Hospitality Show 2025', 'XPress
                 Leads...') -> manual_category='junk' (trash)
  PERSONAL     - personal contacts ('Tennis - Gricel Botos (4.5)')
                 -> manual_category='personal'

    python -m scripts.ops.audit_malformed_names              # dry-run, show buckets
    python -m scripts.ops.audit_malformed_names --apply       # apply all buckets
    python -m scripts.ops.audit_malformed_names --apply --only RECOVERABLE,STORE_INBOX

Read-only without --apply. Run from repo root, venv active, DATABASE_URL set.
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

APPLY = "--apply" in sys.argv
ONLY = None
if "--only" in sys.argv:
    try:
        ONLY = {b.strip().upper() for b in sys.argv[sys.argv.index("--only") + 1].split(",")}
    except Exception:
        ONLY = None

_DOMAIN_RE = re.compile(r"\.(com|net|org|edu|io|co|us|gov)\b", re.I)
_EVENT_KW = ("hospitality show", "xpress leads", "lead vendor", "conference",
             "expo", "webinar", "summit", "official lead", "register", "newsletter")
_PERSONAL_KW = ("tennis", "pickleball", "golf", "soccer", "coach ")
_ROLE_KW = ("manager", "director", "coordinator", "supervisor", "housekeeping",
            "concierge", "front desk", "reservations", "purchasing", "accounting",
            "general manager", "property manager", "resort manager", "am_email",
            "operations")


def _classify(name: str, email: str):
    """Return (bucket, salvaged_name_or_None)."""
    n = (name or "").strip()
    low = n.lower()
    dom = (email or "").split("@")[-1].lower()

    # EVENT_SPAM
    if any(k in low for k in _EVENT_KW):
        return "EVENT_SPAM", None
    # PERSONAL
    if any(k in low for k in _PERSONAL_KW):
        return "PERSONAL", None
    # STORE_INBOX: sedano's numbered inboxes (sedof23 / sedanos21) or name==local
    if "sedanos.com" in dom or re.fullmatch(r"sed(of|anos)\d+", low):
        return "STORE_INBOX", None
    # RECOVERABLE: a real 'First Last' is present, with junk around it
    salv = _salvage_name(n)
    if salv:
        return "RECOVERABLE", salv
    # DOMAIN_NAME: the name is a website
    if _DOMAIN_RE.search(low) and "@" not in n:
        return "DOMAIN_NAME", None
    # ROLE_INBOX: role words, or name mirrors a role-ish local part, or digits-only-ish
    if any(k in low for k in _ROLE_KW):
        return "ROLE_INBOX", None
    if re.fullmatch(r"[a-z]+\d+", low) or re.fullmatch(r"\d.*", low):
        return "ROLE_INBOX", None
    # default: treat unrecognized digit/domain junk as ROLE_INBOX (clear, keep)
    return "ROLE_INBOX", None


def _salvage_name(n: str):
    """Extract a clean 'First Last' from a junk-wrapped name, else None.
    'Charles Chen-12311' -> Charles Chen ; 'xx-Scott Williams Disabled 6.16.23'
    -> Scott Williams ; 'Forrest Christopher (bos3cxf)' -> Forrest Christopher."""
    if not n:
        return None
    s = n
    s = re.sub(r"^xx[-\s]+", "", s, flags=re.I)        # leading xx-
    s = re.sub(r"\(.*?\)", "", s)                        # (bos3cxf)
    s = re.sub(r"[-/]\s*\d.*$", "", s)                   # -12311, - 6.16.23 tails
    s = re.sub(r"\b(disabled|inactive|former|do not contact)\b.*$", "", s, flags=re.I)
    s = re.sub(r"\s+\d[\d.]*\s*$", "", s)                # trailing rating/number
    s = re.sub(r"\s{2,}", " ", s).strip(" -|.,")
    toks = [t for t in s.split() if t]
    # must look like a real 2-token name, all-alpha, no leftover digits/domains
    if len(toks) == 2 and all(t.isalpha() and len(t) >= 2 for t in toks):
        if not _DOMAIN_RE.search(s.lower()):
            return f"{toks[0]} {toks[1]}"
    return None


async def main() -> None:
    async with async_session() as s:
        rows = (await s.execute(text(
            "SELECT id, display_name, first_name, last_name, email "
            "FROM contacts "
            "WHERE display_name ~ '[0-9]' OR display_name ~* '\\.(com|net|org|edu)' "
            "ORDER BY id"
        ))).mappings().all()

        buckets = {k: [] for k in
                   ("RECOVERABLE", "STORE_INBOX", "DOMAIN_NAME", "ROLE_INBOX",
                    "EVENT_SPAM", "PERSONAL")}
        for r in rows:
            b, salv = _classify(r["display_name"], r["email"])
            buckets[b].append((r, salv))

        print("=" * 80)
        print(f" MALFORMED NAMES: {len(rows)} contacts")
        print("=" * 80)
        for b, items in buckets.items():
            action = {
                "RECOVERABLE": "salvage clean name",
                "STORE_INBOX": "clear name, KEEP (client store inbox)",
                "DOMAIN_NAME": "clear name, keep (real inbox, bad name)",
                "ROLE_INBOX": "clear name, keep (role/system inbox)",
                "EVENT_SPAM": "trash (manual_category=junk)",
                "PERSONAL": "personal (manual_category=personal)",
            }[b]
            print(f"\n {b} ({len(items)}) -> {action}")
            for r, salv in items[:12]:
                extra = f"  => {salv}" if salv else ""
                print(f"   #{r['id']:<6} {(r['display_name'] or '')[:42]:<42} {r['email']}{extra}")
            if len(items) > 12:
                print(f"   ... +{len(items)-12} more")

        if not APPLY:
            print("\n  DRY-RUN. --apply to act on all buckets, "
                  "or --apply --only RECOVERABLE,STORE_INBOX to limit.")
            return

        def _wanted(b):
            return ONLY is None or b in ONLY

        clear_ids, salvage, junk_ids, personal_ids = [], [], [], []
        for b, items in buckets.items():
            if not _wanted(b):
                continue
            if b == "RECOVERABLE":
                salvage += [(r["id"], salv) for r, salv in items]
            elif b in ("STORE_INBOX", "DOMAIN_NAME", "ROLE_INBOX"):
                clear_ids += [r["id"] for r, _ in items]
            elif b == "EVENT_SPAM":
                junk_ids += [r["id"] for r, _ in items]
            elif b == "PERSONAL":
                personal_ids += [r["id"] for r, _ in items]

        if clear_ids:
            await s.execute(text(
                "UPDATE contacts SET first_name=NULL, last_name=NULL, display_name=NULL "
                "WHERE id = ANY(:ids)"), {"ids": clear_ids})
        for cid, salv in salvage:
            fn, ln = salv.split(" ", 1)
            await s.execute(text(
                "UPDATE contacts SET first_name=:fn, last_name=:ln, display_name=:dn "
                "WHERE id=:id"), {"fn": fn, "ln": ln, "dn": salv, "id": cid})
        if junk_ids:
            await s.execute(text(
                "UPDATE contacts SET manual_category='junk' WHERE id = ANY(:ids)"),
                {"ids": junk_ids})
        if personal_ids:
            await s.execute(text(
                "UPDATE contacts SET manual_category='personal' WHERE id = ANY(:ids)"),
                {"ids": personal_ids})
        await s.commit()
        print(f"\n  DONE — cleared {len(clear_ids)} names, salvaged {len(salvage)}, "
              f"trashed {len(junk_ids)}, marked {len(personal_ids)} personal.")


if __name__ == "__main__":
    asyncio.run(main())
