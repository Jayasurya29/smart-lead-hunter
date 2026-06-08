"""Names status (2026-06-05): how much of the contacts table has a real
human name vs derived/empty — and where to resume the resolve sweep.

Buckets per row:
  REAL     — display name looks like a person (space-separated words,
             not derived from the email local part)
  SHARED   — role mailbox (is_shared_mailbox flag, or detected by the
             same classifier as fix_shared_mailboxes.py)
  DERIVED  — display name is just the email local part dressed up
  EMPTY    — no name at all

Run:
    python names_status.py            # counts + resume hint
    python names_status.py --list     # also print the unresolved rows
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

_CLEAN = re.compile(r"[^a-z0-9]")

ROLE_KEYS = {
    "ap", "ar", "accountspayable", "payables", "accounting", "payroll",
    "billing", "invoices", "csr", "customerservice", "service", "info",
    "contact", "hello", "office", "admin", "sales", "orders", "order",
    "purchasing", "procurement", "receiving", "warehouse", "frontdesk",
    "frontdeskmanager", "frontoffice", "reservations", "reservation",
    "concierge", "housekeeping", "engineering", "security", "hr",
    "humanresources", "careers", "jobs", "marketing", "events",
    "banquets", "catering", "spa", "press", "media", "pr", "support",
    "help", "gm",
}
ROLE_SUFFIXES = (
    "procurement", "purchasing", "orders", "reservations", "frontdesk",
    "accounting", "accountspayable", "payables", "sales", "service",
    "billing", "marketing", "events", "info",
)


def is_role_local(local: str) -> bool:
    key = _CLEAN.sub("", local.lower())
    if key in ROLE_KEYS:
        return True
    return any(key.endswith(s) and len(key) > len(s) for s in ROLE_SUFFIXES)


async def main(show_list: bool) -> None:
    async with async_session() as db:
        has_flag = bool(
            (
                await db.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_name='contacts' AND column_name='is_shared_mailbox'"
                    )
                )
            ).scalar()
        )
        cols = "id, email, display_name, first_name, last_name"
        if has_flag:
            cols += ", is_shared_mailbox"
        rows = (
            await db.execute(text(f"SELECT {cols} FROM contacts ORDER BY id"))
        ).mappings().all()

    real, shared, derived, empty = [], [], [], []
    for r in rows:
        email = (r["email"] or "").lower()
        local = email.split("@", 1)[0] if "@" in email else ""
        disp = (r["display_name"] or "").strip()
        first = (r["first_name"] or "").strip()
        last = (r["last_name"] or "").strip()

        if (has_flag and r.get("is_shared_mailbox")) or (local and is_role_local(local)):
            shared.append(r)
            continue
        if not disp and not first and not last:
            empty.append(r)
            continue
        name_key = _CLEAN.sub("", (disp or (first + last)).lower())
        if local and name_key == _CLEAN.sub("", local.lower()):
            derived.append(r)
            continue
        if first and last:
            real.append(r)
        elif " " in disp:
            real.append(r)
        else:
            derived.append(r)  # single token that isn't the local part — still not a person name

    total = len(rows)
    print(f"contacts total:        {total}")
    print(f"  REAL person names:   {len(real):>5}  ({len(real) * 100 // max(total, 1)}%)")
    print(f"  SHARED role inboxes: {len(shared):>5}  (run fix_shared_mailboxes if unflagged)")
    print(f"  DERIVED from email:  {len(derived):>5}  <- resolve_names targets")
    print(f"  EMPTY no name:       {len(empty):>5}  <- resolve_names targets")

    unresolved = sorted(derived + empty, key=lambda r: r["id"])
    if unresolved:
        print(f"\nresume point: lowest unresolved id = {unresolved[0]['id']}, "
              f"highest = {unresolved[-1]['id']}")
        print(f"e.g.  python resolve_names.py --after-id {unresolved[0]['id'] - 1} --limit 50")
    if show_list:
        print("\nunresolved rows:")
        for r in unresolved:
            print(f"  #{r['id']:>5}  {r['email']:<45} '{r['display_name'] or ''}'")


if __name__ == "__main__":
    asyncio.run(main(show_list="--list" in sys.argv))
