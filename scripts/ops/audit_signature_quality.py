"""
audit_signature_quality.py
==========================
Read-only. Checks whether signature parsing is producing GOOD names, by
comparing the parsed name against the email address it was attached to.

    python -m scripts.ops.audit_signature_quality

The sig extractor takes the last 30 lines of each message segment and asks
Gemini to pull a name/title/org. This measures the result quality without
re-parsing anything — it uses what is already stored.

CHECKS
  1. Contacts WITH a signature — does the parsed name actually match the email?
     A signature that produced "Maria Gonzalez" on mgonzalez@ = good.
     A signature that produced "Front Desk" or a name sharing nothing with the
     address = the extractor grabbed the wrong text.
  2. How many contacts got their name FROM a signature vs other sources.
  3. Signatures that produced a role-word name (grabbed a department line, not
     a person).
"""

import asyncio
import re
import sys
from collections import Counter

from sqlalchemy import text

from app.database import async_session

try:
    from app.services.name_validation import name_fits_email
except Exception:
    name_fits_email = None

ROLE_WORD = re.compile(
    r"front desk|reception|housekeep|concierge|manager|director|department|"
    r"reservation|sales|accounting|purchasing|team|office|desk|admin", re.I)

SQL = """
    SELECT email, first_name, last_name, display_name, org_source, has_signature
    FROM contacts
    WHERE email LIKE '%@%'
      AND COALESCE(manual_category, contact_category,'') != 'junk'
"""


async def main() -> int:
    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL))).all()]
    total = len(rows)

    with_sig = [r for r in rows if r["has_signature"]]
    print(f"\nSIGNATURE QUALITY  ({total:,} non-junk contacts)\n" + "=" * 66)
    print(f"\n  have a parsed signature : {len(with_sig):,}  ({100.0*len(with_sig)/total:.0f}%)")

    # name source breakdown
    print("\n  where the NAME came from (org_source as proxy):")
    for k, v in Counter(r["org_source"] or "(none)" for r in rows).most_common():
        print(f"    {k:<18} {v:>7,}")

    # role-word names = extractor grabbed a department line
    roley = [r for r in with_sig
             if ROLE_WORD.search(f"{r['first_name'] or ''} {r['last_name'] or ''} {r['display_name'] or ''}")]
    print(f"\n  signatures that produced a ROLE-WORD name (suspect): {len(roley)}")
    for r in roley[:12]:
        nm = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip() or r["display_name"]
        print(f"     {r['email'][:38]:<40} -> {nm!r}")

    # name-vs-email mismatch among signature contacts
    if name_fits_email:
        mismatch = []
        for r in with_sig:
            v = name_fits_email(r["first_name"], r["last_name"], r["display_name"], r["email"])
            if v.code == "MISMATCH":
                mismatch.append(r)
        print(f"\n  signature names that DON'T match their email: {len(mismatch)}")
        print(f"  ({100.0*len(mismatch)/len(with_sig):.1f}% of signature contacts — lower is better)")
        for r in mismatch[:12]:
            nm = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip() or r["display_name"]
            print(f"     {r['email'][:38]:<40} -> {nm!r}")
    else:
        print("\n  (name_fits_email unavailable — skipped mismatch check)")

    print("\n" + "=" * 66)
    print("  If role-word and mismatch counts are LOW, extraction is working well")
    print("  despite the crude last-30-lines rule. If HIGH, the rule is grabbing")
    print("  the wrong text and tightening it would help.")
    print("  Read-only.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
