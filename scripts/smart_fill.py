#!/usr/bin/env python3
"""smart_fill.py — gap-driven enrichment, from the command line.

Profiles what every eligible contact is MISSING and routes each gap to the
resolver that fills it (see app/services/smart_fill.py). Buyers, P1/P2 and
decision-makers always go first when a pass is capped.

USAGE (repo root):
    python scripts/smart_fill.py                       # audit only (default)
    python scripts/smart_fill.py --roles 50            # fill 50 missing roles
    python scripts/smart_fill.py --linkedin 100        # find 100 LinkedIn URLs
    python scripts/smart_fill.py --roles 50 --linkedin 100 --category buyer
    python scripts/smart_fill.py --roles 200 --dm-only --dry-run

WHAT EACH PASS COSTS
    --roles N      ~6 Serper queries + 1 Gemini call per contact (tier-2 deep
                   enrich). Also resolves names on nameless rows for free.
    --linkedin N   1–2 Serper queries per contact. Zero Wiza credits.

WHAT THIS SCRIPT DELIBERATELY DOES NOT SPEND
    Wiza credits. The only email gap lives in saved lead-generator contacts
    (LinkedIn-but-no-email); the audit counts them and prints the existing
    credit-guarded endpoint to run — 2 credits per found email, hard-capped
    at 10 contacts per call, never auto-fired from here.

Re-run after a role pass: newly-named contacts become eligible for the
LinkedIn pass.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def print_census(c: dict) -> None:
    print("\n── Gap census (eligible = not junk/seller/competitor/personal/")
    print("   operational, not a shared mailbox) ──────────────────────────")
    print(f"   Eligible contacts:        {c['eligible']:>7,}")
    print(f"   Missing role/title:       {c['missing_role']:>7,}   → --roles N")
    print(f"   Missing LinkedIn (named): {c['missing_linkedin']:>7,}   → --linkedin N")
    print(f"   Missing real name:        {c['missing_name']:>7,}   → role pass fills some;")
    print("                                         bulk: python resolve_names.py "
          "--after-id N --limit 50")
    print(f"   Missing phone:            {c['missing_phone']:>7,}   → (signature/vCard "
          "capture fills these as mail flows)")
    print(f"   Saved leads w/ LinkedIn,  {c['lead_email_gap']:>7,}   → existing guarded "
          "endpoint, 2 Wiza")
    print("   but NO email:                         credits per hit, max 10/call:")
    print('                                         curl -X POST "http://192.168.1.151:8000'
          '/api/contacts/bulk-enrich-email?limit=5" \\')
    print('                                              -H "X-Requested-With: '
          'XMLHttpRequest"')
    print("──────────────────────────────────────────────────────────────\n")


async def run(args) -> None:
    from app.services.smart_fill import gap_census, run_smart_fill

    if args.roles <= 0 and args.linkedin <= 0:
        print_census(await gap_census())
        print("Audit only — pass --roles N and/or --linkedin N to fill gaps.")
        return

    summary = await run_smart_fill(
        role_limit=args.roles,
        linkedin_limit=args.linkedin,
        category=args.category,
        dm_only=args.dm_only,
        dry_run=args.dry_run,
    )
    print_census(summary["census"])
    if args.dry_run:
        print(f"DRY RUN — would process: roles={summary['roles'].get('planned', 0)} "
              f"linkedin={summary['linkedin'].get('planned', 0)}. Nothing written.")
        return
    r, li = summary["roles"], summary["linkedin"]
    if r:
        print(f"Roles:    processed={r.get('processed', 0)} ok={r.get('ok', 0)} "
              f"errors={r.get('errors', 0)}")
    if li:
        print(f"LinkedIn: processed={li.get('processed', 0)} found={li.get('found', 0)} "
              f"not_found={li.get('not_found', 0)}")
    print("\nRe-run the audit to watch the gaps drain; a second --linkedin pass "
          "catches contacts the role pass just named.")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--roles", type=int, default=0,
                   help="Fill role/title for up to N contacts (tier-2 deep enrich)")
    p.add_argument("--linkedin", type=int, default=0,
                   help="Find LinkedIn URLs for up to N named contacts")
    p.add_argument("--category", type=str, default="",
                   help="Restrict passes to one category (e.g. buyer)")
    p.add_argument("--dm-only", action="store_true",
                   help="Restrict passes to decision-makers")
    p.add_argument("--dry-run", action="store_true",
                   help="Show the plan (census + candidate counts); write nothing")
    args = p.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
