"""
batch_check_status.py
====================
Runs the SAME "Check status" flow the per-contact button runs, in batch over
stale-looking contacts:

  1. enrich_contact_deep(id)      -> Phase 1+2: detect move, re-file to the new
                                     employer, flag the old one 'former'
  2. if it moved -> apply_seat_successor(session, id)  -> Phase 3: find who took
                                     the vacated seat, file a successor lead stub

It reuses those exact functions, so batch and button can never diverge.

This is the MOST EXPENSIVE action in the app (Serper + LLM + grounded successor
per contact), so it is deliberately conservative:
  - targets only contacts WORTH re-checking: real person name, real hotel org,
    NOT already flagged 'former', NOT a role inbox, NOT checked in the last
    STALE_DAYS days
  - DRY-RUN by default (just shows who WOULD be checked, no API calls, no writes)
  - --apply runs the real flow, committing per contact (resumable; safe to stop)
  - --limit caps the run; re-run to continue; a checked contact is stamped so it
    isn't re-picked next run

Run from repo root, venv active, DATABASE_URL set, workers' creds available:
    python batch_check_status.py                 # preview targets, no calls
    python batch_check_status.py --apply --limit 50
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session
from app.services.contact_freshness import select_stale_contacts

APPLY = "--apply" in sys.argv


def _arg(flag, default):
    if flag in sys.argv:
        try:
            return int(sys.argv[sys.argv.index(flag) + 1])
        except Exception:
            return default
    return default


LIMIT = _arg("--limit", 30)
STALE_DAYS = _arg("--stale-days", 365)   # default matches the scheduled job


async def main() -> None:
    async with async_session() as s:
        # SAME targeting the scheduled task uses (shared service -> no drift)
        rows = await select_stale_contacts(s, STALE_DAYS, LIMIT)

        print("=" * 72)
        print(f" BATCH CHECK-STATUS  ({'APPLY' if APPLY else 'DRY-RUN'})  "
              f"limit={LIMIT} stale>{STALE_DAYS}d")
        print(f" targets: {len(rows)}")
        print("=" * 72)
        for r in rows:
            nm = " ".join(x for x in (r["first_name"], r["last_name"]) if x) or "(name)"
            lt = r["last_touch"]
            lt_s = lt.date().isoformat() if lt else "no date"
            print(f"  {lt_s}  {r['email']:<40} {nm:<20} @ {r['organization']}")

        if not APPLY:
            print("\n  DRY-RUN — no API calls, no writes. Re-run with --apply.")
            print("  Each --apply target costs: 1 Serper + 1 LLM judge "
                  "(+ grounded successor only if moved).")
            return

        from app.services.contact_tier2_enrichment import enrich_contact_deep
        from app.services.current_employer import apply_seat_successor

        moved = same = successors = errors = 0
        for r in rows:
            cid = r["id"]
            try:
                # Phase 1+2 — detect move, re-file, flag former
                res = await enrich_contact_deep(cid)
                changed = bool(res.get("employer_changed"))
                left = bool(res.get("left_industry"))
                if changed:
                    moved += 1
                    new = res.get("current_employer") or "?"
                    print(f"  MOVED   {r['email']:<38} -> {new}")
                    # Phase 3 — fill the vacated seat
                    try:
                        suc = await apply_seat_successor(s, cid)
                        if suc.get("found") or suc.get("successor_name"):
                            successors += 1
                            print(f"          successor: {suc.get('successor_name','?')} "
                                  f"@ {suc.get('former_org','?')}")
                    except Exception as se:
                        print(f"          (successor search skipped: {se})")
                elif left:
                    moved += 1
                    print(f"  LEFT    {r['email']:<38} -> left industry "
                          f"({res.get('current_employer') or 'non-hotel'})")
                else:
                    same += 1
                # stamp as checked so it isn't re-picked next run
                await s.execute(text(
                    "UPDATE contacts SET enrichment_source='status_checked', updated_at=now() "
                    "WHERE id=:id AND enrichment_source IS DISTINCT FROM 'grounded_name'"),
                    {"id": cid})
                await s.commit()
            except Exception as e:
                errors += 1
                await s.rollback()
                print(f"  ERROR   {r['email']:<38} {e}")

        print("\n" + "=" * 72)
        print(f"  checked {len(rows)} | moved/left {moved} | still-current {same} "
              f"| successors filed {successors} | errors {errors}")
        print("  Re-run --apply to continue through the rest of the stale pool.")


if __name__ == "__main__":
    asyncio.run(main())
