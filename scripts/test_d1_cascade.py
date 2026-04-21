"""
D1 CASCADE TEST HARNESS
========================
Dry-run the iterative researcher against 4 hand-picked leads to verify that
the D1 changes to iterative_researcher.py behave as intended:

  1. Cascade fires on HOT/URGENT/WARM leads with no GM
  2. Cascade does NOT fire on COOL/EXPIRED leads
  3. Cascade does NOT fire on leads where a GM IS found
  4. Task Force GM query only runs on Marriott-family brands
  5. Iter 6 reweights corporate/regional VPs to P1 when cascade fired

The script does NOT write to the database. It loads each lead, runs the
researcher in memory, captures logs, prints a summary, and discards state.

Usage (from repo root):
  python scripts/test_d1_cascade.py             # default test set
  python scripts/test_d1_cascade.py 547 355     # custom lead IDs

Default test grid:
  547  - Unscripted Eau Gallie          HOT, has GM    → cascade NOT fire
  355  - EDITION Residences Miami       HOT, no GM     → cascade FIRES + Task Force (Marriott)
  749  - Rosewood San Francisco         HOT, no GM     → cascade FIRES, NO Task Force
  1248 - Kimpton Salt Lake City         COOL, no GM    → cascade NOT fire
"""

import asyncio
import logging
import sys
from io import StringIO
from pathlib import Path

# Make the script runnable from anywhere by adding repo root to sys.path.
# Script lives at <repo>/scripts/test_d1_cascade.py → repo root = parents[1].
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Explicitly load .env from repo root. When run via uvicorn/celery this is
# already done by the framework, but raw `python scripts/foo.py` does not
# trigger it, so SERPER_API_KEY / GEMINI creds / DB URL would be missing.
try:
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

from sqlalchemy import text

from app.database import async_session
from app.services.iterative_researcher import (
    ResearchState,
    run_iterative_research,
)

# Configure console logging so researcher progress streams in real-time.
# Without this, the researcher's logger.info() calls go nowhere (no handler
# attached to root logger in a raw script context — unlike uvicorn/celery
# which set this up automatically).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
# Mute noisy low-level libs
for noisy in ("httpx", "httpcore", "urllib3", "sqlalchemy.engine"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


DEFAULT_IDS = [547, 355, 749, 1248]

EXPECTATIONS = {
    547: {
        "label": "HOT w/ GM (control)",
        "cascade_should_fire": False,
        "task_force_expected": False,
        "reason": "GM already exists in contacts — Iter 2 should find one, no cascade.",
    },
    355: {
        "label": "EDITION Miami Edgewater (Marriott-family)",
        "cascade_should_fire": True,
        "task_force_expected": True,
        "reason": "HOT + no GM + EDITION is a Marriott brand → cascade WITH Task Force.",
    },
    749: {
        "label": "Rosewood San Francisco (non-Marriott)",
        "cascade_should_fire": True,
        "task_force_expected": False,
        "reason": "HOT + no GM + Rosewood is its own parent → cascade WITHOUT Task Force.",
    },
    1248: {
        "label": "Kimpton Salt Lake City (COOL)",
        "cascade_should_fire": False,
        "task_force_expected": False,
        "reason": "COOL timeline — cascade intentionally does not fire.",
    },
}


async def load_lead(lead_id: int):
    async with async_session() as s:
        row = (
            await s.execute(
                text(
                    """
                    SELECT id, hotel_name, brand, management_company,
                           city, state, country,
                           opening_date, timeline_label,
                           search_name, former_names,
                           COALESCE(description, '') AS description
                    FROM potential_leads
                    WHERE id = :id
                    """
                ),
                {"id": lead_id},
            )
        ).first()
        return row


def _coerce_list(val):
    return val if isinstance(val, list) else []


def _check(label: str, expected: bool, actual: bool) -> str:
    if expected == actual:
        return f"  ✓ {label}: expected={expected} actual={actual}"
    return f"  ✗ {label}: expected={expected} actual={actual}  <-- MISMATCH"


async def test_lead(lead_id: int):
    lead = await load_lead(lead_id)
    if not lead:
        print(f"\n  ❌ Lead id={lead_id} NOT FOUND in potential_leads — skipping\n")
        return

    exp = EXPECTATIONS.get(
        lead_id,
        {
            "label": "(no expectation)",
            "cascade_should_fire": None,
            "task_force_expected": None,
            "reason": "",
        },
    )

    print("=" * 95)
    print(f"LEAD id={lead.id}  {lead.hotel_name!r}")
    print(f"  Brand:    {lead.brand}")
    print(f"  Timeline: {lead.timeline_label}")
    print(f"  Opening:  {lead.opening_date}")
    print(f"  Location: {lead.city}, {lead.state}, {lead.country}")
    print(f"  EXPECTED: {exp['label']}")
    print(f"            {exp['reason']}")
    print("-" * 95)
    print(f"  Running researcher (6 iterations × many queries — takes 2-4 min)...")
    sys.stdout.flush()

    state = ResearchState(
        hotel_name=lead.hotel_name,
        brand=lead.brand,
        management_company=lead.management_company,
        city=lead.city,
        state=lead.state,
        country=lead.country,
        opening_date=str(lead.opening_date) if lead.opening_date else None,
        timeline_label=lead.timeline_label,
        search_name=lead.search_name,
        former_names=_coerce_list(lead.former_names),
        description=lead.description or None,  # ← PHASE B: richer classifier input
        is_existing_hotel=False,  # pipeline leads
    )

    # Capture iterative_researcher logs to inspect cascade markers
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    r_logger = logging.getLogger("app.services.iterative_researcher")
    r_logger.addHandler(handler)
    prev_level = r_logger.level
    r_logger.setLevel(logging.INFO)

    try:
        state = await run_iterative_research(state)
    except Exception as ex:
        print(f"  ❌ RESEARCHER CRASHED: {type(ex).__name__}: {ex}")
        return
    finally:
        r_logger.removeHandler(handler)
        r_logger.setLevel(prev_level)

    log_text = buf.getvalue()
    cascade_log_lines = [
        ln for ln in log_text.splitlines() if "[ITER 2 CASCADE]" in ln
    ]
    task_force_mentioned = any("Task Force" in ln for ln in cascade_log_lines)

    # Summary
    print(f"OBSERVED:")
    print(f"  Iterations done:         {state.iterations_done}")
    print(f"  Total queries run:       {len(state.queries_run)}")
    print(f"  GM found in Iter 2:      {state.has_named_gm}")
    print(f"  Cascade flag flipped:    {state.gm_search_cascade_active}")
    print(f"  Total candidates found:  {len(state.discovered_names)}")

    if cascade_log_lines:
        print(f"  Cascade log lines:")
        for ln in cascade_log_lines:
            print(f"    {ln}")

    # Verify expectations
    if exp["cascade_should_fire"] is not None:
        print(f"\nCHECKS:")
        print(_check("cascade flag", exp["cascade_should_fire"], state.gm_search_cascade_active))
        if exp["cascade_should_fire"]:
            print(_check("task force query", exp["task_force_expected"], task_force_mentioned))

    # Top contacts w/ Iter 6 verdict
    with_prio = [c for c in state.discovered_names if c.get("_final_priority")]
    priority_sort = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
    with_prio.sort(key=lambda c: priority_sort.get(c.get("_final_priority", "P9"), 9))

    if with_prio:
        print(f"\nTOP CONTACTS (Iter 6 verdict):")
        for c in with_prio[:7]:
            name = c.get("name", "?")
            title = c.get("title") or "(no title)"
            org = c.get("organization") or c.get("_current_employer") or ""
            prio = c.get("_final_priority", "?")
            reasoning = (c.get("_final_reasoning") or "").strip()
            print(f"  [{prio}] {name} — {title}")
            if org:
                print(f"        @ {org}")
            if reasoning:
                # wrap reasoning at ~85 chars for readability
                r = reasoning[:150] + ("..." if len(reasoning) > 150 else "")
                print(f"        → {r}")
    else:
        print(f"\n  (No contacts received an Iter 6 verdict)")

    # Interpretive hint for cascade-fired leads
    if state.gm_search_cascade_active and with_prio:
        p1_corporate_regional = sum(
            1
            for c in with_prio
            if c.get("_final_priority") == "P1"
            and (c.get("scope") in ("chain_corporate", "chain_area"))
        )
        p1_area_cluster_gm = sum(
            1
            for c in with_prio
            if c.get("_final_priority") == "P1"
            and any(
                kw in (c.get("title") or "").lower()
                for kw in ("area general manager", "regional general manager", "cluster general manager", "task force")
            )
        )
        print(f"\nCASCADE REWEIGHT CHECK (expected P1 upweights when cascade fired):")
        print(f"  P1 corporate/regional contacts:  {p1_corporate_regional} (should be > 0)")
        print(f"  P1 area/cluster/task-force GM:   {p1_area_cluster_gm} (could be 0 if not surfaced)")

    print()


async def main():
    ids = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else DEFAULT_IDS
    print(f"D1 cascade dry-run test — lead IDs: {ids}")
    print("(no DB writes — contacts are discarded after each run)\n")

    for lead_id in ids:
        await test_lead(lead_id)

    print("=" * 95)
    print("DONE. Manual review checklist:")
    print()
    print("  LEAD 547  (HOT with GM, control):")
    print("    ✓ Cascade flag = False")
    print("    ✓ At least one P1 labeled 'general manager' or similar")
    print()
    print("  LEAD 355  (EDITION Miami Edgewater - Marriott):")
    print("    ✓ Cascade flag = True")
    print("    ✓ Log line includes 'Task Force'")
    print("    ✓ Corporate/regional contacts marked P1 (not P3)")
    print("    ✓ DOSM / Director of Revenue marked P3 (if surfaced)")
    print()
    print("  LEAD 749  (Rosewood San Francisco - non-Marriott):")
    print("    ✓ Cascade flag = True")
    print("    ✓ Log line does NOT include 'Task Force'")
    print("    ✓ Corporate/regional contacts marked P1 (not P3)")
    print()
    print("  LEAD 1248 (Kimpton Salt Lake City - COOL):")
    print("    ✓ Cascade flag = False")
    print("    ✓ No cascade log lines")
    print()


if __name__ == "__main__":
    asyncio.run(main())
