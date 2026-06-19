#!/usr/bin/env python3
"""scripts/diag_linkedin_match.py -- why did find_linkedin_url pick the wrong slug? (read-only)

Shows, for a name+org, every linkedin.com/in candidate Serper surfaced and whether
the accept() guard takes it -- so we can see the wrong-person match (e.g. Andrew
Radwanski -> gradwanski, while the real andrew-r-... was rejected).

USAGE (repo root, venv active):
    python scripts/diag_linkedin_match.py "Andrew Radwanski" "Ritz Carlton Orlando Grand Lakes" andrew.radwanski@ritzcarlton.com
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def main() -> int:
    name = sys.argv[1] if len(sys.argv) > 1 else "Andrew Radwanski"
    org = sys.argv[2] if len(sys.argv) > 2 else "Ritz Carlton Orlando Grand Lakes"
    email = sys.argv[3] if len(sys.argv) > 3 else "andrew.radwanski@ritzcarlton.com"

    from app.services.smart_fill import find_linkedin_debug, _serper_linkedin_raw, _norm_linkedin

    parts = name.split()
    first_l, last_l = parts[0].lower(), parts[-1].lower()
    anchor = (org or "").strip() or ((email or "").split("@")[-1].split(".")[0] if "@" in (email or "") else "")

    dbg = find_linkedin_debug(name, org, email)
    print("=" * 74)
    print(f"name={name!r}  org={org!r}")
    print(f"first={first_l!r}  last={last_l!r}")
    print("=" * 74)
    print(f"ACCEPTED by current guard: {dbg.get('url')!r}")
    print(f"queries run: {dbg.get('queries')}")

    # Rebuild the SAME query list find_linkedin_url/_debug use, and dump raw
    # results for EACH so we see which query surfaced gradwanski and whether the
    # real profile appears anywhere (with its title/company).
    local = (email or "").split("@")[0] if "@" in (email or "") else ""
    queries = [f'"{name}" {anchor} site:linkedin.com/in'.strip()]
    if anchor:
        queries.append(f'"{name}" {anchor} linkedin')
        queries.append(f"{first_l} {last_l} {anchor} linkedin")
    if local and "." in local:
        queries.append(f'{local.replace(".", " ")} {anchor} linkedin')
    # also a bare name query (what often surfaces the real abbreviated-slug profile)
    queries.append(f'"{name}" linkedin')
    # TITLE/LOCATION-flavored queries -- surface a HIDDEN-SURNAME profile
    # ("Andrew R.") that exact-name queries skip. Title+location is the decisive
    # signal here. (Pass title as 4th arg to exercise; defaults below for Andrew.)
    title_hint = sys.argv[4] if len(sys.argv) > 4 else "Director of Event Operations"
    loc_hint = "Orlando"
    if title_hint:
        queries.append(f"{first_l} {last_l[:1]} {title_hint} {loc_hint} linkedin")
        queries.append(f'{first_l} "{title_hint}" {anchor} linkedin')

    for q in queries:
        print(f"\n>>> QUERY: {q!r}")
        raw = _serper_linkedin_raw(q) or []
        any_li = False
        for r in raw:
            bits = r.split(" :: ")
            title = bits[0] if len(bits) > 0 else ""
            snippet = bits[1] if len(bits) > 1 else ""
            link = bits[2] if len(bits) > 2 else ""
            url = _norm_linkedin(link) or _norm_linkedin(r)
            if not url:
                continue
            any_li = True
            slug = url.rsplit("/", 1)[-1].lower()
            slug_compact = slug.replace("-", "").replace("_", "")
            last_full = len(last_l) >= 3 and (last_l in slug)
            last_prefix = len(first_l) >= 2 and len(last_l) >= 3 and (first_l + last_l[:3]) in slug_compact
            gate = "PASS" if (last_full or last_prefix) else "fail"
            print(f"   [{gate}] slug={slug!r}")
            print(f"          TITLE  : {title}")
            print(f"          SNIPPET: {snippet[:180]}")
        if not any_li:
            print("   (no linkedin.com/in results)")
    print("\n(read-only -- TITLE/SNIPPET carry role+company; the guard ignores them)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
