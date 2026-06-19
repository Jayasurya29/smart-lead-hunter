#!/usr/bin/env python3
"""scripts/probe_wiza_linkedin.py -- can Wiza find the LinkedIn URL search can't? (1-2 credits)

Andrew Radwanski's real profile (andrew-r-a64b2662) never surfaces in name search
because his surname is hidden ("Andrew R."). Wiza is keyed to resolve the PERSON,
so a name+domain reveal should return his linkedin_profile_url directly. This
probe checks that -- it does ONE name+domain reveal and prints the profile URL +
company/title + credits. (enrichment_level 'none' = 1 credit profile-only.)

USAGE (repo root, venv active):
    python scripts/probe_wiza_linkedin.py "Andrew Radwanski" ritzcarlton.com
    python scripts/probe_wiza_linkedin.py "Andrew Radwanski" "" "Ritz-Carlton Orlando Grande Lakes"
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


async def main() -> int:
    name = sys.argv[1] if len(sys.argv) > 1 else "Andrew Radwanski"
    domain = sys.argv[2] if len(sys.argv) > 2 else "ritzcarlton.com"
    org = sys.argv[3] if len(sys.argv) > 3 else ""

    import httpx
    from app.services.wiza_enrichment import _get_api_key, _post_reveal_and_poll

    key = _get_api_key()
    if not key:
        print("WIZA_API_KEY not set")
        return 1

    reveal = {"full_name": name}
    if domain:
        reveal["domain"] = domain
    elif org:
        reveal["company"] = org
    body = {"individual_reveal": reveal, "enrichment_level": "none"}  # 1 credit, profile only
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    print(f"Wiza name+{'domain' if domain else 'company'} reveal: {name!r} / {domain or org!r}")
    async with httpx.AsyncClient(timeout=90) as client:
        data = await _post_reveal_and_poll(client, headers, body, name)

    if not data:
        print("No reveal returned (not found / timeout / no credits).")
        return 1

    print("\n==== KEY FIELDS ====")
    print(f"  name                 : {data.get('name')!r}")
    print(f"  linkedin_profile_url : {data.get('linkedin_profile_url')!r}")
    print(f"  company              : {data.get('company')!r}")
    print(f"  title                : {data.get('title')!r}")
    print(f"  company_domain       : {data.get('company_domain')!r}")
    print(f"  credits              : {json.dumps(data.get('credits') or {}, ensure_ascii=False)}")
    print("\n(does the linkedin_profile_url match andrew-r-a64b2662 ?)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
