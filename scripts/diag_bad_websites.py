#!/usr/bin/env python3
"""diag_bad_websites.py -- READ-ONLY. Flag wrong hotel_website values.

Two signals that a row's hotel_website belongs to a DIFFERENT property:
  SHARED : >=2 differently-named rows point at the same host+path URL
           (one owns it, the rest are mis-scraped) -- e.g. Hyatt Coconut Point
           carrying Hyatt Sarasota's URL.
  SOLO   : a single row whose name shares NO distinctive token with its own
           URL slug (e.g. "Four Seasons Miami" -> brickell-arch-... slug).

For SHARED clusters the row whose name best matches the slug is kept; the
others are flagged. Prints a ready-to-run `SET hotel_website=NULL` per flagged
row. No writes.

Usage (repo root, venv, DATABASE_URL set):
    python scripts/diag_bad_websites.py
"""

import argparse
import asyncio
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

PARENT_DOMAINS = {
    "marriott.com", "hilton.com", "hyatt.com", "ihg.com", "wyndham.com",
    "wyndhamhotels.com", "choicehotels.com", "accor.com", "omnihotels.com",
    "ritzcarlton.com", "fairmont.com", "sonesta.com", "loewshotels.com",
    "marriotthotels.com", "kimptonhotels.com", "disney.go.com",
    "disneyworld.disney.go.com", "disneyland.disney.go.com",
}
_STOP = {
    "the", "a", "an", "by", "and", "of", "at", "en", "us", "hotel", "hotels",
    "resort", "resorts", "spa", "suites", "suite", "inn", "collection",
    "autograph", "tribute", "tapestry", "curio", "luxury", "beach", "club",
    "downtown", "overview", "hoteldetail", "index", "home", "www", "com",
    "marriott", "hilton", "hyatt", "ihg", "wyndham", "omni", "sonesta", "loews",
    "kimpton", "doubletree", "embassy", "renaissance", "westin", "sheraton",
    "ritz", "carlton", "conrad", "regency", "grand", "places", "stay", "web",
}


def _toks(s: str) -> set:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = "".join(c if c.isalnum() else " " for c in s)
    return {t for t in s.split() if t not in _STOP and len(t) > 2}


def _url_key(url: str | None) -> str | None:
    if not url:
        return None
    h = url.strip().lower()
    for p in ("https://", "http://"):
        if h.startswith(p):
            h = h[len(p):]
    if h.startswith("www."):
        h = h[4:]
    h = h.split("?")[0].split("#")[0].rstrip("/")
    for suf in ("/overview", "/index.html", "/index.do", "/default.htm"):
        if h.endswith(suf):
            h = h[: -len(suf)].rstrip("/")
    host = h.split("/")[0]
    if "/" not in h and host in PARENT_DOMAINS:
        return None
    return h or None


def _slug_toks(url_key: str) -> set:
    # tokens from the path portion (drop the host)
    return _toks(url_key.split("/", 1)[1]) if "/" in url_key else set()


async def main(args: argparse.Namespace) -> int:
    async with async_session() as s:
        rows = [
            dict(r)
            for r in (
                await s.execute(
                    text(
                        "SELECT id, hotel_name, hotel_website, city, state "
                        "FROM existing_hotels WHERE COALESCE(hotel_website,'') <> '' "
                        "AND duplicate_of_id IS NULL"
                    )
                )
            ).mappings().all()
        ]

    by_url: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        k = _url_key(r.get("hotel_website"))
        if k:
            r["_url"] = k
            by_url[k].append(r)

    shared, solo = [], []
    for k, grp in by_url.items():
        slug = _slug_toks(k)
        names = {tuple(sorted(_toks(r["hotel_name"]))) for r in grp}
        if len(grp) >= 2 and len(names) >= 2:
            # keep the row whose name best overlaps the slug; flag the rest
            best = max(grp, key=lambda r: len(_toks(r["hotel_name"]) & slug) if slug else 0)
            for r in grp:
                if r["id"] != best["id"]:
                    shared.append((r, best, k))
        elif len(grp) == 1 and slug:
            r = grp[0]
            if not (_toks(r["hotel_name"]) & slug):
                solo.append((r, k))

    print(f"\nScanned {len(rows)} rows with a website.")
    print(f"SHARED-URL mismatches (wrong site copied from another property): {len(shared)}")
    for r, best, k in shared[: args.limit]:
        print(f"  id={r['id']:<6} {r['hotel_name']}  -> URL belongs to: {best['hotel_name']}")
        print(f"        {r.get('hotel_website')}")
        print(f"        UPDATE existing_hotels SET hotel_website=NULL, updated_at=NOW() WHERE id={r['id']};")
    print(f"\nSOLO name/slug mismatches (name shares no token with its own URL): {len(solo)}")
    for r, k in solo[: args.limit]:
        print(f"  id={r['id']:<6} {r['hotel_name']}")
        print(f"        {r.get('hotel_website')}")
        print(f"        UPDATE existing_hotels SET hotel_website=NULL, updated_at=NOW() WHERE id={r['id']};")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    sys.exit(asyncio.run(main(ap.parse_args())))
