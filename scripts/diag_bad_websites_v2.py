#!/usr/bin/env python3
"""diag_bad_websites_v2.py -- READ-ONLY. Flag NON-property website URLs.

Unlike v1 (name/slug overlap, too noisy), this only flags rows whose URL is on
an aggregator, blog, press/media, OTA, or booking-engine domain -- i.e. clearly
not the hotel's own site. Brand sites (fourseasons.com/santabarbara, etc.) are
left alone. Prints a `SET hotel_website=NULL` per hit. No writes.

Usage:  python scripts/diag_bad_websites_v2.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

from urllib.parse import urlparse  # noqa: E402

# Exact OTA/booking hosts (matched against the host, www. stripped).
BAD_HOSTS = {
    "hotels.com", "booking.com", "expedia.com", "tripadvisor.com", "agoda.com",
    "kayak.com", "trivago.com", "yelp.com", "opentable.com", "reservations.com",
    "hotels.cloudbeds.com", "cloudbeds.com", "hosteeva.com", "resort.to",
    "mejoreshoteles.net", "rapidhotels.com", "thclinic.org", "sixflags.com",
}
# Host suffixes that mark blogs / guides / press / media / OTAs.
BAD_HOST_SUFFIX = ("-guide.com", ".cloudbeds.com")
# URL path/query fragments that mark a non-property page.
BAD_FRAG = (
    "/blog/", "/story", "press-release", "/news-", "/media/", "/news/",
    "srsltid=", "news-and-media", "/reservation/",
)


def _is_bad(url: str) -> str | None:
    u = (url or "").strip().lower()
    if not u:
        return None
    host = (urlparse(u if "//" in u else "//" + u).hostname or "").removeprefix("www.")
    if host in BAD_HOSTS:
        return host
    if any(host.endswith(sfx) for sfx in BAD_HOST_SUFFIX):
        return host
    return next((f for f in BAD_FRAG if f in u), None)


async def main() -> int:
    async with async_session() as s:
        rows = [
            dict(r)
            for r in (
                await s.execute(
                    text(
                        "SELECT id, hotel_name, hotel_website FROM existing_hotels "
                        "WHERE COALESCE(hotel_website,'') <> '' AND duplicate_of_id IS NULL"
                    )
                )
            ).mappings().all()
        ]

    hits = []
    for r in rows:
        frag = _is_bad(r["hotel_website"])
        if frag:
            hits.append((r, frag))

    print(f"\nScanned {len(rows)} rows -> {len(hits)} non-property URLs.\n")
    for r, frag in hits:
        print(f"  id={r['id']:<6} {r['hotel_name']}   [{frag}]")
        print(f"        {r['hotel_website']}")
        print(f"        UPDATE existing_hotels SET hotel_website=NULL, updated_at=NOW() WHERE id={r['id']};")
    if hits:
        ids = ",".join(str(r["id"]) for r, _ in hits)
        print(f"\nAll at once:\n  UPDATE existing_hotels SET hotel_website=NULL, updated_at=NOW() WHERE id IN ({ids});")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
