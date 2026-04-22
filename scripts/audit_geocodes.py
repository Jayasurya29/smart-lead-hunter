"""
Geocode Audit Script — flags suspicious coordinates on potential_leads
and sap_clients / existing_hotels.

Usage:
    python audit_geocodes.py

What it checks:
  1. Missing coords (lat/lng null)
  2. Zero-zero coords (0, 0 is Gulf of Guinea — always wrong for hotels)
  3. Out of US + Caribbean bounding box
  4. Mismatched state (lat/lng in CA but lead says TX)
  5. SAP billing-address coords (SAP clients geocoded from billing ≠ property)
  6. Identical coords across many rows (geocoder fallback to city center)

Sample size: outputs first 15 of each issue category.
"""

import asyncio
import os
import sys
from collections import Counter

from sqlalchemy import text
from app.database import async_session


# USA + Caribbean + Puerto Rico bounding box
# Lower-left: (15° N, -170° W) covers Caribbean + Hawaii
# Upper-right: (72° N, -50° W) covers Alaska + Puerto Rico
US_CARIBBEAN_BBOX = {
    "lat_min": 15.0,
    "lat_max": 72.0,
    "lng_min": -170.0,
    "lng_max": -50.0,
}


def in_target_region(lat, lng):
    if lat is None or lng is None:
        return False
    b = US_CARIBBEAN_BBOX
    return (
        b["lat_min"] <= float(lat) <= b["lat_max"]
        and b["lng_min"] <= float(lng) <= b["lng_max"]
    )


def is_zero_zero(lat, lng):
    if lat is None or lng is None:
        return False
    return abs(float(lat)) < 0.001 and abs(float(lng)) < 0.001


async def audit_table(session, table_name, id_col="id", name_col="hotel_name"):
    print(f"\n{'=' * 70}")
    print(f"AUDITING: {table_name}")
    print(f"{'=' * 70}")

    # Get basic counts
    r = await session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    total = r.scalar()

    r = await session.execute(
        text(f"SELECT COUNT(*) FROM {table_name} WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    )
    with_coords = r.scalar()

    print(f"Total rows: {total}")
    print(f"With coords: {with_coords} ({100*with_coords/total:.1f}%)" if total else "With coords: 0")
    print(f"Missing coords: {total - with_coords}")

    # Pull all for analysis
    extra_cols = ""
    if table_name == "potential_leads":
        extra_cols = ", city, state, country, brand"
    elif table_name == "sap_clients":
        extra_cols = ", city, state, country"
        # sap_clients uses different name column
        name_col = "client_name"
        id_col = "id"

    r = await session.execute(
        text(f"""
            SELECT {id_col}, {name_col}, latitude, longitude {extra_cols}
            FROM {table_name}
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
    )
    rows = r.fetchall()

    # Issue 1: Zero-zero coords
    zero_zero = [r for r in rows if is_zero_zero(r[2], r[3])]
    print(f"\n🔴 Zero-zero coords (Gulf of Guinea): {len(zero_zero)}")
    for row in zero_zero[:5]:
        print(f"    id={row[0]}: {row[1]} → ({row[2]}, {row[3]})")

    # Issue 2: Outside US + Caribbean
    out_of_region = [r for r in rows if not is_zero_zero(r[2], r[3]) and not in_target_region(r[2], r[3])]
    print(f"\n🔴 Outside US+Caribbean bbox: {len(out_of_region)}")
    for row in out_of_region[:10]:
        extra = f" — {row[4] or '?'}, {row[5] or '?'}, {row[6] or '?'}" if len(row) > 4 else ""
        print(f"    id={row[0]}: {row[1]} → ({row[2]:.4f}, {row[3]:.4f}){extra}")

    # Issue 3: Identical coords across multiple rows (geocoder fallback)
    coord_counts = Counter(
        (round(float(r[2]), 4), round(float(r[3]), 4))
        for r in rows
        if not is_zero_zero(r[2], r[3])
    )
    clustered = [(coord, count) for coord, count in coord_counts.items() if count >= 3]
    clustered.sort(key=lambda x: -x[1])
    print(f"\n🟡 Clustered coords (≥3 rows share exact lat/lng): {len(clustered)}")
    print("   (Common for city-center fallbacks when specific address not found)")
    for (coord, count) in clustered[:10]:
        # Show a sample of names at this coord
        sample_rows = [r for r in rows if round(float(r[2]), 4) == coord[0] and round(float(r[3]), 4) == coord[1]][:3]
        sample_names = ", ".join(r[1][:30] for r in sample_rows)
        print(f"    ({coord[0]}, {coord[1]}) × {count} rows  — e.g. {sample_names}")

    # Issue 4: State/coord mismatch for potential_leads (only)
    if table_name == "potential_leads":
        # Rough US state bounding boxes (only major outliers worth checking)
        # Focus on obvious mismatches — California lat range, etc.
        STATE_LAT_RANGES = {
            "FL": (24.4, 31.1),
            "CA": (32.5, 42.0),
            "TX": (25.8, 36.5),
            "NY": (40.4, 45.0),
            "HI": (18.9, 22.3),
            "AK": (51.2, 71.5),
        }
        STATE_LNG_RANGES = {
            "FL": (-87.7, -80.0),
            "CA": (-124.5, -114.1),
            "TX": (-106.7, -93.5),
            "NY": (-79.8, -71.8),
            "HI": (-160.5, -154.8),
            "AK": (-180.0, -130.0),
        }
        mismatches = []
        for row in rows:
            if len(row) < 5 or row[5] is None:
                continue  # no state
            state = row[5].strip().upper() if isinstance(row[5], str) else ""
            if state not in STATE_LAT_RANGES:
                continue
            lat_min, lat_max = STATE_LAT_RANGES[state]
            lng_min, lng_max = STATE_LNG_RANGES[state]
            lat, lng = float(row[2]), float(row[3])
            if not (lat_min <= lat <= lat_max and lng_min <= lng <= lng_max):
                mismatches.append(row)
        print(f"\n🔴 State vs coord mismatch (FL/CA/TX/NY/HI/AK): {len(mismatches)}")
        for row in mismatches[:15]:
            print(f"    id={row[0]}: {row[1]} [{row[5]}] → ({row[2]:.4f}, {row[3]:.4f})")


async def main():
    async with async_session() as session:
        # potential_leads
        await audit_table(session, "potential_leads")
        # sap_clients (if exists)
        r = await session.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sap_clients')"
        ))
        if r.scalar():
            await audit_table(session, "sap_clients")
        # existing_hotels (if exists)
        r = await session.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'existing_hotels')"
        ))
        if r.scalar():
            await audit_table(session, "existing_hotels")


if __name__ == "__main__":
    asyncio.run(main())
