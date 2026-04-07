"""
One-shot DB cleanup: normalize Caribbean state values + delete junk.
=====================================================================
The state field in existing_hotels has Caribbean parishes/districts
scattered around: 'Pembroke Parish', 'Christ Church', 'West Bay',
'Saint Michael', 'Grand Cayman', 'St. Thomas', 'Curacao', 'Grenadines'.

This consolidates them under proper country names matching the
zones_registry Caribbean entries.

Also deletes obviously international rows that shouldn't be in the
DB at all per scorer.INTERNATIONAL_SKIP rules.

Run once:
    python -m app.scripts.normalize_caribbean
"""

from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.config import settings
from app.models.existing_hotel import ExistingHotel

# Caribbean parish/district → proper country name
# Country names match the Caribbean zones I added to zones_registry.py
CARIBBEAN_NORMALIZATIONS = {
    # Bermuda parishes
    "Pembroke Parish": "Bermuda",
    "Pembroke": "Bermuda",
    "Hamilton Parish": "Bermuda",
    "Southampton Parish": "Bermuda",
    "St. George's Parish": "Bermuda",
    "Sandys Parish": "Bermuda",
    "Smith's Parish": "Bermuda",
    "Devonshire Parish": "Bermuda",
    "Paget Parish": "Bermuda",
    "Warwick Parish": "Bermuda",

    # Barbados parishes
    "Christ Church": "Barbados",
    "Saint Michael": "Barbados",
    "St. Michael": "Barbados",
    "Saint James": "Barbados",
    "St. James": "Barbados",
    "Saint Peter": "Barbados",
    "St. Peter": "Barbados",
    "Saint Philip": "Barbados",
    "St. Philip": "Barbados",

    # Cayman Islands districts
    "Grand Cayman": "Cayman Islands",
    "West Bay": "Cayman Islands",
    "George Town": "Cayman Islands",
    "Bodden Town": "Cayman Islands",
    "East End": "Cayman Islands",
    "North Side": "Cayman Islands",
    "Cayman Brac": "Cayman Islands",
    "Little Cayman": "Cayman Islands",

    # USVI
    "St. Thomas": "US Virgin Islands",
    "Saint Thomas": "US Virgin Islands",
    "St. John": "US Virgin Islands",
    "Saint John": "US Virgin Islands",
    "St. Croix": "US Virgin Islands",
    "Saint Croix": "US Virgin Islands",

    # BVI
    "Tortola": "British Virgin Islands",
    "Virgin Gorda": "British Virgin Islands",

    # Curaçao spelling
    "Curacao": "Curaçao",

    # Grenadines / SVG
    "Grenadines": "St. Vincent & Grenadines",
    "Mustique": "St. Vincent & Grenadines",
    "Bequia": "St. Vincent & Grenadines",
    "Canouan": "St. Vincent & Grenadines",
}

# State values that mean "delete this row" — international, not target market
# Per scorer.INTERNATIONAL_SKIP rules
INTERNATIONAL_DELETE = {
    "Bogotá", "Bogota",       # Colombia
    "Mexico City", "CDMX",    # Mexico (we treat Mexico as international, not Caribbean)
    "Cancun", "Cancún",
    "Cabo", "Los Cabos",
    "Toronto", "Vancouver",   # Canada
    "London", "Paris",        # Europe
    "Dubai", "Abu Dhabi",     # Middle East
}


def main():
    sync_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    engine = create_engine(sync_url, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    db = Session()

    print("=" * 60)
    print("CARIBBEAN NORMALIZATION + INTERNATIONAL CLEANUP")
    print("=" * 60)

    # ── BEFORE snapshot ─────────────────────────────────────────
    print("\nBefore — relevant rows:")
    targets = list(CARIBBEAN_NORMALIZATIONS.keys()) + list(INTERNATIONAL_DELETE)
    rows = (
        db.query(ExistingHotel.state, func.count(ExistingHotel.id))
        .filter(ExistingHotel.state.in_(targets))
        .group_by(ExistingHotel.state)
        .order_by(func.count(ExistingHotel.id).desc())
        .all()
    )
    for state, count in rows:
        verdict = "DELETE" if state in INTERNATIONAL_DELETE else f"→ {CARIBBEAN_NORMALIZATIONS[state]}"
        print(f"  {str(state):25} {count:3}  {verdict}")

    if not rows:
        print("  (no rows match — nothing to do)")
        db.close()
        return

    # ── 1. NORMALIZE Caribbean parishes/districts ───────────────
    total_normalized = 0
    print("\nNormalizing Caribbean entries...")
    for old_value, new_value in CARIBBEAN_NORMALIZATIONS.items():
        result = (
            db.query(ExistingHotel)
            .filter(ExistingHotel.state == old_value)
            .update({"state": new_value, "country": "Caribbean"}, synchronize_session=False)
        )
        if result:
            print(f"  '{old_value}' → '{new_value}': {result} rows")
            total_normalized += result

    # ── 2. DELETE international junk ────────────────────────────
    total_deleted = 0
    print("\nDeleting international rows (out of market)...")
    for bad_state in INTERNATIONAL_DELETE:
        rows_to_delete = (
            db.query(ExistingHotel)
            .filter(ExistingHotel.state == bad_state)
            .all()
        )
        for row in rows_to_delete:
            print(f"  DELETE id={row.id} name='{row.name}' state='{row.state}'")
            db.delete(row)
            total_deleted += 1

    db.commit()

    print(f"\n✓ Normalized {total_normalized} rows")
    print(f"✓ Deleted    {total_deleted} international rows")

    # ── AFTER snapshot ──────────────────────────────────────────
    print("\nAfter — Caribbean countries in DB:")
    caribbean_countries = list(set(CARIBBEAN_NORMALIZATIONS.values())) + [
        "Bahamas", "Jamaica", "Dominican Republic", "Puerto Rico",
        "Aruba", "Anguilla", "Antigua and Barbuda", "Saint Lucia",
        "Turks and Caicos", "Grenada", "Dominica", "Trinidad and Tobago",
        "St. Kitts and Nevis", "St. Martin / Sint Maarten",
    ]
    rows = (
        db.query(ExistingHotel.state, func.count(ExistingHotel.id))
        .filter(ExistingHotel.state.in_(caribbean_countries))
        .group_by(ExistingHotel.state)
        .order_by(func.count(ExistingHotel.id).desc())
        .all()
    )
    if rows:
        for state, count in rows:
            print(f"  {str(state):30} {count}")
    else:
        print("  (no Caribbean rows yet — run pipeline on Caribbean zones to populate)")

    db.close()
    print("\nDone. Run discovery on Caribbean zones with:")
    print("  python -m app.services.pipeline BS --sync   # Bahamas")
    print("  python -m app.services.pipeline KY --sync   # Cayman")
    print("  python -m app.services.pipeline JM --sync   # Jamaica")
    print("  ...etc")


if __name__ == "__main__":
    main()
