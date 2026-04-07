"""
One-shot DB cleanup: normalize state values.
============================================
Some rows store 'FL', others store 'Florida'. This unifies them all
to full state names (which appears to be the dominant convention).

Run once:
    python -m app.scripts.normalize_states
"""

import sys
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.config import settings
from app.models.existing_hotel import ExistingHotel

# Code → Full name mapping
STATE_CODE_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


def main():
    sync_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    engine = create_engine(sync_url, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    db = Session()

    print("=" * 60)
    print("STATE NORMALIZATION")
    print("=" * 60)

    # Show current distribution
    print("\nBefore:")
    rows = (
        db.query(ExistingHotel.state, func.count(ExistingHotel.id))
        .group_by(ExistingHotel.state)
        .order_by(func.count(ExistingHotel.id).desc())
        .all()
    )
    for state, count in rows:
        print(f"  {str(state):30} {count}")

    # Normalize
    total_updated = 0
    for code, full_name in STATE_CODE_TO_NAME.items():
        # Update rows where state == code (case insensitive, exact match)
        result = (
            db.query(ExistingHotel)
            .filter(func.upper(ExistingHotel.state) == code)
            .update({"state": full_name}, synchronize_session=False)
        )
        if result:
            print(f"  {code} → {full_name}: {result} rows")
            total_updated += result

    db.commit()
    print(f"\n✓ Normalized {total_updated} rows")

    # Show after
    print("\nAfter:")
    rows = (
        db.query(ExistingHotel.state, func.count(ExistingHotel.id))
        .group_by(ExistingHotel.state)
        .order_by(func.count(ExistingHotel.id).desc())
        .all()
    )
    for state, count in rows:
        print(f"  {str(state):30} {count}")

    db.close()


if __name__ == "__main__":
    main()
