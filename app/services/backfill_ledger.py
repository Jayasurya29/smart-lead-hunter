"""
app/services/backfill_ledger.py
===============================
The "count each real email once" gate for the historical backfill.

`sync_mailbox(..., ledger=BackfillLedger(session))` calls `await ledger.take(rfc_id)`
for each message's RFC `Message-ID`. `take` does an atomic INSERT ... ON CONFLICT
DO NOTHING:

  - returns True  → this Message-ID is NEW → count the message
  - returns False → already counted (prior batch / run / another mailbox) → skip

Because the insert shares `sync_mailbox`'s transaction, a crash mid-batch rolls
back BOTH the ledger rows and the contact upserts together — so a resumed batch
re-runs cleanly with no double-count. Daily sync passes no ledger, so none of
this affects it.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Postgres TEXT has no hard length limit, but RFC Message-IDs are short; guard
# against a pathological header blowing up an index entry.
_MAX_ID_LEN = 998


class BackfillLedger:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.new = 0
        self.dup = 0
        self.blank = 0

    async def take(self, rfc_message_id: str) -> bool:
        """True if this is the first time we've counted this email."""
        rid = (rfc_message_id or "").strip()[:_MAX_ID_LEN]
        if not rid:
            # No stable id → can't dedup it; count it (rare). Better to slightly
            # over-count a header-less message than to drop a real contact.
            self.blank += 1
            return True
        row = (
            await self.session.execute(
                text(
                    "INSERT INTO synced_messages (rfc_message_id) VALUES (:id) "
                    "ON CONFLICT (rfc_message_id) DO NOTHING RETURNING rfc_message_id"
                ),
                {"id": rid},
            )
        ).first()
        if row is None:
            self.dup += 1
            return False
        self.new += 1
        return True

    def stats(self) -> dict:
        return {"new": self.new, "dup": self.dup, "blank": self.blank}
