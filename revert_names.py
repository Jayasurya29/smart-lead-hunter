"""Revert resolved names on specific contacts (back to nameless).

    python revert_names.py 4736
    python revert_names.py 4736 5084
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main(ids: list[int]) -> None:
    async with async_session() as session:
        for cid in ids:
            row = (
                await session.execute(
                    text(
                        "SELECT email, first_name, last_name FROM contacts "
                        "WHERE id = :id"
                    ),
                    {"id": cid},
                )
            ).one_or_none()
            if not row:
                print(f"#{cid}: not found")
                continue
            await session.execute(
                text(
                    "UPDATE contacts SET first_name = NULL, last_name = NULL, "
                    "display_name = email, updated_at = NOW() WHERE id = :id"
                ),
                {"id": cid},
            )
            print(f"#{cid}: reverted '{row.first_name} {row.last_name}' -> nameless ({row.email})")
        await session.commit()


if __name__ == "__main__":
    ids = [int(x) for x in sys.argv[1:] if x.isdigit()]
    if not ids:
        print("usage: python revert_names.py <id> [<id> ...]")
        sys.exit(1)
    asyncio.run(main(ids))
