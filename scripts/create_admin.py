"""
Create the first admin user for Smart Lead Hunter.

Run once after migration:
    python create_admin.py

Prompts for email and password interactively.
"""

import asyncio
import getpass
import sys
import os

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))

import bcrypt
from sqlalchemy import select
from app.database import async_session
from app.models.user import User


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


async def create_admin():
    print("\n=== Create Admin User ===\n")

    first_name = input("First name: ").strip()
    last_name = input("Last name: ").strip()
    email = input("Email: ").strip().lower()

    password = getpass.getpass("Password (min 8 chars): ")
    if len(password) < 8:
        print("Password too short. Must be at least 8 characters.")
        return

    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Passwords don't match.")
        return

    async with async_session() as session:
        # Check if already exists
        existing = await session.execute(select(User).where(User.email == email))
        if existing.scalar_one_or_none():
            print(f"User {email} already exists.")
            return

        user = User(
            first_name=first_name,
            last_name=last_name,
            email=email,
            password_hash=hash_password(password),
            role="admin",
            is_active=True,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)

    print(f"\nAdmin user created: {email} (ID: {user.id})")
    print("You can now log in at http://localhost:3000/login")


if __name__ == "__main__":
    asyncio.run(create_admin())
