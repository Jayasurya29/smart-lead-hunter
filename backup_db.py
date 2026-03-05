#!/usr/bin/env python3
"""
SMART LEAD HUNTER — Database Backup Script
===========================================
Creates timestamped PostgreSQL backups with automatic rotation.

Usage:
    # Manual backup
    python backup_db.py

    # Keep only last 7 backups
    python backup_db.py --keep 7

    # Custom backup directory
    python backup_db.py --dir /path/to/backups

Schedule with Windows Task Scheduler or cron:
    # Daily at 2 AM (Windows Task Scheduler)
    # Action: python C:\\Users\\it2\\smart-lead-hunter\\backup_db.py --keep 14

    # Daily at 2 AM (Linux cron)
    # 0 2 * * * cd /app && python backup_db.py --keep 14
"""

import argparse
import gzip
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def get_db_config() -> dict:
    """Parse DATABASE_URL into connection components."""
    url = os.getenv("DATABASE_URL", "")
    if not url:
        print("ERROR: DATABASE_URL not set in .env")
        sys.exit(1)

    # postgresql://user:pass@host:port/dbname
    url = url.replace("postgresql+asyncpg://", "postgresql://")
    url = url.replace("postgresql://", "")

    # user:pass@host:port/dbname
    user_pass, host_db = url.split("@", 1)
    user, password = user_pass.split(":", 1)
    host_port, dbname = host_db.split("/", 1)

    if ":" in host_port:
        host, port = host_port.split(":", 1)
    else:
        host = host_port
        port = "5432"

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
    }


def backup(backup_dir: str = "backups", keep: int = 14) -> str:
    """Create a gzipped database backup.

    Args:
        backup_dir: Directory to store backups
        keep: Number of backups to retain (oldest deleted)

    Returns:
        Path to the backup file
    """
    db = get_db_config()
    backup_path = Path(backup_dir)
    backup_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"slh_backup_{timestamp}.sql.gz"
    filepath = backup_path / filename

    print(f"Backing up {db['dbname']} on {db['host']}:{db['port']}...")

    # Set password via environment variable (pg_dump reads PGPASSWORD)
    env = os.environ.copy()
    env["PGPASSWORD"] = db["password"]

    # Check if pg_dump is available (Docker or local)
    pg_dump_cmd = "pg_dump"

    # Try Docker first if we're not inside the container
    use_docker = False
    try:
        result = subprocess.run(
            ["docker", "exec", "smart-lead-hunter-db", "pg_dump", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            use_docker = True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    try:
        if use_docker:
            # pg_dump inside the Docker container
            cmd = [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={db['password']}",
                "smart-lead-hunter-db",
                "pg_dump",
                "-U",
                db["user"],
                "--no-owner",
                "--no-acl",
                db["dbname"],
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=300)
        else:
            # Local pg_dump
            cmd = [
                pg_dump_cmd,
                "-h",
                db["host"],
                "-p",
                db["port"],
                "-U",
                db["user"],
                "--no-owner",
                "--no-acl",
                db["dbname"],
            ]
            result = subprocess.run(cmd, capture_output=True, env=env, timeout=300)

        if result.returncode != 0:
            print(f"ERROR: pg_dump failed: {result.stderr.decode()}")
            sys.exit(1)

        # Compress and write
        with gzip.open(filepath, "wb") as f:
            f.write(result.stdout)

        size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"Backup created: {filepath} ({size_mb:.1f} MB)")

    except FileNotFoundError:
        print(
            "ERROR: pg_dump not found. Install PostgreSQL client tools or use Docker."
        )
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("ERROR: pg_dump timed out after 5 minutes")
        sys.exit(1)

    # Rotate old backups
    backups = sorted(backup_path.glob("slh_backup_*.sql.gz"))
    if len(backups) > keep:
        for old in backups[: len(backups) - keep]:
            old.unlink()
            print(f"Deleted old backup: {old.name}")

    print(f"Backups retained: {min(len(backups), keep)}")
    return str(filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Lead Hunter — Database Backup")
    parser.add_argument(
        "--dir", default="backups", help="Backup directory (default: backups)"
    )
    parser.add_argument(
        "--keep", type=int, default=14, help="Backups to keep (default: 14)"
    )
    args = parser.parse_args()

    backup(backup_dir=args.dir, keep=args.keep)
