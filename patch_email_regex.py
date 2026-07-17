"""patch_email_regex.py — fix _EMAIL_RE in inbox_sync.py so glued headers
("...@jauniforms.comSubject: RE:") can no longer corrupt extracted addresses.

The TLD is matched as a single-case letter run, so lowercase "com" stops at
the capital "S" of a glued "Subject"/"From"/"Sent" header word, while all-caps
addresses (JOHN@ACME.COM) still match. Proven against all 3 corruption
patterns found in the DB (.comsubject / .gdsubject / .netsubject).

Idempotent. Creates .bak, py_compile-gates, auto-restores on failure.
Run from repo root:  python patch_email_regex.py
"""
import py_compile
import shutil
import sys

TARGET = "app/services/inbox_sync.py"
BAK = TARGET + ".bak_emailre"

OLD = r'_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")'
NEW = (
    r'_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?:[a-z]{2,}|[A-Z]{2,})")'
    "  # single-case TLD run: stops at glued 'Subject'/'From' header words"
)

src = open(TARGET, encoding="utf-8").read()

if "single-case TLD run" in src:
    print("already patched — nothing to do")
    sys.exit(0)

assert src.count(OLD) == 1, "anchor not found/unique — file diverged, aborting"

shutil.copy2(TARGET, BAK)
open(TARGET, "w", encoding="utf-8", newline="\n").write(src.replace(OLD, NEW, 1))

try:
    py_compile.compile(TARGET, doraise=True)
except py_compile.PyCompileError as e:
    shutil.copy2(BAK, TARGET)
    print("COMPILE FAILED — restored backup:", e)
    sys.exit(1)

print("patched OK — backup at", BAK)
print("restart uvicorn + celery worker so the sync picks it up")
