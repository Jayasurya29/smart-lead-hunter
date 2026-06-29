"""Vertex cutover smoke test.

Proves the new Google Cloud account is the one actually serving Gemini
calls — using the SAME auth path the app uses (genai.Client(vertexai=True)),
so a PASS here means production will work, not just this script.

Checks, in order:
  1. Which service account / project is ACTIVE (reads vertex-key.json).
  2. Config the app will resolve (project / location / key path).
  3. A real generate on gemini-2.5-flash       (proves auth + billing + model).
  4. A real generate on gemini-2.5-flash-lite   (the triage tier).
  5. A real GROUNDED call (google_search tool)  (proves grounding on new project).

Run from repo root, venv active:
    python scripts/verify_vertex_cutover.py

Optional overrides (otherwise read from app settings / env):
    python scripts/verify_vertex_cutover.py --project NEW_ID --key vertex-key.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OK = "\033[92m✅"
NO = "\033[91m❌"
RST = "\033[0m"


def _resolve_config(args):
    """Resolve project/location/key exactly the way the app does."""
    project = location = key_path = None
    try:
        from app.config import settings  # type: ignore

        project = getattr(settings, "vertex_project_id", None)
        location = getattr(settings, "vertex_location", None)
        key_path = getattr(settings, "vertex_key_path", None)
    except Exception as ex:  # scripts/tests fallback path
        print(f"   (app.config not importable, using env/defaults: {ex})")
        import os

        project = os.getenv("VERTEX_PROJECT_ID", "")
        location = os.getenv("VERTEX_LOCATION", "global")
        key_path = os.getenv("VERTEX_KEY_PATH", "vertex-key.json")

    if args.project:
        project = args.project
    if args.location:
        location = args.location
    if args.key:
        key_path = args.key
    return project, location, key_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", help="override VERTEX_PROJECT_ID")
    ap.add_argument("--location", help="override VERTEX_LOCATION")
    ap.add_argument("--key", help="override VERTEX_KEY_PATH")
    args = ap.parse_args()

    project, location, key_path = _resolve_config(args)
    key_file = (PROJECT_ROOT / key_path) if not Path(key_path).is_absolute() else Path(key_path)

    print("=" * 64)
    print(" VERTEX CUTOVER SMOKE TEST")
    print("=" * 64)
    print(f"  resolved project_id : {project}")
    print(f"  resolved location   : {location}")
    print(f"  resolved key path   : {key_file}")

    # ── 1. Which account/SA is actually in the key file? ──
    if not key_file.exists():
        print(f"{NO} key file not found at {key_file}{RST}")
        return 1
    try:
        sa = json.loads(key_file.read_text())
        sa_project = sa.get("project_id")
        sa_email = sa.get("client_email")
    except Exception as ex:
        print(f"{NO} key file is not valid JSON: {ex}{RST}")
        return 1
    print(f"  key file project_id : {sa_project}")
    print(f"  key file client SA  : {sa_email}")
    if sa_project and project and sa_project != project:
        print(
            f"{NO} MISMATCH: settings project ({project}) != key file project "
            f"({sa_project}). One of them is still pointing at the OLD account.{RST}"
        )
        return 1
    print(f"{OK} key file and config agree on project{RST}")

    # ── 2. Build the genai client the SAME way the app does ──
    try:
        import os

        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(key_file))
        from google import genai
        from google.genai import types

        client = genai.Client(vertexai=True, project=project, location=location)
    except Exception as ex:
        print(f"{NO} could not init genai Vertex client: {ex}{RST}")
        return 1
    print(f"{OK} genai.Client(vertexai=True) initialized{RST}")

    # ── 3 + 4. Real generate on both Flash tiers ──
    failures = 0
    for model in ("gemini-2.5-flash", "gemini-2.5-flash-lite"):
        try:
            resp = client.models.generate_content(
                model=model,
                contents="Reply with exactly one word: OK",
            )
            txt = (resp.text or "").strip()
            print(f"{OK} {model}: live (replied: {txt!r}){RST}")
        except Exception as ex:
            failures += 1
            print(f"{NO} {model}: FAILED — {type(ex).__name__}: {ex}{RST}")

    # ── 5. Grounded call (this is what breaks if region/project is wrong) ──
    try:
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents="In one short sentence, who currently owns the brand Aman Resorts?",
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())]
            ),
        )
        txt = (resp.text or "").strip()[:120]
        print(f"{OK} grounded call: live (sample: {txt!r}){RST}")
    except Exception as ex:
        failures += 1
        print(f"{NO} grounded call: FAILED — {type(ex).__name__}: {ex}{RST}")

    print("=" * 64)
    if failures == 0:
        print(f"{OK} CUTOVER VERIFIED — all calls served by project {project}{RST}")
        print("    Check the NEW project's billing dashboard in ~1h to confirm")
        print("    the usage landed there (and old project goes quiet).")
        return 0
    print(f"{NO} {failures} check(s) failed — do NOT trust the cutover yet.{RST}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
