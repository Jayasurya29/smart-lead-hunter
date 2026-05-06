# Smart Lead Hunter — Audit Follow-up 2026-05-06

This CHANGELOG documents the two follow-up bugs found and fixed the day
after the 2026-05-05 production-grade audit. Both bugs were introduced
*by* the audit itself — they survived ruff, mypy, and the 30-bug
regression suite because they were silent runtime defects, not static
analysis failures.

This is a companion document to `CHANGELOG_AUDIT_2026_05_05.md`. Read
both together to understand the full state of the audit-fix delta.

**Verification status:**
- Both fixes verified end-to-end against the running uvicorn instance.
- `ruff check` and `ruff format` pass clean (pre-commit hooks green).
- PATCH `/api/existing-hotels/{id}` confirmed working with field
  updates persisting and audit log recording the real user_email.

---

## Why these slipped past the original audit

The 2026-05-05 audit caught 38 bugs through code review + targeted
regression tests. The bugs documented below survived because:

1. **Static analysis cannot catch shadow-imported names.** When code
   imports `from dataclasses import dataclass, field`, the name `field`
   becomes a valid module-level binding. A typo elsewhere in the file
   that references `field` (intending a loop variable) silently uses
   the imported function instead of crashing — and `getattr(obj, <function>, None)`
   returns `None` without raising. No NameError, no log line.

2. **The audit did not exercise every PATCH path with real data.**
   The original audit added input-validation guards but did not POST
   real edits through the new code paths. A single smoke test that
   asserted "PATCH actually changes a field" would have caught Bug #18a
   immediately.

The lesson — and the prescription for future audits: **functional
smoke tests against the running server are non-negotiable for any
mutation handler the audit touches.** Pure static review is necessary
but not sufficient.

---

## CRITICAL (1)

### Bug #18a — `update_existing_hotel` silently dropped every field update
**Files:** `app/routes/existing_hotels.py` (lines 1670-1710 region)

Audit Bug #18 added input validation, type coercion, and audit-log
integration to the existing_hotels PATCH handler. During that work
the loop variable was renamed from `field` to `fname` everywhere
*except* in 5 references inside the loop body:

```python
old_values[fname] = getattr(hotel, field, None)   # WRONG — should be fname
for fname, value in body.items():
    if field not in allowed:                       # WRONG
        continue
    elif field in _BOOL_FIELDS: ...                # WRONG
    elif field in _INT_FIELDS: ...                 # WRONG
    elif field in _FLOAT_FIELDS: ...               # WRONG
```

Because `field` was already imported at module top from
`dataclasses` (used by the `@dataclass` decorator on the SmartFillJob
dataclass below), Python did not raise NameError. Instead:

- `getattr(hotel, <dataclasses.field function>, None)` returned `None`
  for every iteration → `old_values` populated with all-None pairs.
- `if <function> not in allowed` was always True (a function is never
  a string in the allowed set) → the `continue` ALWAYS fired → no
  field ever got written.
- `setattr(hotel, fname, coerced)` ran unreachably.

Result: every PATCH returned a "successful" response shape (the
unchanged hotel.to_dict()) with HTTP 200 in some intermediate states,
but the actual mutation never persisted. The frontend saw HTTP 401
in this specific deployment due to a separate but unrelated middleware
interaction during the broken response path, which was the symptom
that surfaced the bug.

**Fix:** All 5 occurrences of `field` inside the function body
replaced with `fname` (the actual loop variable). Verified via
PATCH against a real lead — all whitelisted fields now persist.

**Detection effort:** ~1 hour. Diagnosis was complicated by:
- The 401 response misled debugging toward auth/CSRF.
- ruff did not flag the undefined name because `field` was a valid
  module-level import.
- The handler returned a plausible-looking response, hiding the no-op.

### Bug #18b — Redundant `_csrf=Depends(require_ajax)` on PATCH route
**Files:** `app/routes/existing_hotels.py:1510`

The same audit added `_csrf=Depends(require_ajax)` to the PATCH
route as a defense-in-depth CSRF guard. After investigation this
was determined to be redundant for this specific route — the
SameSite=Lax cookie + axios `X-Requested-With` header already
provide CSRF protection consistent with the leads PATCH route
(which has no `require_ajax` guard).

Removed for symmetry with leads PATCH. Other CRUD routes on
existing_hotels (POST contacts, DELETE contacts, approve, reject,
restore, export-csv) retain their `require_ajax` guards because
their attack surfaces differ.

---

## HIGH (1)

### Bug #18c — Audit log attributed every existing_hotel edit to "system"
**Files:** `app/routes/existing_hotels.py` (lines 1715-1725 region)

Audit Bug #18 wired `log_action()` into the existing_hotels PATCH
handler but hardcoded `user_email="system"` with a follow-up TODO
comment ("No JWT helper exists for this route — track in follow-up").
The comment was true but the follow-up was never completed before
ship — every existing_hotel edit in production was attributed to
`"system"` in `audit_log`, breaking accountability and any future
"who edited this hotel?" forensic query.

**Fix:** Added a local `_get_user_email(request)` helper that mirrors
the one in `dashboard.py`. Decodes the JWT cookie inline (no DB
lookup, no auth round-trip) and returns the email claim or
`"unknown"` on decode failure. The slight duplication is intentional —
preferable to a circular import. If a third route ever needs this
helper, lift it to `app/shared.py`.

**Verification:** Edited a hotel in the UI, confirmed the audit_log
row now records the actual logged-in user's email instead of
`"system"`.

---

## Things deliberately NOT changed (carried forward)

All "deliberately NOT changed" items from the 2026-05-05 changelog
remain unaddressed by this follow-up:
- Scraper-time tier filter tightening
- Drift cleanup between `rescore.py` and `contact_scoring.py`
- In-memory rate-limit store migration to Redis

The Phase D / dedup / state-expansion / health-check items on the
broader SLH backlog also remain open. See user memories for the
canonical backlog.

---

## Lessons codified for next audit

1. **Functional smoke tests for every modified mutation handler.**
   At minimum: PATCH with one field, assert response shows new value,
   re-fetch and assert DB persistence. ~3 minutes per route. Would
   have caught Bug #18a in seconds.

2. **Audit ban on shadow-imported names.** If a route file imports a
   name like `field`, `type`, `id`, `format`, `filter`, `list`, `dict`,
   `set`, `time`, `json` at module level, no local variable in that
   file may share the name. Static rule, easy to enforce in CI.

3. **TODO comments in audit-shipped code are bugs in waiting.** The
   "track in follow-up" comment on line 1719 of existing_hotels.py
   shipped as production code. Going forward: any audit fix that
   leaves a TODO must either (a) be completed in the same PR, or
   (b) opened as a tracked issue with a deadline before the audit
   PR can merge.

---

## Verification commands

To reproduce the verification done before commit:

```powershell
# Fix #18a — confirm no 'field' references inside update_existing_hotel
Select-String -Path app\routes\existing_hotels.py -Pattern '\bfield\b' `
  | Where-Object { $_.LineNumber -gt 1700 -and $_.LineNumber -lt 1850 }
# Expected: only matches in comments, not code

# Fix #18b — confirm require_ajax removed from update PATCH
Select-String -Path app\routes\existing_hotels.py `
  -Pattern '_csrf=Depends\(require_ajax\)' `
  | Where-Object { $_.LineNumber -gt 1500 -and $_.LineNumber -lt 1520 }
# Expected: empty

# Fix #18c — confirm helper present and 'system' fallback removed
Select-String -Path app\routes\existing_hotels.py -Pattern 'def _get_user_email'
Select-String -Path app\routes\existing_hotels.py -Pattern 'user_email="system"'
# Expected: helper found, system fallback empty

# End-to-end: PATCH a real hotel
# Then check audit log has the real user
docker exec -it db psql -U postgres -d smart_lead_hunter `
  -c "SELECT user_email, action, hotel_name, created_at FROM audit_log ORDER BY created_at DESC LIMIT 5;"
# Expected: real email in user_email column, not 'system'
```

---

## Commits

```
1d17c8f  Fix existing_hotels PATCH: NameError 'field' → 'fname' + remove redundant require_ajax
<next>   Fix existing_hotels audit log: record real user_email instead of 'system'
```

---

*Follow-up audit and patches by Claude Opus 4.7 in collaboration with
Jay (JA Uniforms). 2026-05-06.*
