# Smart Lead Hunter — Audit Fixes 2026-05-05

This CHANGELOG documents all fixes applied during the 2026-05-05 production-grade
audit. Every entry is keyed to a numbered bug from the Phase 2 audit report
(38 bugs total: 6 CRITICAL, 12 HIGH, 14 MEDIUM, 6 LOW).

**Verification status:**
- Every modified file passes `python -m py_compile` and `ruff check`.
- 312 unit tests pass (282 pre-existing + 30 new audit regression tests).
- Zero test regressions from these changes.

The new test file `tests/test_audit_2026_05_05.py` locks in the contracts these
fixes restored — run it as part of every CI cycle going forward.

---

## CRITICAL (6)

### Bug #1 — `prepare_lead` no longer creates `status='expired'` zombies
**Files:** `app/services/lead_factory.py`

`prepare_lead` was building rows with `status='expired'` whenever the timeline
fell into the EXPIRED bucket. The `save_lead_to_db` direct-to-existing branch
was supposed to graduate them, but on any exception it silently fell through
to `session.add(lead)` and persisted the expired row to `potential_leads` —
violating invariant #1.

Now every lead is built with `status='new'`. If the EXPIRED graduation branch
fails, the partial state is rolled back and the caller receives a real error
rather than a zombie row.

### Bug #2 — Dashboard edit handler no longer creates expired zombies on transfer failure
**Files:** `app/routes/dashboard.py`

The edit handler set `status='expired'` and committed before attempting the
transfer in a fresh session. If transfer failed, the lead persisted at
`status='expired'` permanently. Reordered: transfer first, only flip status on
*failure* (so the daily recompute task picks it up). New response shape
`{status: 'transfer_failed'}` lets the UI tell the user the recompute will retry.

### Bug #3 — `save_leads_batch` no longer undercounts saved leads
**Files:** `app/services/lead_factory.py`

The if/elif chain had no branches for `saved_to_existing` / `merged_to_existing`,
so leads that graduated directly to `existing_hotels` committed but went uncounted.
`ScrapeLog.leads_new` was wrong on every scrape that hit an EXPIRED hotel. New
explicit branches plus a `graduated_to_existing` counter and an `error` branch.

### Bug #4 — `approve_existing_hotel` no longer silently fails CRM push while marking approved
**Files:** `app/routes/existing_hotels.py`

Three defects in this single handler:
1. Referenced `hotel.phone` — column dropped in migration 018, raised
   `AttributeError` on every approve, swallowed by the surrounding except,
   and the hotel was marked approved with **zero Insightly push**.
2. `hotel.gm_name.split()[0]` raised `IndexError` on whitespace-only values.
3. CRM failure was conflated with the status flip.

Fixed by using `contact_phone` / `gm_phone` (canonical post-018), defensive
name-splits, and pushing to Insightly first — only flipping `status='approved'`
on success. CRM failures now return 502.

### Bug #5 — `existing_hotels_parity` router is registered
**Files:** `app/main.py`

The router defined three POST endpoints the frontend actively calls
(`toggle-scope`, `enrich-email`, `rescore`) but was never imported or
registered in main.py. Frontend got 404 on all three. Now registered.

### Bug #6 — Smart Fill `already_opened` no longer drops enriched fields
**Files:** `app/routes/scraping.py` (sync handler + `_apply_enrichment_to_lead`)

When Smart Fill detected `already_opened`, the code returned immediately —
all subsequent field updates (room_count, brand_tier, brand, mgmt_co, owner,
developer, address, hotel_website, lat/lng, search_name, former_names) were
silently dropped. The auto-transfer block then copied the lead's *stale*
fields into `existing_hotels`. Now both sync and SSE paths apply all enriched
fields, then the auto-transfer block at the end handles graduation.

---

## HIGH (12)

### Bug #7 — Canonical 5-tier brand system enforced everywhere
**Files:** `app/schemas.py`, `app/routes/dashboard.py`, `app/services/insightly.py`,
`app/services/existing_hotel_scorer.py`, `app/services/lead_data_enrichment.py`,
`app/services/rescore.py`, `app/routes/scraping.py`,
`scripts/normalize_brand_tiers.py` (new)

JA Uniforms targets 4-star+ properties only. Canonical brand_tier values are
exactly five: `tier1_ultra_luxury, tier2_luxury, tier3_upper_upscale, tier4_upscale, tier5_skip`.
Earlier code paths (Gemini grounding prompt, lead_data_enrichment validators,
`_apply_enrichment_to_lead` apply layer, rescore VALID_TIERS, the existing-hotel
scorer's defensive map) accepted three non-canonical values:
`tier5_upper_midscale, tier6_midscale, tier7_economy`.

**Tightened all writers** to reject non-canonical values. The existing-hotel
scorer's `_score_brand_tier` now normalizes any non-canonical input to
`tier5_skip` and fires `out_of_scope_warning=True` so sales sees the data error.

**Added one-shot migration script** at `scripts/normalize_brand_tiers.py` —
run with `--apply` to rewrite any non-canonical values currently in
`potential_leads` and `existing_hotels` to `tier5_skip` and append a
timestamped note explaining the change.

### Bug #8 — All "queue for transfer" sites now flip `status='expired'` consistently
**Files:** `app/services/lead_data_enrichment.py`, `app/routes/scraping.py`,
`app/tasks/autonomous_tasks.py`

Four sites set `timeline_label='EXPIRED'` and queued for transfer while leaving
`status='new'`. If transfer failed, the lead persisted as a zombie that Pipeline
filters didn't exclude. Now all four sites set `status='expired'` before queueing.

### Bug #9 — `_find_existing_hotel_match` requires city AND state
**Files:** `app/services/lead_transfer.py`

Loose match was `city OR state OR country` — country='USA' for ~95% of rows,
so any two unrelated US hotels with matching legacy names merged. The
single-candidate fallback was even worse (no location check). Both removed;
match now requires both city AND state present and equal (case-insensitive).

### Bug #10 — Re-enrich uses normalized lookup, no longer drops updates
**Files:** `app/routes/contacts.py`

The previous code checked `normalized_name in existing_names` (true on
whitespace/case differences) but then re-fetched with an unnormalized
`LeadContact.name == name` — when the new payload had any name variation,
the membership check passed but the SELECT returned nothing and the update
silently no-op'd. New evidence/score/scope was thrown away. Now uses a
pre-loaded `normalized_name → row` dict for both check and lookup.

### Bug #11 — Fuzzy match excludes approved/pushed leads
**Files:** `app/services/lead_factory.py`

Fuzzy dedup matched against approved/pushed leads, so re-scraping an approved
lead's source page came back as "duplicate" with no enrichment update —
Insightly stayed stale silently. Now excludes approved/pushed/deleted in
addition to expired/rejected.

### Bug #12 — LeadDetail edit handler full invalidation set + transferred response
**Files:** `frontend/src/components/leads/LeadDetail.tsx`

Was missing `['stats']`, `['map-leads']`, `['map-data']`, `['contacts', leadId]`,
`['existing-hotels']`, `['existing-hotels-stats']`. Also handles `transferred` /
`transfer_failed` server responses with appropriate UI messages.

### Bug #13 — `useLeads.ts` mutations comprehensive invalidation via shared helper
**Files:** `frontend/src/hooks/useLeads.ts`

Every mutation hook (approve/reject/restore/delete/smartfill/enrich) now calls
the shared `invalidateLeadEverywhere(qc, id)` helper which invalidates all 8
cache keys. No more 30-second poll lag after mutations.

### Bug #14 — Restore handler recomputes timeline + routes through transfer
**Files:** `app/routes/dashboard.py`

A lead rejected when timeline was HOT could be EXPIRED by restore time. The
old handler set `status='new'` without recomputing — created a zombie. Now
recomputes `timeline_label` and routes through `transfer_lead` if it's now EXPIRED.

### Bug #15 — `rescore_all_leads` includes `status='expired'`
**Files:** `app/services/rescore.py`

Excluded expired leads from rescoring, so zombie expired leads kept stale
scores forever. Now includes them and triggers `transfer_lead` for any whose
timeline re-confirms EXPIRED.

### Bug #16 — `batch_smart_fill` has full validity guards
**Files:** `app/services/lead_data_enrichment.py`

`batch_smart_fill` was overwriting `brand_tier`/`room_count`/`brand`
unconditionally with no validity checks and no smart-vs-full mode gate.
Manual `tier2_luxury` overrides could be flipped to "unknown" by the
nightly batch. Mirrored the validity guards from `batch_full_refresh`:
sentinel detection, mode-gated overwrites, valid-tier whitelist (canonical 5).

### Bug #17 — Auth middleware checks `User.is_active` with TTL cache
**Files:** `app/middleware/auth.py`, `app/routes/auth.py`

Middleware accepted any decoded JWT with a `sub` claim and never checked
`User.is_active` — a deactivated user kept full access until JWT expiry
(8h or 30d). Added `_is_user_active()` with 60-second TTL cache to keep the
hot path fast. `auth.py:deactivate_user` calls `_clear_user_active_cache()`
to force re-fetch on next request.

### Bug #18 — `PATCH /existing-hotels/{id}` validates inputs and syncs canonical/legacy
**Files:** `app/routes/existing_hotels.py`

Was zero validation. Added: length caps, email regex, lead_score 0-100,
room_count 0-10000, lat/lng float coercion, bool/int/float coercion, and
auto-sync of legacy↔canonical pairs (name↔hotel_name, website↔hotel_website,
gm_*↔contact_*, property_type↔hotel_type). Also calls `log_action` for audit.

---

## MEDIUM (14)

### Bug #19 — `normalize_person_name()` for contact dedup
**Files:** `app/services/utils.py`, `app/routes/contacts.py`

Contact dedup was using `normalize_hotel_name` which strips chain suffixes
(Hotels, Inn, Resort, etc.) — wrong for people. New `normalize_person_name`
strips diacritics + honorifics (Mr/Ms/Mrs/Dr/Prof), keeps hyphens and
apostrophes (O'Brien, Mary-Anne), collapses whitespace. `contacts.py`
re-enrich uses it instead.

### Bug #20 — `rescore_lead` returns `needs_transfer` flag
**Files:** `app/services/rescore.py`

If a rescore moves a lead's timeline into EXPIRED, callers get `needs_transfer=True`
in the result dict. `rescore_all_leads` uses this to graduate crossings.

### Bug #21 — `_SSE_PATHS` covers all streaming endpoints via prefix matching
**Files:** `app/main.py`

Original whitelist had 3 dashboard SSE paths. Smart Fill (lead + EH),
EH enrich-stream, and outreach generate-stream were missing. Replaced
with a `_is_sse_path()` helper that does prefix matching for parameterized
routes. Backwards-compat wrapper `_SSE_PATHS` keeps the old `__contains__`
membership-test API.

### Bug #22 — `/stats` policy single-sourced
**Files:** `app/middleware/auth.py`

`/stats` was in both EXCLUDE_PREFIXES and PROTECTED_PREFIXES — exclude check
fired first so behavior was "public". Removed the duplicate from PROTECTED;
docstring now documents the choice. If sales policy changes to require auth,
remove from EXCLUDE_PREFIXES instead.

### Bug #23 — Failed transfers in autonomous_tasks Pass 2 set `status='expired'`
**Files:** `app/tasks/autonomous_tasks.py`

When the daily recompute task's Pass 2 transfer attempt failed, the lead
stayed at whatever status it had — making the resurrection branch (which
fires only on `status='expired'`) never run for it. Now failed transfers
get `status='expired'` in a separate session for next-day retry.

### Bug #24 — `ScrapeLog.status` column widened + Alembic migration
**Files:** `app/models/scrape_log.py`, `alembic/versions/022_widen_scrape_log_status.py` (new)

Code wrote `'completed_with_errors'` (21 chars) into a `String(20)` column.
Widened to `String(30)`; new migration `022` applies the change.

### Bug #25 — Single-worker warning on `_rate_limit_store`
**Files:** `app/main.py`

Added explicit comment that the in-memory rate-limit dict is safe only
under single-worker deploy. Migrate to Redis (already in use for Celery)
before scaling out workers.

### Bug #26 — `LeadContact.to_dict` includes `existing_hotel_id` + `last_enriched_at`
**Files:** `app/models/lead_contact.py`

Frontend reading EH contacts couldn't tell parent; UI couldn't show
freshness. Both fields added.

### Bug #27 — `handleSetPrimary` invalidates lead detail
**Files:** `frontend/src/components/leads/LeadDetail.tsx`

Backend writes `lead.contact_*` when setting primary; lead header card reads
those. Was missing the invalidation; now invalidates both `['contacts', leadId]`
and `['lead', leadId]`.

### Bug #28 — Rolled into Bug #7 (insightly tier_display kept canonical 4 keys)

### Bug #29 — `/leads/{id}` PATCH has admin gate
**Files:** `app/routes/leads.py`

Was documented as admin-only but had no admin dep. Added `Depends(require_admin)`.

### Bug #30 — Edit handler uses `_get_user_email` helper
**Files:** `app/routes/dashboard.py`

Was duplicating JWT-decode boilerplate. Switched to the shared helper
defined at the top of the file.

### Bug #31 — `room_count` upper bound + length checks gated on `isinstance(str)`
**Files:** `app/routes/dashboard.py`

`room_count > 10000` now rejected. Length checks no longer call `str()` on
non-string values (was producing meaningless dict-repr lengths).

### Bug #32 — Newly-inserted contacts registered in dedup dict
**Files:** `app/routes/contacts.py`

Two payload entries with the same name in a single enrichment could both
insert. Now the new contact is registered in `existing_by_norm` so the
second entry hits the update path.

### Bug #33 — `enrich_lead` reject path doesn't auto-reject approved/pushed leads
**Files:** `app/routes/contacts.py`

Re-enriching an approved lead and blindly flipping it to rejected lost the
Insightly link reference and didn't delete from CRM. Now the rejection
signal is recorded as a `key_insights` note instead, and sales decides.

### Bug #34 — `prepare_lead` caps `description` and `key_insights` at 5000 chars
**Files:** `app/services/lead_factory.py`

Some scrapers paste full article bodies into these fields. 5000 cap matches
the dashboard PATCH validation cap.

---

## LOW (6)

### Bug #35 — UTF-8 mojibake in `autonomous_tasks.py` cleaned
**Files:** `app/tasks/autonomous_tasks.py`

Box-drawing artifacts (`â•`) and curly-quote artifacts (`â€"`, `â€™`) re-encoded
to clean UTF-8.

### Bug #36 — Unknown `/api/*` paths return JSON 404
**Files:** `app/main.py`

Catch-all SPA route was returning the React shell with status 200 for unknown
`/api/*` paths. Added explicit `/api/{full_path:path}` route returning JSON 404,
registered before `serve_spa`.

### Bug #37 — `set_auth_cookie` Secure-flag behavior documented
**Files:** `app/routes/auth.py`

Added explicit docstring noting that `secure=IS_PRODUCTION` is keyed off
the `ENVIRONMENT` env var at module import. Dev environments behind HTTPS
reverse proxies need to set `ENVIRONMENT=production` if they want Secure.

### Bug #38 — `_fetch_all_leads` 10k cap with warning
**Files:** `app/services/insightly.py`

Pagination loop now caps at 10,000 leads to prevent unbounded scans of
account-wide CRM lists. Logged warning recommends switching to
`delete_leads_by_ids` (using stored `insightly_lead_ids`).

---

## New files

- `tests/test_audit_2026_05_05.py` — 30 regression tests locking in the
  contracts these fixes restored.
- `scripts/normalize_brand_tiers.py` — one-shot DB migration to map any
  non-canonical brand_tier values to `tier5_skip`. Dry-run by default;
  pass `--apply` to rewrite. Adds a timestamped note to `notes` field.
- `alembic/versions/022_widen_scrape_log_status.py` — widens
  `scrape_logs.status` from VARCHAR(20) to VARCHAR(30).

---

## Modified files

```
app/main.py
app/middleware/auth.py
app/models/lead_contact.py
app/models/scrape_log.py
app/routes/auth.py
app/routes/contacts.py
app/routes/dashboard.py
app/routes/existing_hotels.py
app/routes/leads.py
app/routes/scraping.py
app/schemas.py
app/services/existing_hotel_scorer.py
app/services/insightly.py
app/services/lead_data_enrichment.py
app/services/lead_factory.py
app/services/lead_transfer.py
app/services/rescore.py
app/services/utils.py
app/tasks/autonomous_tasks.py
frontend/src/components/leads/LeadDetail.tsx
frontend/src/hooks/useLeads.ts
```

---

## Recommended deployment steps

1. **Backup the production DB** before deploying.
2. Apply migration `022_widen_scrape_log_status.py`:
   ```
   alembic upgrade head
   ```
3. Run the brand-tier normalization (dry-run first, then apply):
   ```
   python -m scripts.normalize_brand_tiers
   python -m scripts.normalize_brand_tiers --apply
   ```
4. Restart Celery worker and beat (autonomous_tasks changed):
   ```
   celery -A app.tasks.celery_app worker --restart
   celery -A app.tasks.celery_app beat --restart
   ```
5. Restart the FastAPI app.
6. Verify the parity routes work:
   ```
   curl -X POST http://app/api/existing-hotels/1/contacts/1/toggle-scope
   ```
7. Run the new test suite: `pytest tests/test_audit_2026_05_05.py -v`

---

## Things deliberately NOT changed

- **Scraper-time tier filter.** This audit fixed how the system handles
  non-canonical tier values that already exist in the DB; it did not change
  ingestion-time filtering. If your scrapers are still classifying budget
  hotels as `tier6_midscale`, the normalize script will reclassify them as
  `tier5_skip` (out-of-scope) and sales can ignore. Long-term: tighten the
  scrape filter to never produce non-canonical values.
- **The 7-tier vs 5-tier debate as a product decision.** Per Jay's directive
  on 2026-05-05, the canonical set is 5 tiers and that's the system going
  forward.
- **Drift cleanup in `rescore.py:64-149` vs `contact_scoring.py:91-219`.**
  Documented in audit notes as systemic but out of scope for this batch
  — separate PR recommended.
- **In-memory rate-limit store migration to Redis.** Documented as
  required-before-scale, kept as-is for current single-worker deploy.

---

*Audit and patches by Claude Opus 4.7 in collaboration with Jay
(JA Uniforms). 2026-05-05.*
