# Smart Lead Hunter — Audit Fixes 2026-05-06

This CHANGELOG documents the fixes applied during the second
production-grade audit (2026-05-06). It builds on the 38-bug audit from
2026-05-05 (`CHANGELOG_AUDIT_2026_05_05.md`) and the same-day follow-up
(`Changelog audit 2026 05 06.md` covering bugs #18a/#18b/#18c).

The 2026-05-06 audit identified **3 critical bugs, 3 high-severity
bugs, and 5 high-value improvements (HV-1 through HV-5).** All ten
items are addressed in this batch.

**Verification status:**
- 403 unit/regression tests pass (43 new tests in
  `tests/test_audit_2026_05_06.py` lock in the contracts these fixes
  restored, zero regressions in the existing 360+ tests).
- `ruff check` clean across every modified file.
- Compile-checked end-to-end on Python 3.12.

---

## CRITICAL (3)

### CRIT-1 — `/revenue/*` and `/discovery/*` are now authenticated
**Files:** `app/middleware/auth.py`, `app/main.py`, `tests/test_audit_2026_05_06.py`

`PROTECTED_PREFIXES = ("/api/", "/leads", "/sources", "/scrape")` did not
include `/revenue` or `/discovery`. The middleware's catch-all branch
(`if not path.startswith(self.PROTECTED_PREFIXES): await self.app(...)`)
let those paths through with no JWT and no API key.

This left 9 endpoints publicly callable on the LAN — including two
mutating POSTs (`/revenue/bulk-update`, `/revenue/update/{lead_id}`)
that recompute revenue on every lead. `/discovery/queries` was a smaller
information leak (operational query intelligence) but same root cause.

Added both prefixes to `PROTECTED_PREFIXES` (auth) AND
`_RATE_LIMITED_PREFIXES` in `app/main.py` so the rate-limit policy
stays in sync — drift between those two lists is exactly how the
original gap appeared.

### CRIT-2 — Canonical 5-tier `brand_tier` enforced at the AI writeback layer
**Files:** `app/services/lead_data_enrichment.py`, `app/config/canonical_tiers.py`

The 2026-05-05 audit declared Bug #7 fixed ("tightened all writers to
reject non-canonical values"). Schemas, dashboard PATCH, and the
existing_hotels apply layer were correct. But the Gemini AI layer was
still producing AND persisting non-canonical 7-tier values:

  1. **Grounding prompt** (`_build_grounding_prompt`, line 394) told
     Gemini to emit `tier1_ultra_luxury | tier2_luxury |
     tier3_upper_upscale | tier4_upscale | tier5_upper_midscale |
     tier6_midscale | tier7_economy` — explicitly asking for 7 tiers.
  2. **Grounding response validator** (`_validate_grounding_response`)
     accepted all 7 as valid.
  3. **6-stage extraction JSON schema** (the `_extract_data` schema)
     enumerated all 7 in the `enum` array.
  4. **`_collect_field_extraction` apply layer** (line ~2780) used a
     7-tier whitelist and copied any of them into the result dict
     when `current_tier` was a sentinel.
  5. **`batch_full_refresh` apply layer** (nightly Celery task,
     line ~3215) used a 7-tier whitelist and wrote any of them
     directly into `lead.brand_tier`.

So a lead with `brand_tier='unknown'` running through the nightly
full-refresh could have `tier6_midscale` or `tier7_economy` written
straight to the DB, in violation of the canonical 5-tier business
rule.

**Fix:** Single source of truth for tier coercion in this module —
`_coerce_brand_tier_to_canonical()` maps known non-canonical aliases
(`tier5_upper_midscale`, `tier6_midscale`, `tier7_economy`,
`tier8_budget`, etc.) to `tier5_skip` and drops everything else. Both
Gemini prompts (grounded one-shot AND 6-stage extraction) now ask for
the canonical 5 only; the JSON schema enum lists the canonical 5
only; both apply layers route through the coercer.

**Tests:** 6 new tests in `TestCrit2CanonicalTierCoercion` lock in the
contract — the coercer normalizes 7-tier strings to `tier5_skip`,
preserves canonical strings, drops garbage, and the prompt + schema no
longer mention non-canonical names.

### CRIT-3 — Hot-path indexes added via migration 023
**Files:** `alembic/versions/023_hot_path_indexes_and_unaccent.py`,
`app/models/potential_lead.py`, `app/models/existing_hotel.py`,
`tests/conftest.py`

Every `save_lead_to_db` call ran `SELECT … WHERE hotel_name_normalized =
?` against an unindexed column. List endpoints filtered by
`timeline_label`, `brand_tier`, `zone`, `opening_year` — none indexed.
At ~5k rows the impact was tolerable; at 100k it would crater dedup
throughput.

Migration 023 adds:

  - `ix_potential_leads_hotel_name_normalized`
  - `ix_existing_hotels_hotel_name_normalized`
  - `ix_potential_leads_timeline_label`
  - `ix_potential_leads_brand_tier`, `ix_existing_hotels_brand_tier`
  - `ix_potential_leads_zone`, `ix_existing_hotels_zone`
  - `ix_potential_leads_opening_year`
  - `ix_potential_leads_last_user_review_at` (new column from HV-4)

The migration also enables the Postgres `unaccent` and `pg_trgm`
extensions and adds GIN trigram expression indexes on
`unaccent(lower(hotel_name))` for both tables — used by HIGH-2 / HV-3.
The model files reflect the same indexes via `index=True` so new
deployments don't depend on the migration.

`tests/conftest.py` enables the same extensions on the test DB so
route-level integration tests exercise the same SQL the production
server emits.

---

## HIGH (3)

### HIGH-1 — `PATCH /auth/users/{id}` clears `_user_active_cache` on `is_active` flips
**File:** `app/routes/auth.py`

`DELETE /auth/users/{id}` (Bug #17 fix from the 2026-05-05 audit)
correctly invalidates the middleware's TTL cache. `PATCH /auth/users/{id}`
also toggles `is_active` (line 608-609) and was NOT clearing the cache
— admin deactivation via PATCH left the user with up to 60s of
post-deactivation access. Now the PATCH handler invalidates on either
direction (deactivation or re-activation) when `is_active` actually
changed.

### HIGH-2 + HV-3 — Diacritic-insensitive search across `/leads` and `/api/existing-hotels`
**Files:** `app/shared.py`, `app/routes/leads.py`,
`app/routes/existing_hotels.py`, `tests/conftest.py`

`ILIKE` is case-insensitive but not accent-insensitive. Sales searches
for `cafe` against rows stored as `Hôtel du Café` were missing.
`Curaçao` not matching `Curacao` was wrong on every Caribbean hotel.

`app/shared.unaccent_ilike()` builds a SQLAlchemy clause that compiles
to `unaccent(lower(col)) ILIKE unaccent(lower(?))` on Postgres, backed
by the GIN trigram + unaccent indexes from migration 023. A `@compiles`
dispatch on `_Unaccent` makes the function a pure pass-through on
SQLite so the test suite still runs without the extension installed.

Threaded through:
  - `app/shared.apply_lead_filters` (powers `/leads`, `/leads/hot`,
    `/leads/florida`, `/leads/caribbean`)
  - `app/routes/existing_hotels.list_existing_hotels` (search/state/city)
  - `app/routes/leads.export_leads_excel` (search + location filters)

### HIGH-3 — `/leads/export` and `/api/existing-hotels/export-csv` capped at 25k rows
**Files:** `app/routes/leads.py`, `app/routes/existing_hotels.py`

Both export endpoints loaded every matching row into memory and built
the response in a single synchronous pass inside the request handler.
At 100k matching rows that's a ~1GB memory spike + several seconds of
event-loop stall, which would visibly degrade SSE streams (Smart Fill,
discovery) and risk OOM on Render/Heroku-class instances.

Cap is `EXPORT_ROW_CAP = 25_000`. When hit:
  - `/leads/export` sets `X-Result-Truncated: 1` and `X-Result-Cap`
    response headers.
  - `/api/existing-hotels/export-csv` returns `truncated: true` and
    `cap: 25000` in the JSON body.

The existing-hotels export-csv loop also fixed a latent
`AttributeError` on `h.phone` — column dropped in migration 018, same
class of bug as audit-1 Bug #4. Now uses canonical `contact_phone` /
`contact_name` with `gm_*` legacy fallbacks.

---

## HIGH-VALUE IMPROVEMENTS (5)

### HV-1 — Tightened Gemini extraction prompts for 5-tier output and `tier5_skip` short-circuit
**File:** `app/config/canonical_tiers.py`

Folded into the CRIT-2 commit. The `build_tier_rules_prompt_block()`
function — used by the 6-stage extraction prompt — now explicitly tells
Gemini that JA Uniforms is a 4-star+ supplier, instructs it to output
`tier5_skip` for any budget / select-service / extended-stay brand, and
forbids it from inventing tier names. Prompt savings: budget-brand
classifications stop the AI mid-research instead of running 6 stages
to discover JA doesn't pursue them.

### HV-2 — Pre-opening digest task emails sales when leads cross the procurement window
**Files:** `app/services/notifications.py` (new),
`app/tasks/autonomous_tasks.py`, `app/tasks/celery_app.py`,
`.env.example`

The 6-12 month opening window is where uniform decisions get made.
Without notification, sales had no surface to catch leads quietly
sliding from HOT → URGENT → EXPIRED.

`pre_opening_digest_task` runs at 9:00 AM Mon-Fri (just before
`recompute_timeline_labels`), scans active `URGENT`/`HOT` leads, and
emails sales an HTML digest with hotel name, location, opening date,
months out, brand tier, and lead score. Crossing dedup uses a tagged
substring in `notes` (`[SLH:digest_notified]`) so the same lead doesn't
appear in tomorrow's email.

`DIGEST_RECIPIENTS` env var (CSV emails) controls who gets the email;
unset → task logs to stdout instead of failing.

### HV-3 — `unaccent` + `pg_trgm` extensions enabled (combined with HIGH-2)
See HIGH-2 above. The `pg_trgm` index is what makes typo-tolerant
matching (`similarity('Sandls', 'Sandals')`) future-friendly — a
follow-up can switch the search filter from ILIKE to `similarity > 0.4`
without another migration.

### HV-4 — `last_user_review_at` column + stale-review filter and sort
**Files:** `app/models/potential_lead.py`,
`app/routes/dashboard.py`, `app/routes/leads.py`, migration 023

A new `potential_leads.last_user_review_at` timestamp records when
sales last touched a lead via the dashboard (edit / approve / reject /
restore). Distinct from `updated_at`, which any system process bumps
(Smart Fill, rescore, geo-enrich, batch_full_refresh).

`GET /leads` supports two new params:
  - `review_stale_days=N` → returns only leads last reviewed > N days
    ago (or never reviewed)
  - `sort=review_stale` → orders staleest first, NULLs (never
    reviewed) at the top, tie-break by `lead_score` DESC

This catches the failure mode where a HOT lead from 21 days ago has
slipped to URGENT but is buried at the bottom of a score-sorted list
because nobody's looked at it.

### HV-5 — Apply layer refuses to downgrade a valid `brand_tier` to `tier5_skip`
**File:** `app/services/lead_data_enrichment.py`

Folded into the CRIT-2 commit. Even with a clean coerced value, the
apply layers now explicitly check
`is_downgrade_to_skip = (coerced == 'tier5_skip' and current_is_valid
and current_tier != 'tier5_skip')` and decline to write. A single
Gemini confidence dip on a Sandals property should not clobber a
manually-set `tier2_luxury`. Re-classifications between non-skip valid
tiers (`tier4_upscale → tier3_upper_upscale` after a brand promotion)
are still allowed under `mode='full'`.

---

## New files

- `tests/test_audit_2026_05_06.py` — 43 regression tests covering every
  fix in this batch. Run as part of every CI cycle.
- `app/services/notifications.py` — pre-opening digest renderer + email
  sender (HV-2).
- `alembic/versions/023_hot_path_indexes_and_unaccent.py` — migration
  for hot-path indexes, `unaccent` + `pg_trgm` extensions, and the
  `last_user_review_at` column.
- `CHANGELOG_AUDIT_2026_05_06.md` — this file.

---

## Modified files

```
.env.example
app/config/canonical_tiers.py
app/main.py
app/middleware/auth.py
app/models/existing_hotel.py
app/models/potential_lead.py
app/routes/auth.py
app/routes/dashboard.py
app/routes/existing_hotels.py
app/routes/leads.py
app/services/lead_data_enrichment.py
app/shared.py
app/tasks/autonomous_tasks.py
app/tasks/celery_app.py
tests/conftest.py
```

---

## Recommended deployment steps

1. **Backup the production DB.**
2. Apply migration 023:
   ```
   alembic upgrade head
   ```
   Note: enabling `unaccent` and `pg_trgm` extensions requires
   superuser on the target DB. Production DBs already have `vector`
   enabled the same way, so this should be a non-event.
3. Set the new env vars:
   ```
   DIGEST_RECIPIENTS=sales@jauniforms.com,jay@jauniforms.com
   SMTP_HOST=...    SMTP_USER=...    SMTP_PASSWORD=...
   ```
4. Restart Celery worker and beat (autonomous_tasks + celery_app
   changed):
   ```
   celery -A app.tasks.celery_app worker --restart
   celery -A app.tasks.celery_app beat --restart
   ```
5. Restart the FastAPI app.
6. Verify the new test suite: `pytest tests/test_audit_2026_05_06.py -v`
7. Smoke test:
   - `curl -X POST http://app/revenue/bulk-update` should now return 401.
   - `curl http://app/discovery/queries` should now return 401.
   - `curl http://app/api/leads?search=Café` should match rows stored as
     "Cafe".
8. After 24h, check that the daily digest task ran (Celery flower or
   `tail -f` on the Celery log; expect a "Pre-opening digest:
   complete" line at 9:00 AM).

---

## Things deliberately NOT changed

Carried forward from prior audits:

- **In-memory rate-limit + login-attempts state.** Single-worker only;
  audit-1 Bug #25 documents the constraint. Migrate to Redis when
  scaling to multi-worker.
- **`existing_hotels` legacy columns** — migration 019 still pending.
- **Sequential Gemini calls in contact enrichment** — Vertex per-project
  QPS pin at ~50 concurrent. Switch to a request batcher or split
  projects when sustained concurrency rises.

These are tracked as architecture concerns, not bugs.

---

*Audit and patches by Claude Opus 4.7 in collaboration with Jay
(JA Uniforms). 2026-05-06.*
