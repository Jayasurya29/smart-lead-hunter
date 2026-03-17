# Smart Lead Hunter — Full Application Rating & Issues Report

**Date:** February 18, 2026
**Scope:** Complete codebase (backend + frontend), ~20,000 LOC across 70+ files

---

## OVERALL GRADE: **C+ (58/100)**

| Category | Score | Max | Grade |
|---|---|---|---|
| **Backend Code Quality** | 15 | 25 | C |
| **Backend Security** | 12 | 25 | D+ |
| **Architecture** | 14 | 20 | B- |
| **Error Handling** | 8 | 15 | D |
| **Performance** | 9 | 15 | C |
| **Frontend (React)** | --- | --- | **B-** (separately) |

The backend domain logic (scoring, brand tiers, pipeline) is excellent. The infrastructure and security layers need significant work. The React frontend was well-structured but had broken endpoint connections — now fixed.

---

## FRONTEND ISSUES FOUND & FIXED

### Route/Endpoint Mismatches (ALL FIXED)

| # | Frontend Called | Backend Expects | Status |
|---|---|---|---|
| 1 | `POST /dashboard/leads/{id}/approve` | `POST /leads/{id}/approve` (JSON) or `/api/dashboard/leads/{id}/approve` (HTML) | **FIXED** → uses JSON REST endpoint |
| 2 | `POST /dashboard/leads/{id}/reject` | `POST /leads/{id}/reject` (JSON) | **FIXED** → uses JSON REST endpoint |
| 3 | `POST /dashboard/leads/{id}/restore` | Only exists at `/api/dashboard/leads/{id}/restore` | **FIXED** → correct path |
| 4 | `DELETE /leads/{id}` (hard-delete) | Should be `POST /api/dashboard/leads/{id}/delete` (soft-delete) | **FIXED** → soft-delete |
| 5 | `PATCH /dashboard/leads/{id}/edit` | `PATCH /api/dashboard/leads/{id}/edit` | **FIXED** → correct path |
| 6 | `POST /dashboard/leads/{id}/enrich` | `POST /api/dashboard/leads/{id}/enrich` | **FIXED** → correct path |
| 7 | `GET /dashboard/leads/{id}/contacts` | `GET /api/dashboard/leads/{id}/contacts` | **FIXED** → correct path |
| 8 | `POST /dashboard/scrape` | `POST /api/dashboard/scrape` | **FIXED** → correct path |
| 9 | `POST /dashboard/discovery` | `POST /api/dashboard/discovery/start` | **FIXED** → correct path + payload |
| 10 | SSE checks `d.status === 'complete'` | Backend sends `d.type === 'complete'` | **FIXED** in all 3 modals |

### Other Frontend Fixes Applied

| # | Issue | Fix |
|---|---|---|
| 11 | `X-API-Key` header blocked by CORS (missing from `allow_headers`) | Added to backend CORS config |
| 12 | Auth check used `/stats` (may not require auth) | Changed to `/api/auth/verify` |
| 13 | No `Accept: application/json` header → some endpoints return HTML | Added to API client defaults |
| 14 | Vite proxy missing `changeOrigin: true` | Added to all proxy entries |
| 15 | TypeScript error: `Filters` type incompatible with `Record<string, string>` | Exported `LeadFilterState` type, used consistently |
| 16 | Missing API functions: `unsaveContact`, `deleteContact`, `triggerExtractUrl` | Added to `api/leads.ts` |

---

## BACKEND ISSUES (From Previous Audit)

### CRITICAL (Will crash or corrupt data)

| # | File | Issue |
|---|---|---|
| C-01 | `intelligent_pipeline.py:898` | `ClassificationResult` constructed with wrong field names (`reason=` vs `reasoning=`, nonexistent `category=`). Crashes when circuit breaker opens. |
| C-02 | `scraping_tasks.py:133,150,230,308` | Functions return `dict` where `Optional[int]` is expected. Dicts are truthy → errors counted as successes. |
| C-03 | `main.py:2333` | Missing `f` prefix on SSE yield — emits literal `{json.dumps(...)}` instead of JSON. |
| C-04 | `intelligent_pipeline.py:979-980` | Double `record_success()`/`record_failure()` skews circuit breaker. |
| C-05 | `intelligent_pipeline.py:49-59` | `_safe_int` defined twice — first definition is dead code. |

### HIGH (Security or data integrity)

| # | File | Issue |
|---|---|---|
| H-01 | `middleware/auth.py:108-110` | Auth middleware allows ALL requests when `API_AUTH_KEY` not set. |
| H-02 | `main.py:1425,1451,2702,2871+` | CSRF protection missing on 9 dashboard mutation endpoints. |
| H-03 | `main.py:558, contact_enrichment.py:711` | Gemini API key passed in URL query parameter (leaks to logs). |
| H-04 | `main.py:2847` | Raw `str(e)` in enrichment error response bypasses `_safe_error()`. |
| H-05 | `scraping_tasks.py:212` | `asyncio.Lock()` at module scope — wrong event loop in Celery. |
| H-06 | `alembic/versions/001_initial_indexes.py:48,61` | References wrong table `lead_sources` and wrong column `opening_status`. |

### MEDIUM

| # | File | Issue |
|---|---|---|
| M-01 | 5 service files (~20 locations) | `datetime.now()` (naive) mixed with timezone-aware `local_now()` |
| M-02 | `lead_contact.py:69-70` | `datetime.utcnow()` — deprecated since Python 3.12 |
| M-03 | `contact_enrichment.py:506` | Blocking `DDGS().text()` in async context |
| M-04 | `insightly.py` (every method) | New `httpx.AsyncClient` per request — no connection pooling |
| M-05 | `scraping_tasks.py:325` | New pipeline instance per `process_scraped_content` call |
| M-06 | `main.py:48-50` | `_pending_*` dicts have TTL (good) but no max-size cap |
| M-07 | `source_learning.py:133` | Unsafe `SourceLearning(**learning_data)` deserialization |
| M-08 | `requirements.txt` | Missing `duckduckgo-search` and `flower` packages |
| M-09 | `Dockerfile:67` | `COPY . .` may include `.env` in image |

### LOW

| # | Issue |
|---|---|
| L-01 | `source_config.py:123` — year computed at module load; won't update across year boundary |
| L-02 | `contact_enrichment.py:2276` — `if True:` dead conditional |
| L-03 | `contact_validator.py:261` — corrupted Unicode character in comment |
| L-04 | `alembic/env.py:55` — `os.getenv("DATABASE_URL")` can return `None` → crash |
| L-05 | `scraping_engine.py` — MD5 for content hashing (not security-sensitive, but weak) |

---

## FRONTEND QUALITY (Separate Rating)

### Grade: **B- (72/100)**

**Strengths:**
- Clean component architecture (pages → components → hooks → API layer)
- Proper React Query usage with cache invalidation
- Good TypeScript types matching backend Pydantic schemas
- Beautiful UI with Tailwind + Lucide icons
- SSE streaming modals with real-time log display
- Proper auth flow with API key storage

**Issues (now fixed):**
- 10 route mismatches between frontend and backend
- SSE event type mismatch (`status` vs `type`)
- Missing CORS header for `X-API-Key`
- Hard-delete used where soft-delete was needed
- TypeScript type compatibility issue with filter state

**Remaining improvements needed:**
- No error toast/notification system (errors only show in console)
- No optimistic updates on mutations (UI waits for refetch)
- No loading states on individual action buttons (only global loading)
- `strict: false` in tsconfig — should be `true` for production
- No unit tests for components or hooks

---

## WHAT'S ACTUALLY GREAT

1. **Scoring system** — 440 brands across 5 tiers, 100-point system with word-boundary matching. Genuinely impressive domain modeling.
2. **Pipeline architecture** — 5-stage classify→extract→validate→qualify→deduplicate is well-designed.
3. **React frontend structure** — Clean separation of concerns, proper hooks, good TypeScript.
4. **SSE streaming** — Real-time scrape/extract/discovery progress with live log display.
5. **Smart deduplication** — Fuzzy matching with location-aware boost/penalty, Unicode normalization.
6. **Contact enrichment** — Multi-layer search (DuckDuckGo → LinkedIn → Apollo → Gemini verification).
7. **Gold URL tracking** — Smart scrape mode that remembers which URLs produce leads.

---

## TOP 5 FIXES TO DO NEXT

1. **Fix C-01** (ClassificationResult crash) — 1 line change, prevents total pipeline failure
2. **Fix C-02** (return type lies) — change `return {...}` to `return None` in 4 places
3. **Fix C-03** (missing f-string) — add `f` prefix on line 2333
4. **Fix H-01** (auth bypass) — fail-closed when `API_AUTH_KEY` not set
5. **Add `duckduckgo-search`** to requirements.txt — prevents ImportError in discovery
