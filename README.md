<div align="center">

# 🏨 Lead Generator

### *AI-powered hotel sales intelligence — built for the 6-month uniform window*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vertex AI](https://img.shields.io/badge/Gemini%202.5%20Flash-Vertex_AI-4285F4?logo=google&logoColor=white)](https://cloud.google.com/vertex-ai)
[![Tests](https://img.shields.io/badge/tests-360_passing-success?logo=pytest&logoColor=white)](#)
[![Audit](https://img.shields.io/badge/audit-2026.05.06-blueviolet)](./CHANGELOG_AUDIT_2026_05_05.md)
[![License](https://img.shields.io/badge/license-Internal-lightgrey)](#license)

[**Pipeline**](#-the-pipeline) · [**Why**](#-why-this-exists) · [**Stack**](#-tech-stack) · [**Quick Start**](#-quick-start) · [**Architecture**](#-architecture)

</div>

---

## ⚡ At a glance

> **Hotel uniforms get bought 6+ months before the ribbon-cutting.**
> Reach the GM in week 3 and you've already lost — corporate procurement made the call before staff were even hired.

Smart Lead Hunter is the system that surfaces *who actually controls the uniform decision*, *at the right moment in the property's lifecycle*, and routes them to JA Uniforms' sales team **before the competition even knows the property exists**.

It's not a CRM. It's not a scraper. It's a **decision engine** built around one immutable business reality: **timing is everything**, and the right contact for a Marriott pre-opening in Q4 2026 is *not* the same person you'd email about a renovation reopening of an Aman in Aruba.

---

## 🎯 Why this exists

| Most B2B sales tools say | We say |
| --- | --- |
| "Find me hotel decision-makers" | *"Find me the right person to email at this exact moment in this property's lifecycle, given its operating model and tier."* |
| "Score leads 1–100" | *"Score leads against a 100-pt rubric where timing alone is worth 25 points — because procurement closes 6 months out."* |
| "Enrich with AI" | *"Run a 9-iteration researcher that reasons about who replaced whom, verifies current employment, and rejects stale press releases."* |
| "Sync to your CRM" | *"Push to Insightly only after CRM-success — fail closed, no zombie 'approved' rows with empty CRM IDs."* |

This product exists because for hospitality uniform sales, **a generic lead is worse than no lead** — it costs Gemini calls, Serper queries, sales-rep time, and corrupts the pipeline's signal-to-noise.

---

## 🧬 The pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       1.  DISCOVERY & INGEST                            │
│   17 trade sources · 147 geo zones (USA + Caribbean) · Geoapify         │
│   → potential_leads                                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                       2.  SMART FILL  (3-stage pipeline)                │
│   Stage 1  Project-type classifier  (Phase A — saves ~40 calls/dud)     │
│            new_opening · renovation · rebrand · reopening ·             │
│            conversion · ownership_change · residences_only → REJECT     │
│   Stage 2  Branched query construction + Serper search                  │
│   Stage 3  Gemini 2.5 Flash extraction with grounding                   │
│            → opening_date, tier, rooms, brand, mgmt, owner, address     │
├─────────────────────────────────────────────────────────────────────────┤
│                3.  ITERATIVE CONTACT RESEARCHER  (v5)                   │
│   Phase B   Project-type gate (residences_only → auto-reject)           │
│   Iter 1    Discovery — owner / operator / project stage                │
│   Iter 2    GM hunt + cascade (DOSM/Revenue/Area-GM when GM unknown)    │
│   Iter 2.5  Property dept heads (HOT/URGENT only)                       │
│   Iter 3    Corporate / owner hunt with city/state + former-names       │
│   Iter 4    LinkedIn URL resolution per candidate                       │
│   Iter 5    Current-role verification (kills stale press contacts)      │
│   Iter 5.5  Regional fit check for ambiguous-region titles              │
│   Iter 6    Strategist reasoning — assigns P1/P2/P3/P4 with rationale   │
│   Iter 6.5  Employment verification on P1/P2 only (cost-controlled)     │
│   → lead_contacts (with evidence array + tier-trust per source)         │
├─────────────────────────────────────────────────────────────────────────┤
│                       4.  SCORING & REVENUE                             │
│   Lead score   100-pt rubric (tier · timing · location · rooms · …)     │
│   Contact      Tier × scope × strategist priority floor                 │
│   Revenue      3 formulas validated 10/10 against SAP sales history     │
├─────────────────────────────────────────────────────────────────────────┤
│                       5.  OUTPUT & SYNC                                 │
│   React 18 dashboard · Map view · Client intel · Insightly CRM push     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔥 Feature highlights

<table>
<tr>
<td width="50%" valign="top">

### 🤖 Iterative contact researcher

Nine-pass research loop that behaves like a human analyst.
Each pass builds on what the previous one learned.

- **Phase B classification gate** rejects residences-only towers before any enrichment runs (saves ~30 Serper + 10 Gemini calls per dud)
- **GM-missing cascade** pivots to DOSM, DOR, Area GM, Task Force GM when no GM is named yet
- **Strategist reasoning pass** uses Gemini to reason about who's *actually* running this property *right now* — assigns P1–P4 priorities with written justification
- **Employment verification on P1/P2 only** — kills stale press-release ghosts without burning Gemini on long-tail noise

</td>
<td width="50%" valign="top">

### ⚡ Smart Fill / Full Refresh

One click runs a 3-stage Gemini pipeline that fills in everything the trade-press scrape missed.

- Opening date, room count, brand tier, full address, mgmt company, owner, developer
- `INVALID_TIER_SENTINELS` guard means "unknown" Gemini responses can't trample manual edits
- **Specificity-aware regression guard** prevents downgrading "March 2026" to a vaguer "Late 2026"
- Hybrid geocoding — Geoapify-first against grounded address, Vertex grounding coords as fallback with 50km sanity check

</td>
</tr>
<tr>
<td width="50%" valign="top">

### 🎯 Phase-aware contact routing

Three contact phases keyed to the timeline window:

| Phase | When | Target |
|---|---|---|
| **1** | 12+ mo | Mgmt co corporate procurement |
| **2** | 6–11 mo | Incoming pre-opening GM |
| **3** | 3–5 mo | Property dept heads |

Operating model decides the target as much as phase. A franchised Marriott routes to its mgmt co (Crescent / Aimbridge / Highgate). A managed Four Seasons routes to brand regional ops.

</td>
<td width="50%" valign="top">

### 🏅 Trust-tier evidence system

Every contact ships with an evidence array — quoted snippets + source URLs tagged by trust tier:

```
primary    → Operator's own site
official   → Press wires (PRNewswire, BusinessWire)
trade      → Trade pubs (HotelMgmt, HospitalityNet)
aggregator → RocketReach, ZoomInfo (often stale)
indirect   → LinkedIn posts, secondary mentions
unknown    → Treat as weak signal
```

Sales sees *why* a contact was surfaced, at a glance.

</td>
</tr>
<tr>
<td width="50%" valign="top">

### 💰 Revenue potential engine

Three formulas, **all validated 10/10 against JA Uniforms' SAP sales history**:

- **New Opening** — kit-cost × staff × climate × tier multiplier
- **Annual Recurring** — yearly spend × garment% × tier turnover rate
- **Rebrand** — flag-change replacement = 70% of new-opening revenue

Tier parameters cross-checked against CBRE, STR/CoStar, AHLA, USALI, OMR Research benchmarks. Climate factors span 16 US markets + Caribbean.

</td>
<td width="50%" valign="top">

### ⏱️ Timeline buckets (locked, enforced daily)

| Bucket | Months out | Status |
|---|---|---|
| **EXPIRED** | <3 or past | Too late — don't pursue |
| **URGENT** | 3–5 | Tight but possible |
| **HOT** | 6–11 | 🎯 Sweet spot |
| **WARM** | 12–17 | Planning phase |
| **COOL** | 18+ | Watchlist |
| **TBD** | Year only | Needs a month |

Celery recomputes daily 09:30 ET. **Resurrection path**: a previously expired lead whose date moves forward auto-flips back to `new`.

</td>
</tr>
</table>

---

## 📊 Lead scoring

| Category | Max | Calibration |
|---|---|---|
| Brand Tier | **25** | Ultra Luxury 25 · Luxury 20 · Upper Upscale 15 · Upscale 10 |
| Timing | **25** | This year 25 · Next year 18 · +2yr 12 · +3yr+ 6 |
| Location | **20** | Florida 20 · Caribbean / strong US 15 · Other US 10 |
| Room Count | **15** | 500+ 15 · 300+ 12 · 150+ 9 · 100+ 6 |
| Contact Info | **8** | Name 3 · Email 3 · Phone 2 |
| New Build | **4** | New 4 · Conversion 3 · Renovation 2 |
| Existing Client | **3** | Known brand relationship bonus |

Short brand names (≤4 chars) match with `\b` word-boundary regex — *"Trump International"* no longer falsely matches the *"tru"* tier-5 brand.

**Categories:** 🔥 Hot 70+ · ⚡ Warm 50–69 · ❄️ Cool 30–49 · 💧 Cold <30

---

## 🗺️ Three interconnected systems

| System | Source | Purpose |
|---|---|---|
| `potential_leads` | Scraped trade press | Pre-opening pipeline (Pipeline tab) |
| `existing_hotels` | Geoapify discovery | Prospecting map (Map tab) |
| `sap_clients` | SAP B1 imports | Client intelligence (Clients tab) |

These are **fully independent** — conflating them has caused rework before. SAP billing addresses are *never* used for geocoding (those are corporate HQs, not property locations). Each system has its own approval/rejection workflow.

---

## 🛠️ Tech stack

<table>
<tr>
<th>Backend</th>
<th>Frontend</th>
<th>AI / Data</th>
</tr>
<tr>
<td valign="top">

- **FastAPI** (async)
- **Uvicorn** + pure-ASGI middleware
- **SQLAlchemy** async + asyncpg
- **PostgreSQL 16** + pgvector
- **Alembic** (16+ migrations)
- **Celery** + **Redis**
- **JWT** auth (access + refresh)
- **Pure ASGI middleware** for SSE compat

</td>
<td valign="top">

- **React 18** + **TypeScript 5**
- **Vite** + React Router
- **TanStack Query** (cache mgmt)
- **Tailwind CSS** + design tokens
- **Leaflet** + react-leaflet-cluster
- **lucide-react** icons
- Production build served by FastAPI

</td>
<td valign="top">

- **Vertex AI** Gemini 2.5 Flash + Lite
- **Google Serper** (web search)
- **Geoapify** (geocoding + bbox validation)
- **RocketReach / Apollo / Wiza**
- **Crawl4AI** + **Playwright** scraping
- **rapidfuzz** (dedup with NFKD)
- **spaCy** NER fallback
- **openpyxl** XLSX export

</td>
</tr>
</table>

### 💸 AI cost profile

Gemini runs on **Vertex AI Tier 1 paid plan** — not free tier. Cost-controlled via:

- ✅ Two-stage classifier→extractor split (cheap Lite for triage, full Flash for the survivors)
- ✅ Phase B residences rejection (skips ~30% of leads before any enrichment)
- ✅ Iter 6.5 verification only on P1/P2 contacts
- ✅ 7-day Redis extraction cache
- ✅ `us-central1` regional grounding endpoint (global endpoint produces phantom citations)

Current burn: **~$30/month** covered by initial $300 GCP credits.

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+ · Node 20+ · PostgreSQL 16 · Redis 7
- Google Cloud project with Vertex AI enabled + service account JSON
- API keys: Serper, Geoapify, Insightly, (optional) Apollo, Wiza

### 1 · Install

```powershell
# Backend
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
crawl4ai-setup
python -m spacy download en_core_web_sm

# Frontend
cd frontend
npm install
```

### 2 · Environment

Copy `.env.example` → `.env` and fill in:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://user:pw@host:port/db` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to Vertex AI service account JSON |
| `VERTEX_PROJECT` | Your GCP project ID |
| `VERTEX_LOCATION` | `us-central1` (regional — DO NOT use `global` for grounding) |
| `SERPER_API_KEY` | Web search |
| `GEOAPIFY_API_KEY` | Discovery + geocoding |
| `INSIGHTLY_API_KEY` | CRM sync |
| `APOLLO_API_KEY` | Contact enrichment fallback |
| `WIZA_API_KEY` | Email enrichment |
| `JWT_SECRET_KEY` | Auth token signing (32+ chars) |
| `API_AUTH_KEY` | API-key header auth |

### 3 · Database

```powershell
alembic upgrade head
python scripts\create_admin.py    # Seed first admin user
```

### 4 · Run the stack

Three PowerShell windows on Windows:

```powershell
# Window 1 — Web server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Window 2 — Celery worker (--pool=solo required on Windows)
celery -A app.tasks.celery_app worker --loglevel=info --pool=solo

# Window 3 — Celery beat scheduler
celery -A app.tasks.celery_app beat --loglevel=info
```

Or use the bundled launcher:

```powershell
.\start_slh.bat
```

### 5 · Frontend

```powershell
cd frontend

npm run dev         # Dev on :5173, proxies API to :8000
# OR
npm run build       # Prod build (FastAPI auto-serves dist/)
```

**Dashboard:** http://localhost:8000/new-hotels
**API docs:** http://localhost:8000/docs

---

## ⏰ Scheduled tasks

Business hours only (Mon–Fri 09:30–16:30 ET). App is shut down outside these windows.

| Task | Schedule | Queue | Purpose |
|---|---|---|---|
| `recompute_timeline_labels` | Daily **09:30** | maintenance | Refresh bucket labels + resurrection logic |
| `daily_health_check` | Mon–Fri **09:35** | maintenance | Gap cleanup, stale source deactivation |
| `smart_scrape` | Mon–Fri **10:00, 12:30, 15:30** | scraping | Brain picks which sources are due |
| `auto_enrich` | Mon–Fri **11:00, 14:00** | scraping | Top-5 unenriched HOT/URGENT leads |
| `weekly_discovery` | Thu **10:30** | scraping | Find new sources + leads from the web |

Timezone: `America/New_York`.

---

## 🏗️ Architecture

```
smart-lead-hunter/
├── 📁 app/
│   ├── main.py                  FastAPI bootstrap, ASGI middlewares
│   ├── database.py              Async SQLAlchemy setup (asyncpg)
│   ├── config_app.py            Settings & env vars
│   ├── shared.py                Cross-route state, helpers, require_ajax
│   │
│   ├── 📁 config/               Canonical knowledge: brands, procurement, project types
│   ├── 📁 models/               SQLAlchemy ORM (potential_lead, lead_contact, …)
│   ├── 📁 routes/               FastAPI routers (auth, leads, contacts, scraping, …)
│   ├── 📁 services/             Business logic, AI pipelines, scoring, dedup
│   ├── 📁 tasks/                Celery worker tasks + beat schedule
│   └── 📁 middleware/auth.py    Pure-ASGI auth (SSE-safe)
│
├── 📁 frontend/                 React 18 + TS + Vite + Tailwind
│   └── src/
│       ├── pages/               Dashboard · ExistingHotels · MapPage · ClientIntel · Sources · Users · Login
│       ├── components/          leads/ · layout/ · modals/ · stats/ · ui/
│       ├── hooks/               useLeads · useAuth · useSAP · useRevenue
│       └── api/                 client.ts (axios) · leads · sap · revenue · types
│
├── 📁 alembic/versions/         16+ schema migrations (001 → 022)
├── 📁 scripts/                  CLI utilities (run_pipeline, audit_geocodes, grade_sources, …)
├── 📁 tests/                    pytest + 360 passing tests (incl. audit regression suite)
├── 📁 cache/geoapify/           Geoapify response cache (committed for reuse)
├── 📄 docker-compose.yml        Postgres + Redis local-dev stack
├── 📄 start_slh.bat             Windows one-click launcher
├── 📄 revenue_formula_spec.md   Revenue formulas + validation log
├── 📄 CHANGELOG_AUDIT_2026_05_05.md   38-bug production-grade audit
└── 📄 CHANGELOG_AUDIT_2026_05_06.md   Follow-up fixes (NameError, audit log, CSRF)
```

---

## 🔧 Useful scripts

```powershell
# Run Full Refresh on a single lead with verbose stage logs
python -m scripts.test_full_refresh --lead-id 1252

# Dry-run (no DB write)
python -m scripts.test_full_refresh --lead-id 1252 --dry-run

# Backfill geocoordinates for leads missing lat/lng
python -m scripts.backfill_geocodes_leads --limit 50

# Audit all geocoords for state-bbox violations
python -m scripts.verify_geocodes

# Grade all active sources A–F based on yield + quality
python -m scripts.grade_sources

# Discover new trade-press sources
python -m scripts.discover_sources
```

---

## 🛡️ Production-grade audits

Two production-grade audits have been applied to this codebase, with full regression-test coverage:

### 📄 [CHANGELOG_AUDIT_2026_05_05.md](./CHANGELOG_AUDIT_2026_05_05.md)
38 bugs across 6 severity levels: lifecycle invariants, security, data integrity, scoring drift, CRM sync. 30 new regression tests added.

### 📄 [CHANGELOG_AUDIT_2026_05_06.md](./CHANGELOG_AUDIT_2026_05_06.md)
3 follow-up bugs introduced *by* the original audit and fixed within 24 hours, with a "lessons codified for next audit" section to prevent repeats:
- NameError shadowed by an imported `field` (silent no-op writes)
- Redundant CSRF guard
- Audit log lying about who made edits

**Test baseline:** 360 passing · 22 expected skips (DB-dependent in test env).

---

## 🔐 Lifecycle invariants (locked)

These are enforced by code AND regression tests. Violations are bugs:

1. `status='expired'` MUST NEVER persist in `potential_leads` — auto-transfer via `transfer_lead()`
2. `timeline_label` is always derived from `opening_date` via `get_timeline_label()` — never set manually
3. `lead_contacts` CHECK constraint: `lead_id` XOR `existing_hotel_id` (never both, never neither)
4. Auth middleware verifies `User.is_active` with 60-second TTL cache
5. React Query mutations always call `invalidateLeadEverywhere` for full cache refresh
6. Brand tier canonical set is **5 tiers ONLY**: `tier1_ultra_luxury` · `tier2_luxury` · `tier3_upper_upscale` · `tier4_upscale` · `tier5_skip`. Any code introducing tier6/tier7 is a bug.
7. SAP geocoding always uses `hotel_name + city + state` — *never* the SAP billing address (those are HQs).

---

## 🌎 Market focus

- **Geography:** USA + Caribbean only · International auto-skipped · No Canada
- **Timeline:** 2026+ openings · Past openings auto-expire on the nightly bucket refresh
- **Quality:** 4★ and above (tiers 1–4) · Budget brands (tier 5) and residences-only towers filtered before save
- **Project types:** New builds · Conversions · Rebrands · Renovations · Post-closure reopenings · Ownership changes

---

## 🧪 Troubleshooting

<details>
<summary><b>Vertex AI 429 quota exceeded</b></summary>

Upgrade to Tier 2 or add `MIN_DELAY_SECONDS` padding in `intelligence_config.py`. The classifier model has 4K RPM / unlimited RPD; the extractor has 1K RPM / 10K RPD.
</details>

<details>
<summary><b>Celery worker won't start on Windows</b></summary>

Use `--pool=solo`. Prefork doesn't work on Windows. This is non-negotiable — see `commands.txt`.
</details>

<details>
<summary><b>Playwright subprocess errors under Uvicorn</b></summary>

Uvicorn forces SelectorEventLoop, which breaks Playwright on Windows. Playwright works fine in Celery workers and CLI. Dashboard scraping uses httpx + gold URL fallback to avoid this.
</details>

<details>
<summary><b>SSE scrape stream hangs</b></summary>

Middleware is pure-ASGI on purpose — `BaseHTTPMiddleware` buffers responses and cancels long streams. Don't add any non-ASGI middlewares to `app/main.py`.
</details>

<details>
<summary><b>Timeline labels go stale</b></summary>

Celery beat must be running. The `recompute_timeline_labels` task runs daily at 09:30 ET and handles resurrection (expired → new when date moves forward). If beat is down, labels drift.
</details>

<details>
<summary><b>Vertex grounding returns phantom citations</b></summary>

You're using the `global` endpoint. Switch to `us-central1` regional. Global endpoint produces zero-source citations; regional returns real ones.
</details>

<details>
<summary><b>SAP geocoding accuracy is low</b></summary>

SAP billing addresses are corporate HQ, not property locations. Geocoding uses `hotel_name + city + state` instead. Hit rate improves when the SAP customer name contains the actual brand/location.
</details>

<details>
<summary><b>Insightly sync silently fails</b></summary>

Set `INSIGHTLY_API_KEY`. Without it, the client logs a warning at startup and disables CRM sync — no errors at approve time.
</details>

<details>
<summary><b>PATCH returns 401 on existing-hotels</b></summary>

Resolved 2026-05-06. See [CHANGELOG_AUDIT_2026_05_06.md](./CHANGELOG_AUDIT_2026_05_06.md) Bug #18a — was a NameError shadowed by `dataclasses.field` import causing silent no-op writes.
</details>

---

## 📜 License

Internal tool — JA Uniforms. Not for public distribution.

---

<div align="center">

### Built by JA Uniforms IT
**Hotel Intelligence Platform · 2026**

*Closing the 6-month uniform window, one pre-opening at a time.*

</div>
