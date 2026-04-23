# 🏨 Lead Generator

> **Automated hotel lead intelligence for J.A. Uniforms.**
> Discovers, classifies, enriches, scores, and routes pre‑opening 4★+ hotel prospects across the USA and Caribbean — then syncs them to Insightly CRM.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)
![Celery](https://img.shields.io/badge/Celery-beat-37814A?logo=celery&logoColor=white)
![Gemini](https://img.shields.io/badge/Gemini%202.5-Flash-4285F4?logo=google&logoColor=white)

---

## Why this exists

Hotel uniform procurement is won **6+ months before opening day**. Reach the property GM three weeks before the ribbon‑cutting and you've already lost — the vendor decisions were made by corporate procurement, the management company, or the pre‑opening GM long before the on‑property staff were hired.

That single fact drives everything in this system. Every pipeline, every score, every contact tier is structured around **where a property is in its lifecycle** and **who actually controls the uniform decision at that stage**.

---

## What it does

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                       DISCOVERY & INGEST                            │
 │   17 trade sources · 147 geo zones · Geoapify · Gemini classifier   │
 │   → potential_leads                                                 │
 ├─────────────────────────────────────────────────────────────────────┤
 │                       SMART FILL  (3 stages)                        │
 │   1. Classify project_type  (new / reno / rebrand / reopen / …)     │
 │   2. Branched query building + Serper search                        │
 │   3. Extract fields + entities + address  (Gemini 2.5 Flash)        │
 │   → opening_date, tier, rooms, brand, mgmt, owner, developer, addr  │
 ├─────────────────────────────────────────────────────────────────────┤
 │                  ITERATIVE CONTACT RESEARCHER  (v5)                 │
 │   Phase B: project-type gate (residences_only → auto-reject)        │
 │   Iter 1  · Discovery — owner / operator / stage                    │
 │   Iter 2  · GM hunt (with cascade when GM is missing)               │
 │   Iter 2.5· Property dept heads (HOT/URGENT only)                   │
 │   Iter 3  · Corporate / owner hunt                                  │
 │   Iter 4  · LinkedIn resolution                                     │
 │   Iter 5  · Current-role verification (kills stale press releases)  │
 │   Iter 5.5· Regional fit check for ambiguous-region titles          │
 │   Iter 6  · Strategist reasoning pass  → P1 / P2 / P3 / P4          │
 │   Iter 6.5· Employment verification on P1/P2  (Brian-Fry killer)    │
 │   → lead_contacts + evidence array per contact                      │
 ├─────────────────────────────────────────────────────────────────────┤
 │                          SCORING                                    │
 │   Lead: 100-pt rubric · Contact: tier × scope × strategist floor    │
 │   Revenue: 3 formulas validated 10/10 against SAP data              │
 ├─────────────────────────────────────────────────────────────────────┤
 │                        OUTPUT & SYNC                                │
 │   React dashboard · Map view · Client intelligence · Insightly CRM  │
 └─────────────────────────────────────────────────────────────────────┘
```

**Target market:** 4★+ (tiers 1–4), USA + Caribbean, 2026+ openings. Budget brands, international, and residences‑only towers are auto‑filtered.

---

## Feature highlights

### 🔍 Iterative contact researcher

Nine‑pass research loop that behaves like a human analyst digging into a lead. Each iteration's queries are built from facts learned in earlier passes, so the deeper it goes the smarter it gets.

* **Phase B classification gates the whole thing.** A branded‑residences tower (condos with no hotel rooms) is rejected before a single enrichment query runs — saves ~30 Serper calls + 10 Gemini calls per dud.
* **GM‑missing cascade.** When a pre‑opening lead has no GM yet, Iter 2 pivots to DOSM, Director of Revenue Management, Area/Regional GM, Task Force GM — and Iter 6 reweights corporate/regional VPs to P1 because they own the vendor decision until a GM is hired.
* **Strategist reasoning pass.** Final Gemini call reasons about who is *actually* running operations for *this* specific property, in *this* specific phase, *right now* — and assigns P1–P4 priorities with written reasoning that overrides the algorithmic priority.
* **Employment verification on P1/P2 only.** Stops stale press‑release contacts from surfacing as active targets. Noise tiers (P3/P4) skip this check to keep Gemini cost controlled.

### ⚡ Smart Fill / Full Refresh

One click on any lead runs a 3‑stage Gemini pipeline that pulls opening date, room count, brand tier, city/state/country, description, management company, owner, developer, street address, and zip — all with an `INVALID_TIER_SENTINELS` guard so manual edits never get trampled by an "unknown" Gemini response.

Handles the thorny cases the old pipeline couldn't:

| Scenario                               | Behaviour                                                             |
| -------------------------------------- | --------------------------------------------------------------------- |
| New ground‑up build                    | `opening_date` = first opening, `already_opened=false`                |
| Hotel closed for renovation → reopens  | Future reopen date wins, project_type=`renovation`, stays in pipeline |
| Existing hotel changes flag (rebrand)  | Uniform replacement = mandatory → Phase 3 starting point              |
| Post‑hurricane reopening               | Corporate‑led → Phase 1 (not a property GM hunt)                      |
| Residences‑only condo tower            | Auto‑rejected, zero enrichment cost                                   |

### 🎯 Phase-aware contact routing

Three contact phases keyed to timeline + operating model:

| Phase | When                | Target                                      | Urgency          |
| ----- | ------------------- | ------------------------------------------- | ---------------- |
| **1** | 12+ months out      | Management company corporate procurement    | WARM / COOL      |
| **2** | 6–11 months out     | Incoming pre‑opening GM                     | HOT (sweet spot) |
| **3** | 3–5 months out      | Department heads: HK / HR / F&B / Rooms     | URGENT           |

Operating model decides the target even more than the phase. A franchised Marriott property routes to the management company (Crescent, Aimbridge, Highgate). A managed Four Seasons routes to brand regional ops. An all‑inclusive Hyatt routes 100% to corporate.

### 🏅 Trust-tier evidence system

Every contact ships with an evidence array — quoted snippets + source URLs tagged by trust tier:

```
primary    → Company's own website (commonwealthhotels.com/team)
official   → Press wires (prnewswire, businesswire)
trade      → Industry pubs (hotelmanagement.net, hospitalitynet.org)
aggregator → Third-party (rocketreach, zoominfo, theorg) — often stale
indirect   → LinkedIn posts, secondary mentions
unknown    → Couldn't classify — treat as weak
```

Sales can see *why* a contact was surfaced at a glance. Multiple items = corroborated across sources.

### 💰 Revenue potential engine

Three formulas, all validated 10/10 against JA Uniforms' actual SAP sales history:

* **New Opening** — initial uniform provisioning (kit‑cost × staff × climate × tier multiplier)
* **Annual Recurring** — ongoing yearly spend with garment% purchase rate by tier
* **Rebrand** — flag change uniform replacement = 70% of new‑opening revenue

Tier parameters (staff/room, uniformed %, annual $/employee, turnover rate, garment %) are cross‑checked against CBRE, STR/CoStar, AHLA, USALI, and OMR Research benchmarks. Climate factors span 16 US markets + Caribbean.

### ⏱️ Timeline buckets (locked, enforced daily)

| Bucket  | Months to open | Meaning                                       |
| ------- | -------------- | --------------------------------------------- |
| EXPIRED | < 3 or past    | Too late for the sales cycle — don't pursue   |
| URGENT  | 3 – 5          | Tight but possible                            |
| HOT     | 6 – 11         | **Sweet spot** — active decision window       |
| WARM    | 12 – 17        | Planning phase                                |
| COOL    | 18 +           | Too early — watchlist                         |
| TBD     | Year only / ambiguous | Needs a source with a month                   |

Celery recomputes every label daily at 09:30 ET, including a **resurrection path**: a previously expired lead whose opening date gets pushed forward flips back to `new` automatically.

### 📊 Lead scoring (100 points)

| Category         | Max | Calibration                                                     |
| ---------------- | --- | --------------------------------------------------------------- |
| Brand Tier       | 25  | Ultra Luxury 25 · Luxury 20 · Upper Upscale 15 · Upscale 10     |
| Timing           | 25  | This year 25 · Next year 18 · +2yr 12 · +3yr+ 6                 |
| Location         | 20  | Florida 20 · Caribbean / strong US 15 · Other US 10             |
| Room Count       | 15  | 500+ 15 · 300+ 12 · 150+ 9 · 100+ 6                             |
| Contact Info     | 8   | Name 3 · Email 3 · Phone 2                                      |
| New Build        | 4   | New 4 · Conversion 3 · Renovation 2                             |
| Existing Client  | 3   | Known brand relationship bonus                                  |

Short brand names (≤4 chars) match with `\b` word‑boundary regex — "Trump International" no longer falsely matches the "tru" Tier 5 brand.

**Categories:** 🔥 Hot 70+ · ⚡ Warm 50–69 · ❄️ Cool 30–49 · 💧 Cold <30

### 🗺️ Three interconnected systems

| System              | Source                  | Purpose                             |
| ------------------- | ----------------------- | ----------------------------------- |
| `potential_leads`   | Scraped trade press     | Pre‑opening pipeline (Pipeline tab) |
| `existing_hotels`   | Geoapify discovery      | Prospecting map (Map tab)           |
| `sap_clients`       | SAP B1 CSV/XLSX imports | Client intelligence (Clients tab)   |

These are **fully independent** — conflating them has caused rework before. SAP billing addresses are never used for geocoding (they're corporate HQ, not property locations). Each system has its own rejection, approval, and workflow logic.

---

## Tech stack

### Backend

| Layer             | Technology                                                         |
| ----------------- | ------------------------------------------------------------------ |
| Web framework     | FastAPI (async) · Uvicorn · starlette pure‑ASGI middlewares        |
| ORM / Database    | SQLAlchemy (async) · asyncpg · PostgreSQL 16 · pgvector (optional) |
| Migrations        | Alembic (16 revisions)                                             |
| Background tasks  | Celery + Celery Beat · Redis broker                                |
| AI / ML           | Gemini 2.5 Flash Lite (classify) · Gemini 2.5 Flash (extract) via Vertex AI |
| Web search        | Google Serper API                                                  |
| Geocoding         | Geoapify (structured city/state/country params + bounding‑box validation) |
| Contact lookup    | RocketReach (manual) · Apollo (pipeline fallback) · Wiza · Hunter.io (configured) |
| Scraping          | httpx · Crawl4AI · Playwright · BeautifulSoup · lxml               |
| Data tooling      | openpyxl (XLSX) · rapidfuzz (dedup) · spaCy (NER fallback)         |
| CRM               | Insightly v3.1 API                                                 |

### Frontend

| Layer             | Technology                                              |
| ----------------- | ------------------------------------------------------- |
| Framework         | React 18 + TypeScript · Vite · React Router             |
| State             | TanStack Query (React Query)                            |
| Styling           | Tailwind CSS · custom design tokens                     |
| Map               | Leaflet · react‑leaflet · react‑leaflet‑cluster         |
| Icons             | lucide‑react                                            |
| Auth              | JWT (access + refresh) via custom `useAuth` hook        |

### AI cost profile

Gemini runs on **Vertex AI (GCP Tier 1 paid plan)** — not the free tier. Costs are controlled via the two‑stage classifier→extractor split, Phase B residences rejection, Iter 6.5 only running on P1/P2 contacts, and a 7‑day Redis extraction cache.

---

## Quick start

### Prerequisites

* Python 3.11 +
* Node 20 + (for frontend)
* PostgreSQL 16 (pgvector optional)
* Redis 7
* Google Cloud project with Vertex AI enabled + service account JSON
* API keys: Serper, Geoapify, Insightly, Apollo (optional), Wiza (optional)

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

| Variable                          | Purpose                                 |
| --------------------------------- | --------------------------------------- |
| `DATABASE_URL`                    | `postgresql+asyncpg://user:pw@host/db`  |
| `REDIS_URL`                       | `redis://localhost:6379/0`              |
| `GOOGLE_APPLICATION_CREDENTIALS`  | Path to Vertex AI service account JSON  |
| `GCP_PROJECT_ID`                  | Your GCP project ID                     |
| `SERPER_API_KEY`                  | Web search                              |
| `GEOAPIFY_API_KEY`                | Discovery + geocoding                   |
| `INSIGHTLY_API_KEY`               | CRM sync                                |
| `APOLLO_API_KEY`                  | Contact enrichment fallback             |
| `WIZA_API_KEY`                    | Email enrichment                        |
| `HUNTER_API_KEY`                  | Email enrichment (configured, unused)   |
| `JWT_SECRET_KEY`                  | Auth token signing                      |

### 3 · Database

```powershell
# Run migrations
alembic upgrade head

# Create first admin user
python scripts\create_admin.py
```

### 4 · Run the stack (3 terminals on Windows)

```powershell
# Terminal 1 — Web server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — Celery worker (--pool=solo required on Windows)
celery -A app.tasks.celery_app worker --loglevel=info --pool=solo

# Terminal 3 — Celery beat scheduler
celery -A app.tasks.celery_app beat --loglevel=info
```

Or use the included batch file:

```powershell
.\start_slh.bat
```

### 5 · Frontend (dev or prod build)

```powershell
cd frontend

# Dev (hot reload on :5173, proxies to :8000)
npm run dev

# Prod build (FastAPI serves dist/ automatically)
npm run build
```

**Dashboard:** <http://localhost:8000/new-hotels>
**API docs:** <http://localhost:8000/docs>

---

## Scheduled tasks

Business hours only (Mon–Fri 09:30–16:30 ET). App is shut down outside these windows.

| Task                        | Schedule                          | Queue       | Purpose                                      |
| --------------------------- | --------------------------------- | ----------- | -------------------------------------------- |
| `recompute_timeline_labels` | **Daily 09:30**                   | maintenance | Refresh bucket labels + resurrection logic   |
| `daily_health_check`        | Mon–Fri 09:35                     | maintenance | Gap cleanup, stale source deactivation       |
| `smart_scrape`              | Mon–Fri 10:00, 12:30, 15:30       | scraping    | Brain picks which sources are due            |
| `auto_enrich`               | Mon–Fri 11:00, 14:00              | scraping    | Enrich top 5 unenriched HOT/URGENT leads     |
| `weekly_discovery`          | Thu 10:30                         | scraping    | Find new sources + leads from the web        |

Timezone: `America/New_York`.

---

## Project structure

```
smart-lead-hunter/
├── app/
│   ├── main.py                         FastAPI bootstrap, ASGI middlewares
│   ├── database.py                     Async SQLAlchemy setup
│   ├── config_app.py                   Settings / env vars
│   ├── shared.py                       Cross-route state (scrape locks, pending stores)
│   │
│   ├── config/
│   │   ├── brand_registry.py           Canonical brand → tier / operator / procurement
│   │   ├── procurement_intelligence.py Operating-model → prospecting strategy
│   │   ├── project_type_intelligence.py Classifies new / reno / rebrand / reopen / …
│   │   ├── sap_title_classifier.py     7-tier buyer classification from 780 SAP titles
│   │   ├── enrichment_config.py        Title priority lists per timeline phase
│   │   ├── intelligence_config.py      Single source of truth for all thresholds
│   │   ├── locations.py                Florida / Caribbean / USA mapping
│   │   └── region_map.py               Country → region (for search terms)
│   │
│   ├── models/
│   │   ├── potential_lead.py           Main lead table (pre-opening pipeline)
│   │   ├── lead_contact.py             Per-lead contacts (with evidence + strategist)
│   │   ├── existing_hotel.py           Discovery map prospects
│   │   ├── sap_client.py               Imported SAP B1 clients
│   │   ├── source.py                   Scrape source config
│   │   ├── scrape_log.py               Per-run source metrics
│   │   ├── audit_log.py                User action audit
│   │   ├── user.py                     Auth users
│   │   └── failed_domain.py            Blocked domains
│   │
│   ├── routes/
│   │   ├── auth.py                     JWT login / register / refresh
│   │   ├── leads.py                    List / CRUD / export / bulk enrich-geo
│   │   ├── contacts.py                 Enrichment trigger + per-contact CRUD
│   │   ├── scraping.py                 SSE scrape / extract-url / discovery / SmartFill
│   │   ├── dashboard.py                Stats + filter facets
│   │   ├── existing_hotels.py          Map tab CRUD + zone discovery
│   │   ├── sap.py                      Client intelligence import + analytics
│   │   ├── revenue.py                  Revenue potential endpoints
│   │   ├── sources.py                  Source management
│   │   └── health.py                   Liveness / readiness
│   │
│   ├── services/
│   │   ├── iterative_researcher.py     9-iteration contact research loop (v5)
│   │   ├── contact_enrichment.py       Researcher adapter + v4 legacy fallback
│   │   ├── lead_data_enrichment.py     SmartFill 3-stage pipeline
│   │   ├── lead_geo_enrichment.py      Website + geocoding (Geoapify)
│   │   ├── contact_scoring.py          Unified per-contact score formula
│   │   ├── contact_validator.py        Email / phone / title validation
│   │   ├── scorer.py                   100-point lead score
│   │   ├── rescore.py                  Batch rescore after field changes
│   │   ├── revenue_calculator.py       3 validated revenue formulas
│   │   ├── revenue_updater.py          Auto-update revenue on field change
│   │   ├── source_tier.py              Trust-tier classifier for evidence
│   │   ├── source_intelligence.py      Adaptive source scheduling + learning
│   │   ├── source_seed.py              Initial source seeding
│   │   ├── source_config.py            Per-source pattern configuration
│   │   ├── scraping_engine.py          HTTPX / Crawl4AI / Playwright 3-engine scraper
│   │   ├── smart_scraper.py            Unified scraping entrypoint
│   │   ├── intelligent_pipeline.py     Classify → extract two-stage AI pipeline
│   │   ├── smart_deduplicator.py       RapidFuzz fuzzy-matching dedup
│   │   ├── orchestrator.py             Pipeline coordinator
│   │   ├── pipeline.py                 Geoapify discovery pipeline
│   │   ├── sap_import.py               CSV/XLSX → sap_clients upsert
│   │   ├── insightly.py                CRM sync client
│   │   ├── wiza_enrichment.py          Email enrichment
│   │   ├── ai_client.py                Vertex AI Gemini wrapper
│   │   ├── gemini_classifier.py        Quick-reject classifier
│   │   ├── zones_registry.py           147 geo zones (USA + Caribbean)
│   │   ├── url_filter.py               URL relevance filtering
│   │   ├── lead_factory.py             Lead creation + dedup on save
│   │   └── utils.py                    Timeline labels, date parsing, normalization
│   │
│   ├── tasks/
│   │   ├── celery_app.py               Celery config + beat schedule
│   │   ├── autonomous_tasks.py         Smart scrape, auto enrich, timeline refresh
│   │   └── scraping_tasks.py           Legacy per-source scrape tasks
│   │
│   └── middleware/auth.py              API key middleware
│
├── frontend/
│   ├── src/
│   │   ├── pages/                      Dashboard · ExistingHotels · MapPage · ClientIntelligence · SourcesPage · Users · Login · Register
│   │   ├── components/
│   │   │   ├── leads/                  LeadTable · LeadDetail · FilterBar · RevenuePotential
│   │   │   ├── layout/                 AppLayout
│   │   │   ├── modals/                 Confirm / Import / Approval flows
│   │   │   ├── stats/                  StatsCards
│   │   │   └── ui/                     ConfirmDialog
│   │   ├── hooks/                      useLeads · useAuth · useSAP · useRevenue · useBackgroundTask
│   │   ├── api/                        client.ts · leads.ts · sap.ts · revenue.ts · types.ts
│   │   └── lib/utils.ts                Formatters, colour helpers
│   └── vite.config.ts
│
├── alembic/
│   └── versions/                       16 migrations (001 → 016)
│
├── scripts/
│   ├── run_pipeline.py                 Run full scrape + enrich from CLI
│   ├── test_full_refresh.py            Full-Refresh CLI with verbose stage logs
│   ├── test_d1_cascade.py              Existing-hotels cascade test
│   ├── audit_geocodes.py               Detect bad coordinates
│   ├── verify_geocodes.py              Audit coords against zone bboxes
│   ├── backfill_geocodes_leads.py      Rerun geocoding for null-coord leads
│   ├── backup_db.py                    pg_dump wrapper
│   ├── create_admin.py                 Seed first admin user
│   ├── grade_sources.py                Rate all sources A–F
│   ├── discover_sources.py             Find new trade press sources
│   └── tune_source_frequencies.py      Rebalance scrape intervals
│
├── tests/                              pytest + conftest.py
├── cache/geoapify/                     Geoapify response cache (committed for reuse)
├── docker-compose.yml                  Postgres + Redis for local dev
├── Dockerfile                          App container
├── start_slh.bat                       Windows one-click launcher
├── alembic.ini
├── requirements.txt
└── revenue_formula_spec.md             Revenue formula specification + validation log
```

---

## Useful scripts

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

## Schema migrations

| Rev | Summary                                                              |
| --- | -------------------------------------------------------------------- |
| 001 | Initial indexes                                                      |
| 002 | Add `timeline_label` column                                          |
| 003 | City index                                                           |
| 004 | User tables                                                          |
| 005 | Unique normalized name + perf indexes                                |
| 006 | Audit logs                                                           |
| 007 | Status index                                                         |
| 008 | Geocoords + website                                                  |
| 009 | Merge heads                                                          |
| 010 | Fix `website_verified`                                               |
| 011 | Add `strategist_priority` to lead_contacts                           |
| 012 | Add `search_name` + `former_names` to potential_leads                |
| 013 | Add per-contact `score_breakdown`                                    |
| 014 | Add per-contact `evidence` array                                     |
| 015 | Add `project_type` to potential_leads                                |
| 016 | Add `address` + `zip_code` to potential_leads                        |
| —   | `add_sap_clients_table` (side branch) — SAP import table             |

---

## Troubleshooting

**`GEOAPIFY_API_KEY not set — skipping geocoding`**
Set the env var and restart. Without it, new leads save with `latitude=null, longitude=null`.

**`Vertex AI 429 — quota exceeded`**
Upgrade to Tier 2 or add `MIN_DELAY_SECONDS` padding in `intelligence_config.py`. The classifier model has 4K RPM / unlimited RPD; the extractor has 1K RPM / 10K RPD.

**Celery worker won't start on Windows**
Use `--pool=solo`. Prefork doesn't work on Windows. This is non-negotiable — see `commands.txt`.

**Playwright subprocess errors under Uvicorn**
Uvicorn forces a SelectorEventLoop, which breaks Playwright on Windows. Playwright works fine in Celery workers and CLI (`scripts/run_pipeline.py`). Dashboard scraping uses httpx + gold URL fallback to avoid this.

**SSE scrape stream hangs**
Middleware is pure‑ASGI on purpose — `BaseHTTPMiddleware` buffers responses and cancels long streams. Don't add any non‑ASGI middlewares to `app/main.py`.

**Timeline labels go stale**
Celery beat must be running. The `recompute_timeline_labels` task runs daily at 09:30 ET and handles resurrection (expired → new when date moves forward). If beat is down, labels drift.

**SAP geocoding accuracy is low**
SAP billing addresses are corporate HQ, not property locations. Geocoding uses `hotel_name + city + state` instead. Hit rate improves when the SAP customer name contains the hotel's actual brand/location.

**Insightly sync silently fails**
Set `INSIGHTLY_API_KEY`. Without it, the client logs a warning at startup and disables CRM sync — no errors at approve time.

---

## Market focus

* **Geography:** USA + Caribbean only. International leads auto-skipped.
* **Timeline:** 2026+ openings. Past openings auto-expire on the nightly bucket refresh.
* **Quality:** 4★ and above (tiers 1–4). Budget brands (tier 5) and residences‑only towers are filtered before save.
* **Project types:** New builds · conversions · rebrands · renovations · post‑closure reopenings · ownership changes.

---

## License

Internal tool — J.A. Uniforms. Not for public distribution.

---

<div align="center">

**Built by J.A. Uniforms IT — Hotel Intelligence Platform**

</div>
