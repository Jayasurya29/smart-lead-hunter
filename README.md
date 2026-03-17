# 🏨 Smart Lead Hunter

Automated hotel lead generation system for **J.A. Uniforms** — finds new hotel openings across the **USA & Caribbean** (2026+ openings only), scores them, and pushes qualified leads to Insightly CRM.

Targets **4-star+ properties only** (Tiers 1–4). Budget brands (Tier 5) are automatically filtered out.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    SCRAPING ENGINE                        │
│  17 Active Sources → HTTPX → Crawl4AI → Playwright       │
│  Rate limiting · Content caching · Domain learning        │
├──────────────────────────────────────────────────────────┤
│                    AI PIPELINE                            │
│  1. Classify     → Gemini 2.5 Flash Lite (quick reject)  │
│  2. Extract      → Gemini 2.5 Flash (primary)            │
│                    Ollama llama3.2 (backup, local)        │
│  3. Validate     → Email/phone/date parsing              │
│  4. Enrich       → Contact lookup (Hunter/Apollo)        │
│  5. Score        → 100-point system                      │
├──────────────────────────────────────────────────────────┤
│                    DEDUPLICATION                          │
│  RapidFuzz fuzzy matching + smart merge across sources    │
├──────────────────────────────────────────────────────────┤
│                    SCORING (100 pts)                      │
│  Brand Tier: 25 · Location: 20 · Timing: 25              │
│  Room Count: 15 · Contact: 8 · Build: 4 · Client: 3     │
├──────────────────────────────────────────────────────────┤
│                    OUTPUT                                 │
│  Dashboard (FastAPI + HTMX) · Insightly CRM sync         │
│  Celery workers · Scheduled scrapes (6 AM daily)          │
└──────────────────────────────────────────────────────────┘
```

**Total Monthly Cost: $0** (Gemini free tier + Ollama local)

---

## Dashboard Features

| Feature | Description |
|---------|-------------|
| **Live Search** | Instant client-side filtering as you type — no page reload |
| **Smart Filters** | Score, location, tier, opening year, and sort options |
| **Sort Options** | Score ↓/↑, Recently Added, Oldest First, Name A-Z, Opening Date |
| **Lead Detail Panel** | Click any row to view/edit full lead details in side panel |
| **Key Insights** | AI-generated bullet points per lead with expandable view |
| **Approve / Reject / Delete** | One-click lead management with confirmation dialogs |
| **Deleted Tab** | Soft-delete with restore capability — nothing is permanently lost |
| **Timestamps** | "Added" column shows when each lead was discovered |
| **Source Provenance** | Track which sources contributed to each lead, with per-source extraction details |
| **Run Scrape Now** | On-demand scraping with real-time SSE progress streaming |
| **Stat Cards** | Clickable overview — Total, Hot, Warm, New, Approved, This Week, Deleted |
| **URL Extractor** | Paste any article URL for direct one-off lead extraction |

**Tech Stack:** FastAPI + Jinja2 · HTMX (partial updates) · Alpine.js (interactivity) · Tailwind CSS

---

## Quick Start

### 1. Clone & Install

```bash
git clone <repo-url>
cd smart-lead-hunter
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Post-Install Setup

```bash
playwright install chromium
crawl4ai-setup
python -m spacy download en_core_web_sm
```

### 3. Environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required keys:

| Key | Source | Cost |
|-----|--------|------|
| `GEMINI_API_KEY` | [aistudio.google.com/apikey](https://aistudio.google.com/apikey) | Free |
| `HUNTER_API_KEY` | [hunter.io](https://hunter.io) | Free tier |
| `APOLLO_API_KEY` | [apollo.io](https://www.apollo.io) | Free tier |
| `INSIGHTLY_API_KEY` | Your Insightly account | Included |

### 4. Start Services

```bash
# Database + Redis
docker compose up -d

# Web app
uvicorn app.main:app --reload --port 8000

# Celery worker (separate terminal)
celery -A app.tasks.celery_app worker --loglevel=info

# Celery beat scheduler (separate terminal)
celery -A app.tasks.celery_app beat --loglevel=info
```

Dashboard: **http://localhost:8000/dashboard**
API Docs: **http://localhost:8000/docs**

---

## AI Stack

| Component | Model | Purpose | Cost |
|-----------|-------|---------|------|
| **Classification** | Google Gemini 2.5 Flash Lite | Quick reject — skip irrelevant pages before full extraction | $0 (free tier) |
| **Primary Extraction** | Google Gemini 2.5 Flash | Full lead extraction from scraped pages | $0 (free tier) |
| **Backup Extraction** | Ollama llama3.2 (local) | Fallback when Gemini unavailable | $0 (runs locally) |
| **Email validation** | email-validator | Verify extracted emails | $0 |
| **Phone parsing** | phonenumbers | Format/validate phone numbers | $0 |
| **Fuzzy matching** | RapidFuzz | Deduplication across sources | $0 |

The pipeline uses a two-stage AI approach: Gemini 2.5 Flash Lite first classifies whether a page contains hotel lead information (quick reject). Only pages that pass classification are sent to Gemini 2.5 Flash for full extraction. If Gemini fails (rate limit, network), it falls back to Ollama automatically. If neither is available, regex-only extraction still captures emails, phones, and room counts.

---

## Sources (17 Active)

Sources are organized by category and validated through systematic batch testing. Only high-quality, consistently producing sources remain active.

| Category | Description |
|----------|-------------|
| 🏨 Chain Newsrooms | Official hotel brand press releases (Hilton, Hyatt, Marriott, etc.) |
| 💎 Luxury & Independent | Ultra-luxury and boutique hotel announcements |
| 📰 Aggregators | Hotel Dive, CoStar, and industry news aggregators |
| 🏗️ Industry | Construction and development pipeline trackers |
| 🌴 Florida | Florida-specific hospitality and business news |
| 🏝️ Caribbean | Caribbean hotel and tourism development news |
| ✈️ Travel Pubs | Travel industry publications |
| 📡 PR Wire | Press release distribution services |

Sources are evaluated on a grading system (A–F) based on lead yield, content quality, and reliability. Underperforming sources are deactivated.

---

## Scoring System (100 points)

| Category | Max Points | Details |
|----------|-----------|---------|
| Brand Tier | 25 | Ultra Luxury (25) → Luxury (20) → Upper Upscale (15) → Upscale (10) → Budget (skip) |
| Timing | 25 | This year (25) → Next year (18) → +2yr (12) → +3yr+ (6) |
| Location | 20 | Florida (20) → Caribbean/Strong US (15) → Other US (10) → International (skip) |
| Room Count | 15 | 500+ (15) → 300+ (12) → 150+ (9) → 100+ (6) |
| Contact Info | 8 | Name (3) + Email (3) + Phone (2) |
| New Build | 4 | New (4) → Conversion (3) → Renovation (2) |
| Existing Client | 3 | Known brand relationship bonus |

**Auto-skip:** Budget brands (Tier 5) and international locations are filtered out before saving.

**Lead Categories:**
- 🔥 **Hot** (70+) — High-priority, immediate outreach
- ⚡ **Warm** (50–69) — Good potential, schedule follow-up
- ❄️ **Cold** (<50) — Lower priority, monitor

---

## Lead Management Workflow

```
Scraped → Classification (2.5 Flash Lite) → Extraction (2.5 Flash) → Dedup → Scoring → Pipeline
                                                                                          │
                                                                             ┌────────────┼────────────┐
                                                                             ▼            ▼            ▼
                                                                         ✅ Approve   ❌ Reject    🗑️ Delete
                                                                             │            │            │
                                                                             ▼            │            │
                                                                       Insightly CRM      └──► Restore ◄┘
```

- **Pipeline** — New leads awaiting review
- **Approved** — Qualified leads synced to Insightly CRM
- **Rejected** — Not a fit (can be restored)
- **Deleted** — Soft-deleted (can be restored from Deleted tab)

---

## Project Structure

```
smart-lead-hunter/
├── app/
│   ├── main.py                      # FastAPI app + all API endpoints
│   ├── config.py                    # Settings (env vars, thresholds)
│   ├── database.py                  # SQLAlchemy async setup
│   ├── logging_config.py            # Logging configuration
│   ├── __init__.py
│   ├── middleware/
│   │   ├── __init__.py
│   │   └── auth.py                  # Authentication middleware
│   ├── models/
│   │   ├── __init__.py
│   │   ├── potential_lead.py        # PotentialLead model
│   │   ├── scrape_log.py            # ScrapeLog model
│   │   └── source.py                # Source model
│   ├── services/
│   │   ├── __init__.py
│   │   ├── ai_rate_limiter.py       # Gemini API rate limit management
│   │   ├── gold_url_tracker.py      # Track high-value URLs per source
│   │   ├── insightly.py             # Insightly CRM integration
│   │   ├── intelligent_pipeline.py  # Two-stage AI pipeline (classify → extract)
│   │   ├── orchestrator.py          # Pipeline coordinator (LeadHunterOrchestrator)
│   │   ├── scorer.py                # 100-point scoring system
│   │   ├── scraping_engine.py       # 3-engine scraper (HTTPX/Crawl4AI/Playwright)
│   │   ├── smart_deduplicator.py    # RapidFuzz dedup + intelligent merge
│   │   ├── source_config.py         # Source configuration management
│   │   ├── source_learning.py       # Adaptive source behavior learning
│   │   ├── source_seed.py           # Initial source database seeding
│   │   ├── targeted_contact_finder.py # Contact enrichment (Hunter/Apollo)
│   │   ├── url_filter.py            # URL relevance filtering
│   │   └── utils.py                 # Shared utility functions
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── celery_app.py            # Celery config + schedules
│   │   └── scraping_tasks.py        # Async task definitions
│   ├── templates/
│   │   ├── base.html                # Layout (Tailwind + HTMX + Alpine)
│   │   ├── dashboard.html           # Main dashboard with live search
│   │   └── partials/
│   │       ├── lead_detail.html     # Lead detail side panel
│   │       ├── lead_list.html       # Lead list fragment
│   │       ├── lead_row.html        # Individual lead table row
│   │       ├── scrape_modal.html    # On-demand scrape modal
│   │       └── stats.html           # Dashboard stat cards
│   └── static/
│       └── img/                     # Logo and brand assets
├── data/
│   └── learnings/
│       ├── pipeline_learnings.json  # AI pipeline performance data
│       ├── source_learnings.json    # Source behavior patterns
│       └── url_history.json         # Processed URL tracking
├── database/
│   ├── migrations/                  # Alembic migration files
│   └── schema.sql                   # Database schema reference
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Pytest fixtures
│   └── test_core.py                # Core functionality tests
├── logs/                            # Application log files
├── output/                          # Exported lead data (JSON)
├── blobs/                           # Binary data storage
├── manifests/                       # Deployment manifests
├── .env.example                     # Environment template
├── .gitignore
├── .pre-commit-config.yaml          # Pre-commit hooks
├── alembic.ini                      # Alembic migration config
├── commands.txt                     # Useful command reference
├── deploy_dashboard.py              # Dashboard deployment script
├── docker-compose.yml               # Postgres + Redis
├── Dockerfile                       # Container build config
├── extract_urls.py                  # One-off URL extraction tool
├── grade_sources.py                 # Source grading & evaluation
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

---

## Scheduled Tasks

| Task | Schedule | Queue |
|------|----------|-------|
| Full scrape (all active sources) | Daily at 6:00 AM ET | scraping |
| High-priority sources | Every 6 hours | scraping |
| Duplicate cleanup | Daily at 3:00 AM ET | maintenance |
| Embedding update | Sundays at 2:00 AM ET | maintenance |
| Insightly CRM sync | Hourly at :15 | crm |

---

## Key Scripts

| Script | Purpose |
|--------|---------|
| `python grade_sources.py` | Evaluate and grade all sources (A–F) |
| `python extract_urls.py <url>` | Extract leads from a specific article URL |
| `python deploy_dashboard.py` | Deploy dashboard updates |
| `python -m app.services.orchestrator --test` | Test pipeline with a single source |

---

## Troubleshooting

**"Gemini API key not set"** → Get a free key at [aistudio.google.com/apikey](https://aistudio.google.com/apikey), add to `.env`

**"Ollama not running"** → Install from [ollama.ai](https://ollama.ai/), then:
```bash
ollama serve
ollama pull llama3.2
```

**"Redis connection refused"** → Check Docker is running: `docker compose ps`

**"Port 6379 in use"** → Another Redis is running. The docker-compose uses port 6380 to avoid conflicts.

**Dashboard blank after CDN update** → Verify SRI hashes match. See comment in `base.html`.

**Search not filtering** → Ensure JavaScript is enabled. Search uses client-side row filtering (no server round-trip).

---

## Market Focus

- **Geography:** USA + Caribbean only (international leads auto-skipped)
- **Timeline:** 2026 and future openings only
- **Quality:** 4-star and above (Tiers 1–4 only, budget brands auto-rejected)
- **Target Properties:** New builds, conversions, and major renovations

---

*Built by J.A. Uniforms IT — Hotel Intelligence Platform*
