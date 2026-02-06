# 🏨 Smart Lead Hunter

Automated hotel lead generation system for **J.A. Uniforms** — finds new hotel openings, renovations, and conversions across 79 sources, scores them, and pushes qualified leads to Insightly CRM.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    SCRAPING ENGINE                        │
│  79 Sources → HTTPX → Crawl4AI → Playwright (fallback)   │
│  Rate limiting · Content caching · Domain learning        │
├──────────────────────────────────────────────────────────┤
│                    AI PIPELINE                            │
│  1. QuickReject  → Skip irrelevant pages                 │
│  2. Extract      → Gemini 2.0 Flash (primary)            │
│                    Ollama llama3.2 (backup, local)        │
│  3. Validate     → Email/phone/date parsing              │
│  4. Enrich       → Contact lookup (Hunter/Apollo)        │
│  5. Score        → 100-point system                      │
├──────────────────────────────────────────────────────────┤
│                    DEDUPLICATION                          │
│  RapidFuzz fuzzy matching + merge across sources          │
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
| **Primary AI** | Google Gemini 2.0 Flash | Lead extraction from scraped pages | $0 (free tier) |
| **Backup AI** | Ollama llama3.2 (local) | Fallback when Gemini unavailable | $0 (runs locally) |
| **Email validation** | email-validator | Verify extracted emails | $0 |
| **Phone parsing** | phonenumbers | Format/validate phone numbers | $0 |
| **Fuzzy matching** | RapidFuzz | Deduplication across sources | $0 |

The pipeline tries Gemini first. If Gemini fails (rate limit, network), it falls back to Ollama automatically. If neither is available, regex-only extraction still captures emails, phones, and room counts.

---

## Scoring System (100 points)

| Category | Max Points | Details |
|----------|-----------|---------|
| Brand Tier | 25 | Ultra Luxury (25) → Upscale (10) → Budget (skip) |
| Timing | 25 | This year (25) → Next year (18) → +2yr (12) → +3yr+ (6) |
| Location | 20 | Florida (20) → Caribbean/Strong US (15) → Other US (10) → International (skip) |
| Room Count | 15 | 500+ (15) → 300+ (12) → 150+ (9) → 100+ (6) |
| Contact Info | 8 | Name (3) + Email (3) + Phone (2) |
| New Build | 4 | New (4) → Conversion (3) → Renovation (2) |
| Existing Client | 3 | Known brand relationship bonus |

**Auto-skip:** Budget brands (Tier 5) and international locations are filtered out before saving.

---

## Project Structure

```
smart-lead-hunter/
├── app/
│   ├── main.py                 # FastAPI app + API endpoints
│   ├── config.py               # Settings (env vars)
│   ├── database.py             # SQLAlchemy async setup
│   ├── models.py               # DB models (Lead, Source, ScrapeLog)
│   ├── services/
│   │   ├── orchestrator.py     # Pipeline coordinator
│   │   ├── scraping_engine.py  # 3-engine scraper (HTTPX/Crawl4AI/Playwright)
│   │   ├── extractor.py        # AI extraction (Gemini + Ollama)
│   │   ├── scorer.py           # 100-point scoring system
│   │   ├── deduplicator.py     # RapidFuzz dedup + merge
│   │   └── insightly_crm.py   # CRM integration
│   ├── tasks/
│   │   ├── celery_app.py       # Celery config + schedules
│   │   └── scraping_tasks.py   # Async task definitions
│   └── templates/
│       ├── base.html           # Layout (Tailwind + HTMX + Alpine)
│       ├── dashboard.html      # Main dashboard
│       └── partials/           # HTMX fragments
├── docker-compose.yml          # Postgres + Redis
├── requirements.txt            # Python dependencies
├── .env.example                # Environment template
└── README.md                   # This file
```

---

## Scheduled Tasks

| Task | Schedule | Queue |
|------|----------|-------|
| Full scrape (all 79 sources) | Daily at 6:00 AM ET | scraping |
| High-priority sources | Every 6 hours | scraping |
| Duplicate cleanup | Daily at 3:00 AM ET | maintenance |
| Embedding update | Sundays at 2:00 AM ET | maintenance |
| Insightly CRM sync | Hourly at :15 | crm |

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