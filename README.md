# Smart Lead Hunter

Automated hotel lead generation system for J.A.Uniforms.

## What It Does

- Scrapes 150+ hotel news websites for new openings
- Extracts contact information using AI (Ollama)
- Scores leads based on luxury signals and FL/Caribbean location
- Detects duplicates using vector similarity
- Pushes qualified leads to Insightly CRM

## Tech Stack

| Layer | Technology |
|-------|------------|
| Scraping | Playwright + BeautifulSoup |
| AI Extraction | Ollama (Llama 3.2) |
| NLP | spaCy |
| Scoring | Rule-based Python |
| Database | PostgreSQL + pgvector |
| Backend | FastAPI |
| Task Queue | Celery + Redis |
| CRM | Insightly API |

## Setup

Instructions coming soon.

## Status

🚧 Under Development
```

---

## File 3: requirements.txt

**Create new file → Save as `requirements.txt`:**
```
# Core Framework
fastapi==0.109.0
uvicorn[standard]==0.27.0
pydantic==2.5.0
pydantic-settings==2.1.0
python-dotenv==1.0.0

# Database
sqlalchemy==2.0.25
asyncpg==0.29.0
psycopg2-binary==2.9.9
pgvector==0.2.4

# Redis & Task Queue
redis==5.0.1
celery==5.3.6

# Web Scraping
playwright==1.41.0
beautifulsoup4==4.12.3
lxml==5.1.0
httpx==0.26.0

# AI/ML
ollama==0.1.6
spacy==3.7.2
sentence-transformers==2.3.1

# Utilities
tenacity==8.2.3
python-dateutil==2.8.2

# Testing
pytest==7.4.4
pytest-asyncio==0.23.3
```

---

## File 4: .env.example

**Create new file → Save as `.env.example`:**
```
# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/smart_lead_hunter

# Redis
REDIS_URL=redis://localhost:6379/0

# Ollama
OLLAMA_URL=http://localhost:11434

# Insightly
INSIGHTLY_API_KEY=your-api-key-here
INSIGHTLY_POD=na1

# Environment
ENVIRONMENT=development
```

---

## After Creating All 4 Files

Your VS Code explorer should show:
```
SMART-LEAD-HUNTER
├── .gitignore
├── .env.example
├── README.md
└── requirements.txt