# ============================================================
# SMART LEAD HUNTER — Production Dockerfile
# Fixes: AR5 (no Dockerfile) + SH1 (runs as non-root)
# ============================================================
# Multi-stage build: 
#   Stage 1 — Install Python deps + Playwright
#   Stage 2 — Slim runtime image
# ============================================================

# --------------- Stage 1: Builder ---------------
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps for compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
# Install CPU-only PyTorch first (saves ~4GB vs full CUDA version)
RUN pip install --no-cache-dir --prefix=/install \
    torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --------------- Stage 2: Runtime ---------------
FROM python:3.11-slim

# System runtime deps (Playwright chromium needs these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libxshmfence1 \
    fonts-liberation \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# SH1: Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

# Install Playwright browsers as root (needs write access to /usr/local)
RUN playwright install chromium && playwright install-deps chromium

# Install spacy model
RUN python -m spacy download en_core_web_sm

# Copy application code
COPY . .

# Create required directories
RUN mkdir -p output data/learnings logs \
    && chown -R appuser:appuser /app

# SH1: Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default: run the web app (override in docker-compose for worker/beat)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
