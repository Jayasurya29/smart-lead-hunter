-- ============================================================
-- SMART LEAD HUNTER - COMPLETE DATABASE SCHEMA
-- ============================================================
-- Run this ONCE to create all tables
-- Then run seed_sources.sql to populate sources
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- TABLE: sources
-- ============================================================
-- Stores all hotel news websites we scrape

CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    base_url VARCHAR(500) NOT NULL,
    source_type VARCHAR(50) DEFAULT 'aggregator',  -- chain_newsroom, luxury_independent, aggregator, caribbean, florida, industry, travel_pub, pr_wire
    priority INTEGER DEFAULT 5,                     -- 1-10 (10 = highest priority, scrape first)
    entry_urls TEXT[],                              -- Multiple URLs for fallback/self-healing
    scrape_frequency VARCHAR(20) DEFAULT 'daily',   -- daily, weekly, monthly
    max_depth INTEGER DEFAULT 2,                    -- How deep to crawl links
    use_playwright BOOLEAN DEFAULT false,           -- true = JS-heavy site, false = simple HTML
    is_active BOOLEAN DEFAULT true,
    
    -- Tracking
    last_scraped_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    leads_found INTEGER DEFAULT 0,
    success_rate DECIMAL(5,2) DEFAULT 0.00,
    consecutive_failures INTEGER DEFAULT 0,
    
    -- Health monitoring
    health_status VARCHAR(20) DEFAULT 'new',        -- healthy, degraded, failing, dead, new
    
    -- Notes
    notes TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(base_url)
);

-- ============================================================
-- TABLE: potential_leads
-- ============================================================
-- Stores scraped hotel leads before pushing to Insightly

CREATE TABLE IF NOT EXISTS potential_leads (
    id SERIAL PRIMARY KEY,
    
    -- Hotel Information
    hotel_name VARCHAR(255) NOT NULL,
    hotel_name_normalized VARCHAR(255),              -- Lowercase, no special chars (for dedup)
    brand VARCHAR(100),
    brand_tier VARCHAR(20),                          -- tier1_ultra_luxury, tier2_luxury, tier3_upper_upscale, tier4_upscale, tier5_skip
    hotel_type VARCHAR(50),                          -- resort, hotel, boutique, all-inclusive
    hotel_website VARCHAR(500),
    
    -- Location
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',
    location_type VARCHAR(20),                       -- florida, caribbean, usa, international
    
    -- Contact Information
    contact_name VARCHAR(200),
    contact_title VARCHAR(100),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    
    -- Hotel Details
    opening_date VARCHAR(50),                        -- Flexible: "Q2 2026", "June 2026", "2026"
    opening_year INTEGER,                            -- Extracted year for filtering
    room_count INTEGER,
    description TEXT,
    
    -- Scoring (0-100)
    lead_score INTEGER CHECK (lead_score >= 0 AND lead_score <= 100),
    score_breakdown JSONB,                           -- {"location": 30, "brand": 25, "timing": 20, ...}
    estimated_revenue INTEGER,                       -- Estimated uniform revenue in dollars
    
    -- Source Tracking
    source_id INTEGER REFERENCES sources(id),
    source_url TEXT,
    source_site VARCHAR(100),
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Workflow Status
    status VARCHAR(20) DEFAULT 'new',                -- new, claimed, approved, rejected, pushed
    claimed_by VARCHAR(100),
    claimed_at TIMESTAMPTZ,
    rejection_reason VARCHAR(100),                   -- duplicate, budget_brand, international, old_opening, bad_data
    notes TEXT,
    
    -- Insightly CRM Sync
    insightly_id INTEGER,
    synced_at TIMESTAMPTZ,
    sync_error TEXT,
    
    -- Deduplication
    embedding VECTOR(384),                           -- For semantic similarity
    duplicate_of_id INTEGER REFERENCES potential_leads(id),
    similarity_score DECIMAL(5,4),                   -- How similar to duplicate (0.0-1.0)
    
    -- Raw data
    raw_data JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TABLE: scrape_logs
-- ============================================================
-- Tracks each scrape run for monitoring and debugging

CREATE TABLE IF NOT EXISTS scrape_logs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    
    -- Stats
    urls_scraped INTEGER DEFAULT 0,
    pages_crawled INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    leads_new INTEGER DEFAULT 0,
    leads_duplicate INTEGER DEFAULT 0,
    leads_skipped INTEGER DEFAULT 0,                 -- Skipped due to filters (budget, international)
    
    -- Status
    status VARCHAR(20) DEFAULT 'running',            -- running, success, failed, partial
    error_message TEXT,
    errors JSONB,                                    -- Array of error details
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TABLE: source_health_log
-- ============================================================
-- Tracks health checks for self-healing system

CREATE TABLE IF NOT EXISTS source_health_log (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
    checked_at TIMESTAMPTZ DEFAULT NOW(),
    health_status VARCHAR(20),                       -- healthy, degraded, failing, dead
    urls_tried INTEGER DEFAULT 0,
    urls_succeeded INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    response_time_ms INTEGER,
    error_message TEXT,
    action_taken VARCHAR(50),                        -- none, rotated_url, triggered_healing, flagged_human
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Sources indexes
CREATE INDEX IF NOT EXISTS idx_sources_active ON sources(is_active);
CREATE INDEX IF NOT EXISTS idx_sources_priority ON sources(priority DESC);
CREATE INDEX IF NOT EXISTS idx_sources_type ON sources(source_type);
CREATE INDEX IF NOT EXISTS idx_sources_health ON sources(health_status);
CREATE INDEX IF NOT EXISTS idx_sources_frequency ON sources(scrape_frequency);

-- Potential Leads indexes
CREATE INDEX IF NOT EXISTS idx_leads_status ON potential_leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_score ON potential_leads(lead_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_leads_state ON potential_leads(state);
CREATE INDEX IF NOT EXISTS idx_leads_country ON potential_leads(country);
CREATE INDEX IF NOT EXISTS idx_leads_location_type ON potential_leads(location_type);
CREATE INDEX IF NOT EXISTS idx_leads_brand_tier ON potential_leads(brand_tier);
CREATE INDEX IF NOT EXISTS idx_leads_opening_year ON potential_leads(opening_year);
CREATE INDEX IF NOT EXISTS idx_leads_created ON potential_leads(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_scraped ON potential_leads(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_source ON potential_leads(source_id);
CREATE INDEX IF NOT EXISTS idx_leads_normalized_name ON potential_leads(hotel_name_normalized);
CREATE INDEX IF NOT EXISTS idx_leads_insightly ON potential_leads(insightly_id);

-- Vector similarity index for deduplication (requires ~100 rows first)
-- CREATE INDEX IF NOT EXISTS idx_leads_embedding ON potential_leads 
-- USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Fuzzy matching index for hotel name
CREATE INDEX IF NOT EXISTS idx_leads_name_trgm ON potential_leads 
USING gin (hotel_name gin_trgm_ops);

-- Scrape Logs indexes
CREATE INDEX IF NOT EXISTS idx_scrape_logs_source ON scrape_logs(source_id);
CREATE INDEX IF NOT EXISTS idx_scrape_logs_started ON scrape_logs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_scrape_logs_status ON scrape_logs(status);

-- Health Log indexes
CREATE INDEX IF NOT EXISTS idx_health_log_source ON source_health_log(source_id);
CREATE INDEX IF NOT EXISTS idx_health_log_checked ON source_health_log(checked_at DESC);

-- ============================================================
-- AUTO-UPDATE TRIGGER
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_leads_updated_at ON potential_leads;
CREATE TRIGGER update_leads_updated_at 
    BEFORE UPDATE ON potential_leads 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_sources_updated_at ON sources;
CREATE TRIGGER update_sources_updated_at 
    BEFORE UPDATE ON sources 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- HELPER VIEWS
-- ============================================================

-- View: Active high-priority sources for daily scraping
CREATE OR REPLACE VIEW v_daily_sources AS
SELECT id, name, base_url, source_type, priority, entry_urls, use_playwright, health_status
FROM sources
WHERE is_active = true 
  AND scrape_frequency = 'daily'
  AND health_status != 'dead'
ORDER BY priority DESC, last_scraped_at ASC NULLS FIRST;

-- View: Sources needing attention
CREATE OR REPLACE VIEW v_sources_needing_attention AS
SELECT id, name, base_url, health_status, consecutive_failures, last_success_at
FROM sources
WHERE is_active = true
  AND (health_status IN ('failing', 'dead') OR consecutive_failures >= 3)
ORDER BY consecutive_failures DESC;

-- View: Recent qualified leads (score >= 50, not rejected)
CREATE OR REPLACE VIEW v_qualified_leads AS
SELECT 
    id, hotel_name, brand, brand_tier, city, state, country,
    opening_date, room_count, lead_score, estimated_revenue,
    contact_email, status, created_at
FROM potential_leads
WHERE lead_score >= 50
  AND status NOT IN ('rejected', 'pushed')
  AND duplicate_of_id IS NULL
ORDER BY lead_score DESC, created_at DESC;

-- View: Dashboard stats
CREATE OR REPLACE VIEW v_dashboard_stats AS
SELECT
    (SELECT COUNT(*) FROM potential_leads WHERE duplicate_of_id IS NULL) as total_leads,
    (SELECT COUNT(*) FROM potential_leads WHERE status = 'new' AND duplicate_of_id IS NULL) as new_leads,
    (SELECT COUNT(*) FROM potential_leads WHERE lead_score >= 70 AND duplicate_of_id IS NULL) as hot_leads,
    (SELECT COUNT(*) FROM potential_leads WHERE lead_score BETWEEN 50 AND 69 AND duplicate_of_id IS NULL) as warm_leads,
    (SELECT COUNT(*) FROM potential_leads WHERE status = 'pushed') as pushed_to_crm,
    (SELECT COUNT(*) FROM sources WHERE is_active = true) as active_sources,
    (SELECT COUNT(*) FROM sources WHERE health_status = 'healthy') as healthy_sources,
    (SELECT COUNT(*) FROM sources WHERE health_status IN ('failing', 'dead')) as problem_sources;

-- ============================================================
-- DONE
-- ============================================================

SELECT 'Schema created successfully!' as status;
SELECT 
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE') as tables_created,
    (SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'public') as views_created;