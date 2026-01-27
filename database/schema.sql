-- ============================================================
-- SMART LEAD HUNTER - DATABASE SCHEMA
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- TABLE: sources
-- ============================================================
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    base_url VARCHAR(500) NOT NULL,
    source_type VARCHAR(50) DEFAULT 'aggregator',
    priority INTEGER DEFAULT 5,
    entry_urls TEXT[],
    scrape_frequency VARCHAR(20) DEFAULT 'daily',
    max_depth INTEGER DEFAULT 2,
    use_playwright BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    last_scraped_at TIMESTAMPTZ,
    leads_found INTEGER DEFAULT 0,
    success_rate DECIMAL(5,2),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TABLE: potential_leads
-- ============================================================
CREATE TABLE IF NOT EXISTS potential_leads (
    id SERIAL PRIMARY KEY,
    
    -- Hotel Information
    hotel_name VARCHAR(255) NOT NULL,
    hotel_name_normalized VARCHAR(255),
    brand VARCHAR(100),
    hotel_type VARCHAR(50),
    hotel_website VARCHAR(500),
    
    -- Location
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',
    
    -- Contact Information
    contact_name VARCHAR(200),
    contact_title VARCHAR(100),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    
    -- Hotel Details
    opening_date VARCHAR(50),
    room_count INTEGER,
    description TEXT,
    
    -- Scoring
    lead_score INTEGER CHECK (lead_score >= 0 AND lead_score <= 100),
    score_breakdown JSONB,
    
    -- Source Tracking
    source_id INTEGER REFERENCES sources(id),
    source_url TEXT,
    source_site VARCHAR(100),
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Workflow Status
    status VARCHAR(20) DEFAULT 'new',
    claimed_by VARCHAR(100),
    claimed_at TIMESTAMPTZ,
    notes TEXT,
    
    -- Insightly Sync
    insightly_id INTEGER,
    synced_at TIMESTAMPTZ,
    
    -- Deduplication
    embedding VECTOR(384),
    duplicate_of_id INTEGER,
    
    -- Raw data
    raw_data JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TABLE: scrape_logs
-- ============================================================
CREATE TABLE IF NOT EXISTS scrape_logs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    urls_scraped INTEGER DEFAULT 0,
    pages_crawled INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    leads_new INTEGER DEFAULT 0,
    leads_duplicate INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running',
    errors JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Potential Leads indexes
CREATE INDEX IF NOT EXISTS idx_leads_status ON potential_leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_score ON potential_leads(lead_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_leads_state ON potential_leads(state);
CREATE INDEX IF NOT EXISTS idx_leads_created ON potential_leads(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_normalized_name ON potential_leads(hotel_name_normalized);

-- Vector similarity index for deduplication
CREATE INDEX IF NOT EXISTS idx_leads_embedding ON potential_leads 
USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Fuzzy matching index
CREATE INDEX IF NOT EXISTS idx_leads_name_trgm ON potential_leads 
USING gin (hotel_name gin_trgm_ops);

-- Sources indexes
CREATE INDEX IF NOT EXISTS idx_sources_active ON sources(is_active);
CREATE INDEX IF NOT EXISTS idx_sources_priority ON sources(priority DESC);

-- Scrape Logs indexes
CREATE INDEX IF NOT EXISTS idx_scrape_logs_source ON scrape_logs(source_id);
CREATE INDEX IF NOT EXISTS idx_scrape_logs_started ON scrape_logs(started_at DESC);

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