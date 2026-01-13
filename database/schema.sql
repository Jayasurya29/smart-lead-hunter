-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- TABLE: sources
-- Stores the 150+ websites we scrape
-- ============================================================
CREATE TABLE sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    url VARCHAR(500) NOT NULL,
    scrape_frequency VARCHAR(20) DEFAULT 'daily',
    is_active BOOLEAN DEFAULT true,
    last_scraped_at TIMESTAMP,
    leads_found INTEGER DEFAULT 0,
    success_rate DECIMAL(5,2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- TABLE: potential_leads
-- Scraped leads waiting for review before pushing to Insightly
-- ============================================================
CREATE TABLE potential_leads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Hotel Information
    hotel_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    hotel_website VARCHAR(500),
    
    -- Location
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',
    
    -- Contact Information
    contact_first_name VARCHAR(100),
    contact_last_name VARCHAR(100),
    contact_title VARCHAR(100),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    
    -- Hotel Details
    projected_opening_date DATE,
    room_count INTEGER,
    description TEXT,
    
    -- Scoring
    lead_score INTEGER CHECK (lead_score >= 0 AND lead_score <= 100),
    score_breakdown JSONB,
    
    -- Source Tracking
    source_id UUID REFERENCES sources(id),
    source_url TEXT NOT NULL,
    source_site VARCHAR(100) NOT NULL,
    scraped_at TIMESTAMP DEFAULT NOW(),
    
    -- Workflow Status
    status VARCHAR(20) DEFAULT 'New' CHECK (status IN ('New', 'Claimed', 'Approved', 'Rejected')),
    claimed_by VARCHAR(100),
    claimed_at TIMESTAMP,
    rejection_reason VARCHAR(50) CHECK (rejection_reason IN ('Duplicate', 'Bad Data', 'Wrong Market', 'Not a Hotel', 'Already Client', 'Other')),
    rejection_notes TEXT,
    
    -- Insightly Sync
    insightly_lead_id BIGINT,
    pushed_to_insightly_at TIMESTAMP,
    
    -- Deduplication
    embedding VECTOR(384),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- TABLE: scrape_logs
-- History of scraping runs for monitoring
-- ============================================================
CREATE TABLE scrape_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID REFERENCES sources(id),
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    pages_crawled INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    leads_new INTEGER DEFAULT 0,
    leads_duplicate INTEGER DEFAULT 0,
    errors TEXT,
    status VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- INDEXES for performance
-- ============================================================

-- Potential Leads indexes
CREATE INDEX idx_potential_leads_status ON potential_leads(status);
CREATE INDEX idx_potential_leads_score ON potential_leads(lead_score DESC);
CREATE INDEX idx_potential_leads_source ON potential_leads(source_site);
CREATE INDEX idx_potential_leads_hotel ON potential_leads(hotel_name);
CREATE INDEX idx_potential_leads_state ON potential_leads(state);
CREATE INDEX idx_potential_leads_created ON potential_leads(created_at DESC);
CREATE INDEX idx_potential_leads_insightly ON potential_leads(insightly_lead_id);

-- Vector similarity index for deduplication
CREATE INDEX idx_potential_leads_embedding ON potential_leads 
USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Full-text search index
CREATE INDEX idx_potential_leads_search ON potential_leads 
USING GIN (to_tsvector('english', hotel_name || ' ' || COALESCE(description, '')));

-- Fuzzy matching index for hotel names
CREATE INDEX idx_potential_leads_name_trgm ON potential_leads USING gin (hotel_name gin_trgm_ops);

-- Sources indexes
CREATE INDEX idx_sources_active ON sources(is_active);
CREATE INDEX idx_sources_frequency ON sources(scrape_frequency);

-- Scrape Logs indexes
CREATE INDEX idx_scrape_logs_source ON scrape_logs(source_id);
CREATE INDEX idx_scrape_logs_status ON scrape_logs(status);
CREATE INDEX idx_scrape_logs_date ON scrape_logs(started_at DESC);

-- ============================================================
-- FUNCTIONS
-- ============================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_potential_leads_updated_at 
    BEFORE UPDATE ON potential_leads 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sources_updated_at 
    BEFORE UPDATE ON sources 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();