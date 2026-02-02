-- =============================================================================
-- SMART LEAD HUNTER - SEED SOURCES
-- =============================================================================
-- 64 verified hotel news sources with exact "gold" URLs
-- Run AFTER schema.sql
-- =============================================================================


-- =============================================================================
-- MARRIOTT BRANDS (8 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Marriott News - All Brands', 'https://news.marriott.com/news/', 'chain_newsroom', 10, 
 ARRAY['https://news.marriott.com/news/', 'https://news.marriott.com/'], 'daily', true, true,
 'Main Marriott newsroom - covers ALL 30+ brands. Look for: NOW OPEN, DEBUTS, WELCOMES GUESTS.'),
('Marriott - Ritz-Carlton', 'https://news.marriott.com/brands/the-ritz-carlton/', 'chain_newsroom', 10,
 ARRAY['https://news.marriott.com/brands/the-ritz-carlton/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'Ritz-Carlton specific. Tier 2 Luxury - high uniform spend.'),
('Marriott - St. Regis', 'https://news.marriott.com/brands/st-regis/', 'chain_newsroom', 10,
 ARRAY['https://news.marriott.com/brands/st-regis/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'St. Regis specific. Tier 2 Luxury.'),
('Marriott - W Hotels', 'https://news.marriott.com/brands/w-hotels/', 'chain_newsroom', 9,
 ARRAY['https://news.marriott.com/brands/w-hotels/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'W Hotels specific. Tier 3 Upper Upscale lifestyle.'),
('Marriott - EDITION', 'https://news.marriott.com/brands/edition/', 'chain_newsroom', 9,
 ARRAY['https://news.marriott.com/brands/edition/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'EDITION Hotels. Tier 2 Luxury boutique.'),
('Marriott - JW Marriott', 'https://news.marriott.com/brands/jw-marriott/', 'chain_newsroom', 9,
 ARRAY['https://news.marriott.com/brands/jw-marriott/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'JW Marriott specific. Tier 3 Upper Upscale.'),
('Marriott - Luxury Collection', 'https://news.marriott.com/brands/the-luxury-collection/', 'chain_newsroom', 9,
 ARRAY['https://news.marriott.com/brands/the-luxury-collection/', 'https://news.marriott.com/news/'], 'daily', true, true,
 'Luxury Collection. Tier 2 Luxury independents.'),
('Marriott - Autograph Collection', 'https://news.marriott.com/brands/autograph-collection/', 'chain_newsroom', 8,
 ARRAY['https://news.marriott.com/brands/autograph-collection/', 'https://news.marriott.com/news/'], 'weekly', true, true,
 'Autograph Collection. Tier 4 Upscale boutique.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- HILTON BRANDS (9 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Hilton Stories - All Brands', 'https://stories.hilton.com/releases', 'chain_newsroom', 10,
 ARRAY['https://stories.hilton.com/releases', 'https://stories.hilton.com/'], 'daily', true, true,
 'Main Hilton newsroom - ALL 24 brands. Filter by Latest Openings tag.'),
('Hilton - Waldorf Astoria', 'https://stories.hilton.com/releases?brand=waldorf-astoria', 'chain_newsroom', 10,
 ARRAY['https://stories.hilton.com/releases?brand=waldorf-astoria', 'https://stories.hilton.com/releases'], 'daily', true, true,
 'Waldorf Astoria specific. Tier 2 Luxury - ultra high spend.'),
('Hilton - Conrad', 'https://stories.hilton.com/releases?brand=conrad', 'chain_newsroom', 10,
 ARRAY['https://stories.hilton.com/releases?brand=conrad', 'https://stories.hilton.com/releases'], 'daily', true, true,
 'Conrad Hotels. Tier 2 Luxury - growing fast.'),
('Hilton - LXR', 'https://stories.hilton.com/releases?brand=lxr', 'chain_newsroom', 9,
 ARRAY['https://stories.hilton.com/releases?brand=lxr', 'https://stories.hilton.com/releases'], 'daily', true, true,
 'LXR Hotels & Resorts. Tier 2 Luxury collection.'),
('Hilton - Curio Collection', 'https://stories.hilton.com/releases?brand=curio', 'chain_newsroom', 8,
 ARRAY['https://stories.hilton.com/releases?brand=curio', 'https://stories.hilton.com/releases'], 'weekly', true, true,
 'Curio Collection. Tier 4 Upscale boutique.'),
('Hilton - Canopy', 'https://stories.hilton.com/releases?brand=canopy', 'chain_newsroom', 8,
 ARRAY['https://stories.hilton.com/releases?brand=canopy', 'https://stories.hilton.com/releases'], 'weekly', true, true,
 'Canopy by Hilton. Tier 4 Upscale lifestyle.'),
('Hilton - Tempo', 'https://stories.hilton.com/releases?brand=tempo', 'chain_newsroom', 7,
 ARRAY['https://stories.hilton.com/releases?brand=tempo', 'https://stories.hilton.com/releases'], 'weekly', true, true,
 'Tempo by Hilton. Newer lifestyle brand.'),
('Hilton - 2025 Openings', 'https://stories.hilton.com/growth-development/new-hilton-openings-in-2025', 'chain_newsroom', 8,
 ARRAY['https://stories.hilton.com/growth-development/new-hilton-openings-in-2025'], 'monthly', true, true,
 'GOLD PAGE - Annual roundup. Check for 2026 version.'),
('Hilton - 2026 Openings', 'https://stories.hilton.com/releases/new-hilton-openings-in-2026', 'chain_newsroom', 10,
 ARRAY['https://stories.hilton.com/releases/new-hilton-openings-in-2026', 'https://stories.hilton.com/releases'], 'monthly', true, true,
 'GOLD PAGE - 2026 annual roundup.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- HYATT BRANDS (2 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Hyatt Newsroom - All Brands', 'https://newsroom.hyatt.com/news-releases', 'chain_newsroom', 10,
 ARRAY['https://newsroom.hyatt.com/news-releases', 'https://newsroom.hyatt.com/'], 'daily', true, true,
 'Main Hyatt newsroom - ALL 25+ brands. Park Hyatt, Andaz, Grand Hyatt, Thompson.'),
('Hyatt Newsroom - Homepage', 'https://newsroom.hyatt.com/', 'chain_newsroom', 8,
 ARRAY['https://newsroom.hyatt.com/'], 'weekly', true, true,
 'Hyatt homepage - featured stories fallback.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- IHG BRANDS (2 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('IHG Newsroom - All Brands', 'https://www.ihgplc.com/en/news-and-media/news-releases', 'chain_newsroom', 10,
 ARRAY['https://www.ihgplc.com/en/news-and-media/news-releases', 'https://www.ihgplc.com/news-and-media'], 'daily', true, true,
 'Main IHG newsroom - InterContinental, Kimpton, Regent, Six Senses, Vignette.'),
('IHG News & Media', 'https://www.ihgplc.com/news-and-media', 'chain_newsroom', 7,
 ARRAY['https://www.ihgplc.com/news-and-media'], 'weekly', true, true,
 'IHG media landing page fallback.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- ACCOR / ENNISMORE BRANDS (3 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Accor News Stories', 'https://group.accor.com/en/news-medias/news-stories', 'chain_newsroom', 9,
 ARRAY['https://group.accor.com/en/news-medias/news-stories', 'https://group.accor.com/en/news-medias/press-releases'], 'daily', true, true,
 'Accor newsroom - 45+ brands: Raffles, Fairmont, Sofitel, SLS, Delano.'),
('Accor Press Releases', 'https://group.accor.com/en/news-medias/press-releases', 'chain_newsroom', 9,
 ARRAY['https://group.accor.com/en/news-medias/press-releases'], 'daily', true, true,
 'Accor official press releases.'),
('Ennismore News', 'https://ennismore.com/news/', 'chain_newsroom', 9,
 ARRAY['https://ennismore.com/news/', 'https://ennismore.com/'], 'daily', true, true,
 'Ennismore (Accor lifestyle) - SLS, Delano, Mondrian, Hyde, SO/, Mama Shelter.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- LUXURY INDEPENDENT BRANDS (14 sources) - HIGH PRIORITY
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Four Seasons - New Openings', 'https://press.fourseasons.com/search?themes=new_openings', 'luxury_independent', 10,
 ARRAY['https://press.fourseasons.com/search?themes=new_openings', 'https://press.fourseasons.com/news-releases/', 'https://press.fourseasons.com/'], 'daily', true, true,
 'GOLD URL - Four Seasons filtered by New Openings. Tier 2 Luxury. HIGH PRIORITY.'),
('Four Seasons - News Releases', 'https://press.fourseasons.com/news-releases/', 'luxury_independent', 9,
 ARRAY['https://press.fourseasons.com/news-releases/', 'https://press.fourseasons.com/'], 'daily', true, true,
 'Four Seasons main news feed - all releases.'),
('Four Seasons - New Hotels Page', 'https://www.fourseasons.com/landing/new-hotels/', 'luxury_independent', 8,
 ARRAY['https://www.fourseasons.com/landing/new-hotels/'], 'weekly', true, true,
 'Consumer-facing new hotels showcase.'),
('Aman News', 'https://www.aman.com/news', 'luxury_independent', 10,
 ARRAY['https://www.aman.com/news', 'https://www.aman.com/'], 'daily', true, true,
 'Aman Resorts news. Tier 1 ULTRA LUXURY - highest priority.'),
('Rosewood Media Centre', 'https://www.rosewoodhotelgroup.com/media-centre/press-releases/', 'luxury_independent', 10,
 ARRAY['https://www.rosewoodhotelgroup.com/media-centre/press-releases/', 'https://www.rosewoodhotelgroup.com/media-centre/'], 'daily', true, true,
 'Rosewood Hotels press releases. Tier 1 ULTRA LUXURY.'),
('Auberge Resorts Press', 'https://aubergeresorts.com/press/', 'luxury_independent', 9,
 ARRAY['https://aubergeresorts.com/press/', 'https://aubergeresorts.com/'], 'daily', true, true,
 'Auberge Resorts Collection. Tier 2 Luxury.'),
('Montage Hotels Press', 'https://www.montagehotels.com/press/', 'luxury_independent', 9,
 ARRAY['https://www.montagehotels.com/press/', 'https://www.montagehotels.com/'], 'daily', true, true,
 'Montage Hotels & Resorts. Tier 2 Luxury - US focused.'),
('Oetker Collection Press', 'https://www.oetkercollection.com/press/', 'luxury_independent', 9,
 ARRAY['https://www.oetkercollection.com/press/', 'https://www.oetkercollection.com/'], 'weekly', true, true,
 'Oetker Collection. Tier 1 ULTRA LUXURY.'),
('Belmond Press', 'https://www.belmond.com/press-releases', 'luxury_independent', 9,
 ARRAY['https://www.belmond.com/press-releases', 'https://www.belmond.com/'], 'weekly', true, true,
 'Belmond (LVMH). Tier 1 ULTRA LUXURY.'),
('Mandarin Oriental News', 'https://www.mandarinoriental.com/en/media-centre', 'luxury_independent', 10,
 ARRAY['https://www.mandarinoriental.com/en/media-centre', 'https://www.mandarinoriental.com/'], 'daily', true, true,
 'Mandarin Oriental. Tier 1 ULTRA LUXURY.'),
('Peninsula Hotels News', 'https://www.peninsula.com/en/newsroom', 'luxury_independent', 9,
 ARRAY['https://www.peninsula.com/en/newsroom', 'https://www.peninsula.com/'], 'weekly', true, true,
 'The Peninsula Hotels. Tier 1 ULTRA LUXURY.'),
('Loews Hotels Press', 'https://www.loewshotels.com/press-room', 'luxury_independent', 8,
 ARRAY['https://www.loewshotels.com/press-room', 'https://www.loewshotels.com/'], 'weekly', true, true,
 'Loews Hotels & Co. Tier 4 Upscale - US focused.'),
('SH Hotels News', 'https://www.shhotels.com/news/', 'luxury_independent', 9,
 ARRAY['https://www.shhotels.com/news/', 'https://www.shhotels.com/'], 'daily', true, true,
 'SH Hotels - 1 Hotels, Baccarat, Treehouse. Tier 2-3 Luxury/Lifestyle.'),
('Opal Collection Newsroom', 'https://www.opalcollection.com/newsroom/', 'luxury_independent', 8,
 ARRAY['https://www.opalcollection.com/newsroom/', 'https://www.opalcollection.com/'], 'weekly', true, true,
 'Opal Collection - FLORIDA FOCUSED luxury. High relevance.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- AGGREGATORS - CRITICAL CURATED SOURCES (3 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('The Orange Studio - Hotel Openings', 'https://www.theorangestudio.com/hotel-openings', 'aggregator', 10,
 ARRAY['https://www.theorangestudio.com/hotel-openings', 'https://www.theorangestudio.com/news', 'https://www.theorangestudio.com/'], 'daily', true, true,
 'CRITICAL - Curated luxury/lifestyle hotel openings. Filter for USA/Caribbean.'),
('The Orange Studio - 2025 Openings', 'https://www.theorangestudio.com/news/exciting-hotel-openings-in-2025', 'aggregator', 9,
 ARRAY['https://www.theorangestudio.com/news/exciting-hotel-openings-in-2025'], 'weekly', true, true,
 'Annual roundup. Check for 2026 version.'),
('The Orange Studio - Beach Resorts', 'https://www.theorangestudio.com/news/2025s-most-anticipated-beach-resort-openings', 'aggregator', 9,
 ARRAY['https://www.theorangestudio.com/news/2025s-most-anticipated-beach-resort-openings'], 'monthly', true, true,
 'Beach resort openings - high relevance for Caribbean.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- CARIBBEAN SPECIFIC (3 sources) - HIGH PRIORITY
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Caribbean Journal - Hotels', 'https://www.caribjournal.com/category/hotels/', 'caribbean', 10,
 ARRAY['https://www.caribjournal.com/category/hotels/', 'https://www.caribjournal.com/'], 'daily', true, true,
 'BEST Caribbean hotel news source. All islands covered. HIGH PRIORITY.'),
('Caribbean Journal - Homepage', 'https://www.caribjournal.com/', 'caribbean', 8,
 ARRAY['https://www.caribjournal.com/'], 'daily', true, true,
 'Caribbean Journal homepage - featured stories fallback.'),
('CHTA News', 'https://www.caribbeanhotelandtourism.com/news/', 'caribbean', 8,
 ARRAY['https://www.caribbeanhotelandtourism.com/news/', 'https://www.caribbeanhotelandtourism.com/'], 'weekly', true, true,
 'Caribbean Hotel & Tourism Association - industry news.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- FLORIDA SPECIFIC (4 sources) - HIGH PRIORITY
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Visit Florida Press', 'https://www.visitflorida.com/en-us/media/press-releases.html', 'florida', 9,
 ARRAY['https://www.visitflorida.com/en-us/media/press-releases.html'], 'weekly', true, true,
 'Official Florida tourism press releases. HIGH PRIORITY.'),
('Miami CVB Press Room', 'https://www.miamiandbeaches.com/travel-trade/press-room', 'florida', 9,
 ARRAY['https://www.miamiandbeaches.com/travel-trade/press-room'], 'weekly', true, true,
 'Greater Miami CVB - Miami-Dade hotel developments.'),
('South Florida Biz Journal - Hotels', 'https://www.bizjournals.com/southflorida/news/real-estate/hotel', 'florida', 8,
 ARRAY['https://www.bizjournals.com/southflorida/news/real-estate/hotel'], 'weekly', false, true,
 'South Florida hotel real estate news.'),
('Orlando Biz Journal - Hotels', 'https://www.bizjournals.com/orlando/news/real-estate/hotel', 'florida', 8,
 ARRAY['https://www.bizjournals.com/orlando/news/real-estate/hotel'], 'weekly', false, true,
 'Orlando hotel real estate - theme park corridor.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- INDUSTRY NEWS (7 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Hospitality Net - News', 'https://www.hospitalitynet.org/news/', 'industry', 8,
 ARRAY['https://www.hospitalitynet.org/news/'], 'daily', false, true,
 'Major hospitality industry news aggregator. High volume.'),
('Hotel News Resource - Openings', 'https://www.hotelnewsresource.com/topics/Openings.html', 'industry', 9,
 ARRAY['https://www.hotelnewsresource.com/topics/Openings.html', 'https://www.hotelnewsresource.com/'], 'daily', false, true,
 'GOLD URL - Filtered for hotel openings specifically.'),
('Hotel News Resource - Florida', 'https://www.hotelnewsresource.com/topics/Florida.html', 'industry', 9,
 ARRAY['https://www.hotelnewsresource.com/topics/Florida.html'], 'daily', false, true,
 'GOLD URL - Florida specific hotel news. High relevance.'),
('Hotel Dive - News', 'https://www.hoteldive.com/news/', 'industry', 8,
 ARRAY['https://www.hoteldive.com/news/'], 'daily', true, true,
 'Hotel industry deep dive - development pipeline news.'),
('Hotel Management - News', 'https://www.hotelmanagement.net/news', 'industry', 8,
 ARRAY['https://www.hotelmanagement.net/news'], 'daily', true, true,
 'Hotel management industry news.'),
('Skift - Hotels', 'https://skift.com/hotels/', 'industry', 7,
 ARRAY['https://skift.com/hotels/'], 'daily', true, true,
 'Skift travel intelligence - hotels section.'),
('CoStar - Hospitality', 'https://www.costar.com/hospitality', 'industry', 7,
 ARRAY['https://www.costar.com/hospitality'], 'weekly', true, true,
 'CoStar commercial real estate - hospitality. May require login.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- TRAVEL PUBLICATIONS (6 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Travel + Leisure - Hotels', 'https://www.travelandleisure.com/hotels-resorts', 'travel_pub', 7,
 ARRAY['https://www.travelandleisure.com/hotels-resorts'], 'weekly', true, true,
 'Travel + Leisure hotel coverage - consumer perspective.'),
('Conde Nast Traveler - Hotels', 'https://www.cntraveler.com/hotels', 'travel_pub', 7,
 ARRAY['https://www.cntraveler.com/hotels'], 'weekly', true, true,
 'CNT hotel coverage - luxury focus.'),
('Forbes Travel Guide - News', 'https://www.forbestravelguide.com/news', 'travel_pub', 7,
 ARRAY['https://www.forbestravelguide.com/news'], 'weekly', true, true,
 'Forbes Travel Guide - 5-star focus.'),
('Travel Pulse - Hotels', 'https://www.travelpulse.com/news/hotels-resorts', 'travel_pub', 6,
 ARRAY['https://www.travelpulse.com/news/hotels-resorts'], 'weekly', false, true,
 'Travel industry news - hotels section.'),
('Luxury Travel Advisor - News', 'https://www.luxurytraveladvisor.com/news', 'travel_pub', 7,
 ARRAY['https://www.luxurytraveladvisor.com/news'], 'weekly', true, true,
 'Luxury travel trade news.'),
('Northstar Meetings - Hotels', 'https://www.northstarmeetingsgroup.com/news/hotels-resorts', 'travel_pub', 6,
 ARRAY['https://www.northstarmeetingsgroup.com/news/hotels-resorts'], 'weekly', false, true,
 'Meetings industry - hotel news.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- PR WIRE SERVICES (3 sources)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('PR Newswire - Travel/Hospitality', 'https://www.prnewswire.com/news-releases/travel-hospitality-latest-news/travel-hospitality-list/', 'pr_wire', 7,
 ARRAY['https://www.prnewswire.com/news-releases/travel-hospitality-latest-news/travel-hospitality-list/'], 'daily', false, true,
 'PR Newswire travel/hospitality releases. Filter for openings.'),
('Business Wire - Travel/Hospitality', 'https://www.businesswire.com/portal/site/home/news/industries/travel-hospitality/', 'pr_wire', 7,
 ARRAY['https://www.businesswire.com/portal/site/home/news/industries/travel-hospitality/'], 'daily', false, true,
 'Business Wire travel/hospitality releases.'),
('GlobeNewswire - Hospitality', 'https://www.globenewswire.com/en/search/tag/hospitality', 'pr_wire', 6,
 ARRAY['https://www.globenewswire.com/en/search/tag/hospitality'], 'weekly', false, true,
 'GlobeNewswire hospitality tag.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- WYNDHAM (1 source - upscale only)
-- =============================================================================

INSERT INTO sources (name, base_url, source_type, priority, entry_urls, scrape_frequency, use_playwright, is_active, notes) VALUES
('Wyndham News & Media', 'https://corporate.wyndhamhotels.com/news-media/', 'chain_newsroom', 7,
 ARRAY['https://corporate.wyndhamhotels.com/news-media/', 'https://corporate.wyndhamhotels.com/'], 'weekly', true, true,
 'Wyndham newsroom - focus on Wyndham Grand, Registry Collection only. Skip budget brands.')
ON CONFLICT (base_url) DO UPDATE SET
    name = EXCLUDED.name, priority = EXCLUDED.priority, entry_urls = EXCLUDED.entry_urls,
    notes = EXCLUDED.notes, updated_at = NOW();


-- =============================================================================
-- SUMMARY
-- =============================================================================

SELECT '✅ Sources seeded successfully!' as status;

SELECT 
    source_type,
    COUNT(*) as count,
    SUM(CASE WHEN priority >= 9 THEN 1 ELSE 0 END) as high_priority
FROM sources 
WHERE is_active = true
GROUP BY source_type
ORDER BY count DESC;

SELECT 
    'TOTAL' as metric,
    COUNT(*) as value
FROM sources WHERE is_active = true
UNION ALL
SELECT 'Daily scrape', COUNT(*) FROM sources WHERE scrape_frequency = 'daily' AND is_active = true
UNION ALL
SELECT 'Weekly scrape', COUNT(*) FROM sources WHERE scrape_frequency = 'weekly' AND is_active = true
UNION ALL
SELECT 'Priority 10', COUNT(*) FROM sources WHERE priority = 10 AND is_active = true
UNION ALL
SELECT 'Priority 9', COUNT(*) FROM sources WHERE priority = 9 AND is_active = true;