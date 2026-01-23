-- ============================================================================
-- SMART LEAD HUNTER - SEED SOURCES
-- Populates the sources table with validated hotel opening websites
-- ============================================================================

-- Aggregator Sites (High Priority - Multiple hotels per page)
INSERT INTO sources (id, name, url, scrape_frequency, is_active, leads_found, notes, created_at) VALUES
(gen_random_uuid(), 'The Orange Studio - Hotel Openings', 'https://www.theorangestudio.com/hotel-openings', 'daily', true, 0, 'Aggregator - Lists multiple new hotel openings. Score: 96/100', NOW()),
(gen_random_uuid(), 'New Hotels Opening', 'https://www.newhotelsopening.com/', 'daily', true, 0, 'Aggregator - Dedicated to new hotel announcements', NOW()),
(gen_random_uuid(), 'NewSleeps', 'https://www.newsleeps.com/index.php', 'daily', true, 0, 'Aggregator - New hotel listings worldwide', NOW()),
(gen_random_uuid(), 'Hotel Online - New Openings', 'https://www.hotel-online.com/categories/new-hotel-openings', 'daily', true, 0, 'Industry news - New openings category', NOW());

-- Luxury Hotel Sources (High Priority - Target Market)
INSERT INTO sources (id, name, url, scrape_frequency, is_active, leads_found, notes, created_at) VALUES
(gen_random_uuid(), 'Leading Hotels - Opening Soon', 'https://www.lhw.com/get-inspired/new-hotels/opening-soon', 'weekly', true, 0, 'Luxury - Leading Hotels of the World new properties. Score: 89/100', NOW()),
(gen_random_uuid(), 'Four Seasons Press Room', 'https://press.fourseasons.com', 'weekly', true, 0, 'Luxury chain - Press releases for new openings', NOW()),
(gen_random_uuid(), 'Mandarin Oriental News', 'https://www.mandarinoriental.com/newsroom', 'weekly', true, 0, 'Luxury chain - Newsroom announcements', NOW());

-- Major Hotel Chain Newsrooms
INSERT INTO sources (id, name, url, scrape_frequency, is_active, leads_found, notes, created_at) VALUES
(gen_random_uuid(), 'Hilton Stories - New Openings 2026', 'https://stories.hilton.com/releases/new-hilton-openings-in-2026', 'weekly', true, 0, 'Major chain - Hilton new openings for Americas', NOW()),
(gen_random_uuid(), 'Marriott News Center', 'https://news.marriott.com', 'weekly', true, 0, 'Major chain - Marriott press releases', NOW()),
(gen_random_uuid(), 'Hyatt Newsroom', 'https://newsroom.hyatt.com', 'weekly', true, 0, 'Major chain - Hyatt announcements', NOW()),
(gen_random_uuid(), 'IHG Newsroom', 'https://www.ihgplc.com/en/news-and-media', 'weekly', true, 0, 'Major chain - IHG news and media', NOW()),
(gen_random_uuid(), 'Accor Newsroom', 'https://group.accor.com/en/newsroom', 'weekly', true, 0, 'Major chain - Accor group news', NOW()),
(gen_random_uuid(), 'Wyndham Newsroom', 'https://corporate.wyndham.com/newsroom', 'weekly', true, 0, 'Major chain - Wyndham announcements', NOW()),
(gen_random_uuid(), 'Drury Hotels Coming Soon', 'https://www.druryhotels.com/coming-soon', 'weekly', true, 0, 'Regional chain - Drury new locations', NOW());

-- Hospitality Industry Publications
INSERT INTO sources (id, name, url, scrape_frequency, is_active, leads_found, notes, created_at) VALUES
(gen_random_uuid(), 'Travel + Leisure - Hotel Openings', 'https://www.travelandleisure.com/hotels-resorts/hotel-openings', 'daily', true, 0, 'Publication - Travel + Leisure hotel coverage', NOW()),
(gen_random_uuid(), 'Hospitality Net', 'https://www.hospitalitynet.org', 'daily', true, 0, 'Industry publication - Hotel news', NOW()),
(gen_random_uuid(), 'Hotel News Resource', 'https://www.hotelnewsresource.com', 'daily', true, 0, 'Industry publication - Hotel industry news', NOW()),
(gen_random_uuid(), 'Boutique Hotel News', 'https://www.boutiquehotelnews.com', 'daily', true, 0, 'Publication - Boutique and luxury hotels', NOW()),
(gen_random_uuid(), 'Hotel Management', 'https://www.hotelmanagement.net', 'daily', true, 0, 'Industry publication - Hotel management news', NOW()),
(gen_random_uuid(), 'Luxury Travel Advisor', 'https://www.luxurytraveladvisor.com', 'weekly', true, 0, 'Publication - Luxury travel and hotels', NOW()),
(gen_random_uuid(), 'Travel Weekly', 'https://www.travelweekly.com', 'daily', true, 0, 'Publication - Travel industry news. Score: 80/100', NOW());

-- Caribbean & Regional Sources (Target Market - Florida/Caribbean Focus)
INSERT INTO sources (id, name, url, scrape_frequency, is_active, leads_found, notes, created_at) VALUES
(gen_random_uuid(), 'Caribbean Journal', 'https://www.caribjournal.com', 'daily', true, 0, 'Regional - Caribbean travel and hotel news. Target market!', NOW());

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
-- Run this after inserting to verify:
-- SELECT name, url, is_active FROM sources ORDER BY name;