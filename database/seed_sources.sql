-- ============================================================================
-- SMART LEAD HUNTER - MAJOR HOTEL CHAIN NEWSROOM SOURCES
-- Verified Press Release URLs for New Hotel Opening Announcements
-- Last Updated: January 30, 2026
-- ============================================================================

-- IMPORTANT: These are the OFFICIAL newsroom URLs for major hotel chains
-- Each chain owns multiple brands under one corporate umbrella
-- These URLs announce new hotel openings, groundbreakings, and expansions

-- ============================================================================
-- TIER 1: MARRIOTT INTERNATIONAL (30+ brands)
-- Brands: Ritz-Carlton, St. Regis, W Hotels, EDITION, JW Marriott, 
--         Luxury Collection, Westin, Sheraton, Marriott, Renaissance,
--         Autograph Collection, Tribute Portfolio, Courtyard, Aloft, Moxy
-- ============================================================================

INSERT INTO sources (name, url, category, scrape_frequency, is_active, notes) VALUES
('Marriott News Center', 'https://news.marriott.com/', 'chain_newsroom', 'daily', true, 
 'PRIMARY - Main corporate newsroom for all 30+ Marriott brands including Ritz-Carlton, St. Regis, W Hotels'),

('Marriott Media Centre EMEA', 'https://marriott.pressarea.com/pressrelease?keywords=Opening', 'chain_newsroom', 'weekly', true,
 'Europe/Middle East/Africa regional newsroom - filter by Opening keyword'),

('Marriott Investor Relations', 'https://marriott.gcs-web.com/news-releases', 'chain_newsroom', 'weekly', true,
 'Financial press releases with quarterly development pipeline updates'),

('Marriott New Openings Page', 'https://www.marriott.com/en-us/marriott-brands/portfolio/openings.mi', 'chain_newsroom', 'weekly', true,
 'Consumer-facing new hotel openings list - good for confirmed openings'),

-- ============================================================================
-- TIER 2: HILTON WORLDWIDE (24 brands)
-- Brands: Waldorf Astoria, Conrad, LXR, NoMad, Signia, Canopy, Curio,
--         Tapestry, DoubleTree, Embassy Suites, Hilton, Hilton Garden Inn
-- ============================================================================

('Hilton Stories Main', 'https://stories.hilton.com/', 'chain_newsroom', 'daily', true,
 'PRIMARY - Main newsroom for all 24 Hilton brands including Waldorf Astoria, Conrad'),

('Hilton Press Releases', 'https://stories.hilton.com/releases', 'chain_newsroom', 'daily', true,
 'Direct press releases feed - all brands'),

('Hilton New Openings 2026', 'https://stories.hilton.com/releases/new-hilton-openings-in-2026', 'chain_newsroom', 'monthly', true,
 'Annual new openings roundup - updated each year'),

('Hilton Luxury Hot List', 'https://stories.hilton.com/releases/hiltons-luxury-lifestyle-hot-list-2025-year-in-review-2026-strategic-expansion', 'chain_newsroom', 'monthly', true,
 'Luxury and lifestyle brand openings - Waldorf, Conrad, LXR, NoMad'),

-- ============================================================================
-- TIER 3: HYATT HOTELS CORPORATION (25+ brands)
-- Brands: Park Hyatt, Alila, Miraval, Andaz, Thompson, The Standard,
--         Grand Hyatt, Hyatt Regency, Hyatt Ziva, Hyatt Zilara, Secrets, Dreams
-- ============================================================================

('Hyatt Newsroom', 'https://newsroom.hyatt.com/news-releases', 'chain_newsroom', 'daily', true,
 'PRIMARY - Main newsroom for all Hyatt brands including Park Hyatt, Alila, Thompson'),

('Hyatt Investor News', 'https://investors.hyatt.com/news/investor-news/default.aspx', 'chain_newsroom', 'weekly', true,
 'Financial press releases with development pipeline data'),

-- ============================================================================
-- TIER 4: IHG HOTELS & RESORTS (20 brands)
-- Brands: Six Senses, Regent, InterContinental, Vignette, Kimpton,
--         Hotel Indigo, voco, Crowne Plaza, Holiday Inn, Staybridge Suites
-- ============================================================================

('IHG News Releases', 'https://www.ihgplc.com/en/news-and-media/news-releases', 'chain_newsroom', 'daily', true,
 'PRIMARY - Main newsroom for all IHG brands including Six Senses, Regent, InterContinental, Kimpton'),

('IHG News & Media Main', 'https://www.ihgplc.com/news-and-media', 'chain_newsroom', 'daily', true,
 'Corporate news hub - all announcements'),

('IHG Development News', 'https://development.ihg.com/resources/news-releases', 'chain_newsroom', 'weekly', true,
 'Development-focused news for owners and developers'),

('IHG Owners Association', 'https://www.owners.org/resources/news-and-events', 'chain_newsroom', 'weekly', true,
 'Franchisee news - often has early development announcements'),

-- ============================================================================
-- TIER 5: ACCOR (45+ brands including Ennismore)
-- Brands: Orient Express, Raffles, Fairmont, Sofitel, MGallery, Pullman,
--         Swissôtel, Mövenpick, Novotel, Delano, Mondrian, SLS, Hyde
-- ============================================================================

('Accor News & Stories', 'https://group.accor.com/en/news-stories', 'chain_newsroom', 'daily', true,
 'PRIMARY - Main newsroom for all 45+ Accor brands including Raffles, Fairmont, Sofitel'),

('Accor Pressroom', 'https://press.accor.com/', 'chain_newsroom', 'daily', true,
 'Direct press releases - all brands'),

('Accor 2026 Openings', 'https://group.accor.com/en/news-stories/accor-2026-openings', 'chain_newsroom', 'monthly', true,
 'Annual openings roundup'),

('Accor 2025 Openings', 'https://group.accor.com/en/news-stories/accor-hotel-openings-2025', 'chain_newsroom', 'monthly', true,
 'Previous year openings - reference'),

-- ============================================================================
-- TIER 6: WYNDHAM HOTELS & RESORTS (25 brands)
-- Brands: Wyndham Grand, Registry Collection, Dolce, Trademark,
--         Wyndham, Wyndham Garden, La Quinta, Days Inn (EXCLUDE budget)
-- ============================================================================

('Wyndham News & Media', 'https://corporate.wyndhamhotels.com/news-media/', 'chain_newsroom', 'daily', true,
 'PRIMARY - Main newsroom for Wyndham brands - focus on Wyndham Grand, Registry Collection'),

('Wyndham News Releases', 'https://corporate.wyndhamhotels.com/news-releases/', 'chain_newsroom', 'daily', true,
 'Direct press releases archive'),

('Wyndham Investor Relations', 'https://investor.wyndhamhotels.com/news-events/press-releases', 'chain_newsroom', 'weekly', true,
 'Financial press releases with development updates'),

('Wyndham Business News', 'https://www.wyndhambusiness.com/news-and-events/', 'chain_newsroom', 'weekly', true,
 'B2B news for corporate travel'),

-- ============================================================================
-- TIER 7: FOUR SEASONS (Single ultra-luxury brand)
-- HIGH PRIORITY - Major uniform opportunity
-- ============================================================================

('Four Seasons Press Room', 'https://press.fourseasons.com/', 'chain_newsroom', 'daily', true,
 'PRIMARY - Ultra-luxury single brand - HIGH PRIORITY for uniform sales'),

('Four Seasons News Releases', 'https://press.fourseasons.com/news-releases/', 'chain_newsroom', 'daily', true,
 'Direct press releases'),

('Four Seasons 2026 Travel', 'https://press.fourseasons.com/news-releases/2025/where-to-travel-in-2026/', 'chain_newsroom', 'monthly', true,
 'Annual openings and renovations guide'),

('Four Seasons New Openings', 'https://www.fourseasons.com/newopenings/', 'chain_newsroom', 'weekly', true,
 'Consumer-facing new openings page'),

('PR Newswire Four Seasons', 'https://www.prnewswire.com/news/four-seasons-hotels-and-resorts/', 'aggregator', 'daily', true,
 'Aggregated press releases - backup source'),

-- ============================================================================
-- TIER 8: BWH HOTELS (Best Western - 18 brands)
-- Focus on WorldHotels luxury segment only
-- ============================================================================

('BWH Press Releases', 'https://www.bestwestern.com/en_US/about/press-media.html', 'chain_newsroom', 'weekly', true,
 'Main newsroom - focus on WorldHotels Luxury, Elite brands only'),

-- ============================================================================
-- TIER 9: LUXURY INDEPENDENT BRANDS
-- HIGH PRIORITY - Ultra-luxury independent chains
-- ============================================================================

('Aman New Developments', 'https://www.aman.com/new-developments', 'chain_newsroom', 'weekly', true,
 'HIGH PRIORITY - Ultra-luxury Aman resorts - pipeline includes Miami Beach, Beverly Hills'),

('Aman Trade News', 'https://www.aman.com/trade-professionals/new-noteworthy', 'chain_newsroom', 'weekly', true,
 'Trade professional updates'),

('Rosewood Media', 'https://www.rosewoodhotels.com/en/media', 'chain_newsroom', 'weekly', true,
 'HIGH PRIORITY - Ultra-luxury Rosewood Hotels - 54 properties, 30+ in development'),

('Rosewood Hotel Group', 'https://www.rosewoodhotelgroup.com/en-us/news-and-media', 'chain_newsroom', 'weekly', true,
 'Corporate newsroom including New World Hotels'),

('Loews Hotels Press', 'https://www.loewshotels.com/press/press-releases/press-categories/press-releases', 'chain_newsroom', 'weekly', true,
 'FLORIDA PRIORITY - Properties in Miami Beach, Orlando - luxury American chain'),

-- ============================================================================
-- TIER 10: PR AGGREGATORS (Backup Sources)
-- Use to catch releases that may not appear on brand newsrooms
-- ============================================================================

('PR Newswire Marriott', 'https://www.prnewswire.com/news/marriott-international,-inc./', 'aggregator', 'weekly', true,
 'Marriott press releases aggregator'),

('PR Newswire Hyatt', 'https://www.prnewswire.com/search/news/?keyword=hyatt+hotels+opening', 'aggregator', 'weekly', true,
 'Hyatt press releases aggregator'),

('PR Newswire IHG', 'https://www.prnewswire.com/news/intercontinental-hotels-group-(ihg)/', 'aggregator', 'weekly', true,
 'IHG press releases aggregator'),

('PR Newswire Wyndham', 'https://www.prnewswire.com/news/wyndham-hotels-&-resorts/', 'aggregator', 'weekly', true,
 'Wyndham press releases aggregator'),

('PR Newswire Accor', 'https://www.prnewswire.com/search/news/?keyword=accor+hotel+opening', 'aggregator', 'weekly', true,
 'Accor press releases aggregator'),

('PR Newswire Rosewood', 'https://www.prnewswire.com/search/news/?keyword=rosewood+hotel+opening', 'aggregator', 'weekly', true,
 'Rosewood press releases aggregator');

-- ============================================================================
-- BRAND REFERENCE TABLE
-- Maps major chains to their luxury/target brands for filtering
-- ============================================================================

/*
BRAND PRIORITY REFERENCE (Focus for Smart Lead Hunter):

MARRIOTT INTERNATIONAL:
  - LUXURY: Ritz-Carlton, St. Regis, W Hotels, EDITION, JW Marriott, Luxury Collection
  - UPPER: Westin, Sheraton, Marriott Hotels, Renaissance, Autograph Collection
  - SKIP: Courtyard, Fairfield, Moxy (mid-scale)

HILTON WORLDWIDE:
  - LUXURY: Waldorf Astoria, Conrad, LXR Hotels, NoMad, Signia
  - UPPER: Canopy, Curio Collection, Tapestry, DoubleTree, Embassy Suites
  - SKIP: Hampton, Tru, Home2 (mid-scale/budget)

HYATT:
  - LUXURY: Park Hyatt, Alila, Miraval, Andaz, Thompson, The Standard
  - UPPER: Grand Hyatt, Hyatt Regency, Hyatt Centric
  - RESORTS: Hyatt Ziva, Hyatt Zilara, Secrets, Dreams (Caribbean focus!)
  - SKIP: Hyatt Place, Hyatt House (mid-scale)

IHG:
  - LUXURY: Six Senses, Regent, InterContinental
  - UPPER: Vignette, Kimpton, Hotel Indigo, voco, Crowne Plaza
  - SKIP: Holiday Inn, Holiday Inn Express (mid-scale)

ACCOR:
  - LUXURY: Orient Express, Raffles, Fairmont, Sofitel Legend
  - UPPER: Sofitel, MGallery, Pullman, Swissôtel
  - LIFESTYLE: Delano, Mondrian, SLS, Hyde, 21c (Ennismore)
  - SKIP: Novotel, Mercure, ibis (mid-scale/budget)

WYNDHAM:
  - LUXURY: Wyndham Grand, Registry Collection, Dolce
  - UPPER: Trademark Collection, Wyndham
  - SKIP: Days Inn, Super 8, La Quinta, Microtel (budget)

INDEPENDENTS (ALL HIGH PRIORITY):
  - Four Seasons
  - Aman Resorts
  - Rosewood Hotels
  - Loews Hotels
*/

-- ============================================================================
-- SCRAPING NOTES
-- ============================================================================

/*
SCRAPING STRATEGY:

1. DAILY SCRAPE (High Volume):
   - Marriott News Center
   - Hilton Stories
   - Hyatt Newsroom
   - IHG News Releases
   - Accor News & Stories
   - Wyndham News Releases
   - Four Seasons Press Room

2. WEEKLY SCRAPE (Supplemental):
   - Regional newsrooms (EMEA, APAC, Africa)
   - Investor relations pages
   - PR Newswire aggregators
   - Luxury independents (Aman, Rosewood, Loews)

3. MONTHLY SCRAPE (Annual Roundups):
   - Annual "New Openings" pages
   - Luxury hot lists
   - Pipeline announcements

KEY EXTRACTION TARGETS:
   - Hotel name
   - Brand
   - Location (City, State/Province, Country)
   - Opening date (or "Coming Soon" / Year)
   - Room count
   - Property type (Resort, Hotel, All-Inclusive)
   - Contact information (if available)
   - Source URL

FILTERING RULES:
   - INCLUDE: Florida, Caribbean, Bahamas locations
   - INCLUDE: Luxury and upper-upscale brands
   - EXCLUDE: Budget brands (Days Inn, Super 8, etc.)
   - EXCLUDE: International locations outside Americas
   - EXCLUDE: Press releases about renovations (unless major expansion)
*/