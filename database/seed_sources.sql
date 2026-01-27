-- ============================================================================
-- SMART LEAD HUNTER - SEED SOURCES
-- FOCUS: United States + Caribbean ONLY
-- ============================================================================

-- Clear existing sources (optional - comment out if you want to keep existing)
-- TRUNCATE TABLE sources RESTART IDENTITY CASCADE;

-- ============================================================================
-- TIER 1: AGGREGATOR SITES (Filter for US/Caribbean content)
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('The Orange Studio - Hotel Openings', 'https://www.theorangestudio.com/hotel-openings', 'daily', true, 'Aggregator - Filter for US/Caribbean only'),
('New Hotels Opening', 'https://www.newhotelsopening.com/', 'daily', true, 'Aggregator - Filter for US/Caribbean only'),
('Hotel Openings List 2025', 'https://www.newhotelsopening.com/hotel-openings-2025', 'daily', true, 'Aggregator - 2025 openings'),
('Hotel Openings List 2026', 'https://www.newhotelsopening.com/hotel-openings-2026', 'daily', true, 'Aggregator - 2026 openings'),
('Hotel Online - New Openings', 'https://www.hotel-online.com/press_releases/release/new-hotel-openings', 'daily', true, 'Industry aggregator');

-- ============================================================================
-- TIER 2: FLORIDA SOURCES (53% of your clients!)
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('Florida Restaurant & Lodging', 'https://frla.org/news/', 'daily', true, 'FLORIDA: State association news'),
('South Florida Business Journal - Hotels', 'https://www.bizjournals.com/southflorida/news/industry/hotels', 'daily', true, 'FLORIDA: Miami, Fort Lauderdale, Palm Beach'),
('Orlando Business Journal - Hotels', 'https://www.bizjournals.com/orlando/news/industry/hotels', 'daily', true, 'FLORIDA: Orlando area'),
('Tampa Bay Business Journal - Hotels', 'https://www.bizjournals.com/tampabay/news/industry/hotels', 'daily', true, 'FLORIDA: Tampa, Clearwater, St Pete'),
('Jacksonville Business Journal', 'https://www.bizjournals.com/jacksonville/news/industry/hotels', 'daily', true, 'FLORIDA: Jacksonville area'),
('Miami Herald - Travel', 'https://www.miamiherald.com/travel/', 'daily', true, 'FLORIDA: Miami travel news'),
('Orlando Sentinel - Theme Parks', 'https://www.orlandosentinel.com/theme-parks/', 'daily', true, 'FLORIDA: Orlando attractions/hotels'),
('Visit Florida News', 'https://www.visitflorida.com/en-us/media/press-releases.html', 'weekly', true, 'FLORIDA: State tourism news'),
('Florida Trend - Hospitality', 'https://www.floridatrend.com/hospitality', 'weekly', true, 'FLORIDA: Business magazine'),
('Naples Daily News', 'https://www.naplesnews.com/business/', 'daily', true, 'FLORIDA: Naples, Southwest Florida'),
('Sun Sentinel - Hotels', 'https://www.sun-sentinel.com/business/', 'daily', true, 'FLORIDA: Fort Lauderdale area'),
('Palm Beach Post', 'https://www.palmbeachpost.com/business/', 'daily', true, 'FLORIDA: Palm Beach area'),
('Sarasota Herald-Tribune', 'https://www.heraldtribune.com/business/', 'weekly', true, 'FLORIDA: Sarasota area'),
('Keys Weekly', 'https://keysweekly.com/', 'weekly', true, 'FLORIDA: Florida Keys');

-- ============================================================================
-- TIER 3: CARIBBEAN SOURCES (5% of your clients - growth opportunity)
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('Caribbean Journal', 'https://www.caribjournal.com/', 'daily', true, 'CARIBBEAN: Leading Caribbean travel news'),
('Caribbean Hotel & Tourism Association', 'https://www.caribbeanhotelandtourism.com/news/', 'daily', true, 'CARIBBEAN: Industry association'),
('Loop Caribbean', 'https://caribbean.loopnews.com/', 'daily', true, 'CARIBBEAN: Regional news'),
('Caribbean News Digital', 'https://www.caribbeannewsdigital.com/', 'daily', true, 'CARIBBEAN: Regional news'),
('Bahamas Ministry of Tourism', 'https://www.bahamas.com/press-releases', 'weekly', true, 'CARIBBEAN: Bahamas - 8 clients'),
('Jamaica Tourist Board', 'https://www.visitjamaica.com/press-room/', 'weekly', true, 'CARIBBEAN: Jamaica - 2 clients'),
('Turks and Caicos Tourism', 'https://www.turksandcaicostourism.com/media/', 'weekly', true, 'CARIBBEAN: TCI - 8 clients'),
('Barbados Tourism', 'https://www.visitbarbados.org/media-centre', 'weekly', true, 'CARIBBEAN: Barbados - 4 clients'),
('Aruba Tourism', 'https://www.aruba.com/us/press', 'weekly', true, 'CARIBBEAN: Aruba'),
('Puerto Rico Tourism', 'https://www.discoverpuertorico.com/press-room', 'weekly', true, 'CARIBBEAN: Puerto Rico'),
('Cayman Islands Tourism', 'https://www.visitcaymanislands.com/en-us/about-cayman/media-centre', 'weekly', true, 'CARIBBEAN: Cayman - 8 clients'),
('St. Lucia Tourism', 'https://www.stlucia.org/en/media/', 'weekly', true, 'CARIBBEAN: St. Lucia - 4 clients'),
('USVI Tourism', 'https://www.visitusvi.com/media', 'weekly', true, 'CARIBBEAN: US Virgin Islands'),
('BVI Tourism', 'https://www.bvitourism.com/media', 'weekly', true, 'CARIBBEAN: British Virgin Islands - 2 clients');

-- ============================================================================
-- TIER 4: OTHER US STATES (33% of your clients)
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
-- California (4.5% of clients)
('LA Business Journal - Hotels', 'https://labusinessjournal.com/news/hotels/', 'weekly', true, 'USA: Los Angeles'),
('San Diego Business Journal', 'https://www.sdbj.com/news/hotels-tourism/', 'weekly', true, 'USA: San Diego'),
('SF Business Times - Hotels', 'https://www.bizjournals.com/sanfrancisco/news/industry/hotels', 'weekly', true, 'USA: San Francisco'),

-- New York (3.5% of clients)
('NYC Tourism Press', 'https://www.nycgo.com/press-and-media', 'weekly', true, 'USA: New York City'),
('Crain NY - Hotels', 'https://www.crainsnewyork.com/hospitality-tourism', 'weekly', true, 'USA: New York'),

-- Texas (2.6% of clients)
('Dallas Business Journal - Hotels', 'https://www.bizjournals.com/dallas/news/industry/hotels', 'weekly', true, 'USA: Dallas'),
('Houston Business Journal - Hotels', 'https://www.bizjournals.com/houston/news/industry/hotels', 'weekly', true, 'USA: Houston'),
('San Antonio Business Journal', 'https://www.bizjournals.com/sanantonio/news/industry/hotels', 'weekly', true, 'USA: San Antonio'),
('Austin Business Journal - Hotels', 'https://www.bizjournals.com/austin/news/industry/hotels', 'weekly', true, 'USA: Austin'),

-- Georgia (2.3% of clients)
('Atlanta Business Chronicle - Hotels', 'https://www.bizjournals.com/atlanta/news/industry/hotels', 'weekly', true, 'USA: Atlanta'),

-- Tennessee (2.2% of clients)
('Nashville Business Journal - Hotels', 'https://www.bizjournals.com/nashville/news/industry/hotels', 'weekly', true, 'USA: Nashville'),
('Memphis Business Journal', 'https://www.bizjournals.com/memphis/news/industry/hotels', 'weekly', true, 'USA: Memphis'),

-- South Carolina (1.7% of clients)
('Charleston Business Journal', 'https://www.bizjournals.com/charleston/news/industry/hotels', 'weekly', true, 'USA: Charleston'),
('Greenville Business Magazine', 'https://www.greenvillebusinessmag.com/', 'weekly', true, 'USA: Greenville'),

-- Louisiana (1.1% of clients)
('New Orleans Business', 'https://www.bizjournals.com/neworleans/news/industry/hotels', 'weekly', true, 'USA: New Orleans'),

-- North Carolina (1.1% of clients)
('Charlotte Business Journal - Hotels', 'https://www.bizjournals.com/charlotte/news/industry/hotels', 'weekly', true, 'USA: Charlotte'),
('Triangle Business Journal', 'https://www.bizjournals.com/triangle/news/industry/hotels', 'weekly', true, 'USA: Raleigh-Durham'),

-- Washington DC (1.2% of clients)
('Washington Business Journal - Hotels', 'https://www.bizjournals.com/washington/news/industry/hotels', 'weekly', true, 'USA: Washington DC'),

-- Virginia (1.0% of clients)
('Virginia Business', 'https://www.virginiabusiness.com/', 'weekly', true, 'USA: Virginia'),

-- Pennsylvania (1.1% of clients)
('Philadelphia Business Journal - Hotels', 'https://www.bizjournals.com/philadelphia/news/industry/hotels', 'weekly', true, 'USA: Philadelphia'),
('Pittsburgh Business Times', 'https://www.bizjournals.com/pittsburgh/news/industry/hotels', 'weekly', true, 'USA: Pittsburgh');

-- ============================================================================
-- TIER 5: US HOSPITALITY PUBLICATIONS
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('Travel + Leisure - Hotel Openings', 'https://www.travelandleisure.com/hotels-resorts/hotel-openings', 'daily', true, 'Publication - Filter for US/Caribbean'),
('Conde Nast Traveler - New Hotels', 'https://www.cntraveler.com/tags/hotel-openings', 'daily', true, 'Publication - Filter for US/Caribbean'),
('Hospitality Net', 'https://www.hospitalitynet.org/', 'daily', true, 'Industry publication'),
('Hotel News Resource', 'https://www.hotelnewsresource.com/', 'daily', true, 'Industry publication'),
('Hotel Management', 'https://www.hotelmanagement.net/', 'daily', true, 'Industry publication'),
('Hotel Business', 'https://www.hotelbusiness.com/', 'daily', true, 'Hotel industry news'),
('Lodging Magazine', 'https://lodgingmagazine.com/', 'weekly', true, 'Lodging industry'),
('Hotel News Now', 'https://hotelnewsnow.com/', 'daily', true, 'STR hotel news'),
('Skift', 'https://skift.com/', 'daily', true, 'Travel intelligence - Filter for US');

-- ============================================================================
-- TIER 6: US HOTEL CHAINS - AMERICAS FOCUS
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('Hilton Stories - Americas', 'https://stories.hilton.com/releases', 'weekly', true, 'Chain news - Filter for Americas'),
('Marriott News - Americas', 'https://news.marriott.com/', 'weekly', true, 'Chain news - Filter for Americas'),
('Hyatt Newsroom', 'https://newsroom.hyatt.com/', 'weekly', true, 'Chain news - Filter for Americas'),
('IHG Newsroom - Americas', 'https://www.ihgplc.com/en/news-and-media', 'weekly', true, 'Chain news - Filter for Americas'),
('Wyndham Newsroom', 'https://corporate.wyndham.com/newsroom', 'weekly', true, 'Chain news - US focused'),
('Choice Hotels News', 'https://news.choicehotels.com/', 'weekly', true, 'Chain news - US focused'),
('Best Western News', 'https://www.bestwestern.com/en_US/about/press-media.html', 'weekly', true, 'Chain news - US focused'),
('Drury Hotels Coming Soon', 'https://www.druryhotels.com/coming-soon', 'weekly', true, 'Regional chain - US only');

-- ============================================================================
-- TIER 7: HOTEL DEVELOPMENT / REAL ESTATE (US Focus)
-- ============================================================================
INSERT INTO sources (name, url, scrape_frequency, is_active, notes) VALUES
('Hotel News Resource - Development', 'https://www.hotelnewsresource.com/hotel_development.html', 'daily', true, 'Development pipeline - US'),
('Lodging Econometrics', 'https://lodgingeconometrics.com/', 'weekly', true, 'Construction pipeline - US'),
('Bisnow - Hotels', 'https://www.bisnow.com/tags/hotels', 'daily', true, 'Real estate news - US'),
('Commercial Observer - Hotels', 'https://commercialobserver.com/tag/hotels/', 'weekly', true, 'Real estate - US'),
('Globe St - Hotels', 'https://www.globest.com/hospitality/', 'weekly', true, 'Real estate news - US');

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Florida sources: 14
-- Caribbean sources: 14  
-- Other US states: 20
-- US Publications: 9
-- US Hotel Chains: 8
-- US Development: 5
-- Aggregators: 5
-- -----------------------
-- TOTAL: ~75 sources (all US + Caribbean focused)

-- ============================================================================
-- VERIFY
-- ============================================================================
-- SELECT COUNT(*) as total FROM sources;
-- SELECT scrape_frequency, COUNT(*) FROM sources GROUP BY scrape_frequency;