"""
SMART LEAD HUNTER — National Hospitality Zones Registry
========================================================
~140 zones across all 50 states + DC, focused on areas where
4-star+ hotels actually cluster. Sparse rural areas are excluded
or covered by a single broad zone.

Priority levels:
  - high:   run first; dense metros / resort destinations
  - medium: secondary metros and significant resort areas
  - low:    smaller metros, single-zone state coverage

Each zone uses bbox = (south_lat, west_lng, north_lat, east_lng).

Florida zones match the user's existing 8-zone production schema exactly
(do not modify without DB migration).
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass
class Zone:
    key: str
    name: str
    state: str  # 2-letter postal code
    bbox: Tuple[float, float, float, float]  # (s_lat, w_lng, n_lat, e_lng)
    priority: str  # "high" | "medium" | "low"
    description: str = ""


ZONES: Dict[str, Zone] = {
    # ════════════════════════════════════════════════════════════
    # FLORIDA (8 zones — matches existing production schema)
    # ════════════════════════════════════════════════════════════
    "south_florida": Zone(
        "south_florida",
        "South Florida",
        "FL",
        (25.5, -80.55, 26.95, -79.95),
        "high",
        "Miami-Dade, Broward, Palm Beach counties",
    ),
    "florida_keys": Zone(
        "florida_keys",
        "Florida Keys",
        "FL",
        (24.4, -82.1, 25.3, -80.05),
        "high",
        "Key West to Key Largo",
    ),
    "tampa_bay": Zone(
        "tampa_bay",
        "Tampa Bay",
        "FL",
        (27.0, -82.9, 28.2, -82.15),
        "high",
        "Tampa, St. Pete, Clearwater, Sarasota, Bradenton",
    ),
    "orlando": Zone(
        "orlando",
        "Orlando",
        "FL",
        (28.0, -81.9, 28.85, -80.9),
        "high",
        "Orange, Osceola, Seminole, Kissimmee, Lakeland",
    ),
    "southwest_fl": Zone(
        "southwest_fl",
        "Southwest FL",
        "FL",
        (25.8, -82.3, 27.0, -81.4),
        "high",
        "Naples, Fort Myers, Bonita Springs, Marco Island",
    ),
    "north_fl": Zone(
        "north_fl",
        "North FL",
        "FL",
        (29.0, -82.5, 30.7, -81.0),
        "medium",
        "Jacksonville, Amelia Island, St. Augustine, Gainesville, Ocala",
    ),
    "panhandle": Zone(
        "panhandle",
        "Panhandle",
        "FL",
        (29.6, -87.6, 30.8, -84.8),
        "medium",
        "Pensacola, Destin, Panama City Beach, 30A",
    ),
    "space_coast": Zone(
        "space_coast",
        "Space Coast",
        "FL",
        (26.9, -81.1, 28.65, -80.0),
        "medium",
        "Cocoa Beach, Melbourne, Vero Beach, Fort Pierce, Stuart",
    ),
    # ════════════════════════════════════════════════════════════
    # CALIFORNIA (10 zones — highest hotel density in US)
    # ════════════════════════════════════════════════════════════
    "ca_los_angeles": Zone(
        "ca_los_angeles",
        "Los Angeles Metro",
        "CA",
        (33.7, -118.7, 34.35, -117.6),
        "high",
        "LA, Beverly Hills, Santa Monica, Malibu, Pasadena, Long Beach",
    ),
    "ca_orange_county": Zone(
        "ca_orange_county",
        "Orange County",
        "CA",
        (33.4, -118.15, 33.95, -117.5),
        "high",
        "Anaheim, Newport Beach, Laguna, Dana Point, Huntington Beach",
    ),
    "ca_san_diego": Zone(
        "ca_san_diego",
        "San Diego",
        "CA",
        (32.5, -117.4, 33.25, -116.85),
        "high",
        "San Diego, La Jolla, Coronado, Carlsbad, Del Mar",
    ),
    "ca_sf_bay": Zone(
        "ca_sf_bay",
        "SF Bay Area",
        "CA",
        (37.2, -122.65, 38.05, -121.7),
        "high",
        "San Francisco, Oakland, San Jose, Palo Alto, Half Moon Bay",
    ),
    "ca_napa_sonoma": Zone(
        "ca_napa_sonoma",
        "Napa & Sonoma",
        "CA",
        (38.15, -123.0, 38.85, -122.2),
        "high",
        "Napa Valley, Sonoma, Healdsburg, Calistoga, Yountville",
    ),
    "ca_santa_barbara": Zone(
        "ca_santa_barbara",
        "Santa Barbara & Central Coast",
        "CA",
        (34.35, -120.7, 35.7, -119.5),
        "high",
        "Santa Barbara, Montecito, Ojai, San Luis Obispo, Paso Robles",
    ),
    "ca_palm_springs": Zone(
        "ca_palm_springs",
        "Palm Springs / Coachella Valley",
        "CA",
        (33.55, -116.8, 34.0, -116.0),
        "high",
        "Palm Springs, Rancho Mirage, La Quinta, Indian Wells",
    ),
    "ca_monterey": Zone(
        "ca_monterey",
        "Monterey & Carmel",
        "CA",
        (36.2, -122.0, 36.85, -121.55),
        "medium",
        "Monterey, Carmel, Pebble Beach, Big Sur, Pacific Grove",
    ),
    "ca_lake_tahoe": Zone(
        "ca_lake_tahoe",
        "Lake Tahoe (CA side)",
        "CA",
        (38.85, -120.3, 39.3, -119.9),
        "medium",
        "South Lake Tahoe, Truckee, Squaw Valley, Northstar",
    ),
    "ca_sacramento": Zone(
        "ca_sacramento",
        "Sacramento & Wine Country East",
        "CA",
        (38.3, -121.8, 38.85, -120.95),
        "low",
        "Sacramento, Roseville, Folsom",
    ),
    # ════════════════════════════════════════════════════════════
    # NEW YORK (6 zones)
    # ════════════════════════════════════════════════════════════
    "ny_nyc": Zone(
        "ny_nyc",
        "New York City",
        "NY",
        (40.55, -74.25, 40.92, -73.7),
        "high",
        "Manhattan, Brooklyn, Queens, Bronx, Staten Island",
    ),
    "ny_long_island": Zone(
        "ny_long_island",
        "Long Island & Hamptons",
        "NY",
        (40.55, -73.75, 41.2, -71.85),
        "high",
        "Hamptons, Montauk, North Fork, Sag Harbor",
    ),
    "ny_hudson_valley": Zone(
        "ny_hudson_valley",
        "Hudson Valley",
        "NY",
        (41.0, -74.4, 42.4, -73.5),
        "medium",
        "Tarrytown, Rhinebeck, Hudson, Beacon, Cold Spring",
    ),
    "ny_catskills": Zone(
        "ny_catskills",
        "Catskills",
        "NY",
        (41.7, -75.2, 42.5, -73.8),
        "medium",
        "Woodstock, Phoenicia, Livingston Manor, Hunter, Windham",
    ),
    "ny_adirondacks": Zone(
        "ny_adirondacks",
        "Adirondacks",
        "NY",
        (43.5, -75.0, 44.6, -73.3),
        "low",
        "Lake Placid, Saranac Lake, Lake George",
    ),
    "ny_finger_lakes_niagara": Zone(
        "ny_finger_lakes_niagara",
        "Finger Lakes & Niagara",
        "NY",
        (42.4, -79.2, 43.4, -76.4),
        "low",
        "Niagara Falls, Buffalo, Rochester, Skaneateles, Geneva",
    ),
    # ════════════════════════════════════════════════════════════
    # TEXAS (6 zones)
    # ════════════════════════════════════════════════════════════
    "tx_dfw": Zone(
        "tx_dfw",
        "Dallas / Fort Worth",
        "TX",
        (32.55, -97.55, 33.15, -96.5),
        "high",
        "Dallas, Fort Worth, Plano, Frisco, Irving, Arlington",
    ),
    "tx_houston": Zone(
        "tx_houston",
        "Houston Metro",
        "TX",
        (29.4, -95.85, 30.15, -95.0),
        "high",
        "Houston, The Woodlands, Sugar Land, Galveston",
    ),
    "tx_austin": Zone(
        "tx_austin",
        "Austin",
        "TX",
        (30.05, -98.0, 30.6, -97.5),
        "high",
        "Austin, Round Rock, Lake Travis",
    ),
    "tx_san_antonio": Zone(
        "tx_san_antonio",
        "San Antonio",
        "TX",
        (29.2, -98.85, 29.75, -98.25),
        "medium",
        "San Antonio, Boerne, New Braunfels",
    ),
    "tx_hill_country": Zone(
        "tx_hill_country",
        "Texas Hill Country",
        "TX",
        (29.75, -99.5, 30.6, -98.0),
        "medium",
        "Fredericksburg, Marble Falls, Wimberley, Kerrville",
    ),
    "tx_el_paso": Zone(
        "tx_el_paso", "El Paso", "TX", (31.55, -106.7, 32.0, -106.15), "low", "El Paso"
    ),
    # ════════════════════════════════════════════════════════════
    # NEVADA (2 zones)
    # ════════════════════════════════════════════════════════════
    "nv_las_vegas": Zone(
        "nv_las_vegas",
        "Las Vegas Metro",
        "NV",
        (35.95, -115.45, 36.35, -114.9),
        "high",
        "Las Vegas Strip, Henderson, Summerlin, Boulder City",
    ),
    "nv_reno_tahoe": Zone(
        "nv_reno_tahoe",
        "Reno & Lake Tahoe (NV side)",
        "NV",
        (39.0, -120.05, 39.7, -119.5),
        "medium",
        "Reno, Sparks, Incline Village, Carson City",
    ),
    # ════════════════════════════════════════════════════════════
    # HAWAII (4 zones — one per major island)
    # ════════════════════════════════════════════════════════════
    "hi_oahu": Zone(
        "hi_oahu",
        "Oahu",
        "HI",
        (21.2, -158.3, 21.75, -157.6),
        "high",
        "Honolulu, Waikiki, Ko Olina, North Shore, Kailua",
    ),
    "hi_maui": Zone(
        "hi_maui",
        "Maui",
        "HI",
        (20.55, -156.7, 21.05, -155.95),
        "high",
        "Wailea, Kaanapali, Lahaina, Kapalua, Hana",
    ),
    "hi_big_island": Zone(
        "hi_big_island",
        "Big Island (Hawaii)",
        "HI",
        (18.9, -156.1, 20.3, -154.8),
        "high",
        "Kona, Kohala Coast, Hilo, Waikoloa",
    ),
    "hi_kauai": Zone(
        "hi_kauai",
        "Kauai",
        "HI",
        (21.85, -159.8, 22.25, -159.25),
        "high",
        "Poipu, Princeville, Lihue, Hanalei",
    ),
    # ════════════════════════════════════════════════════════════
    # ILLINOIS (2 zones)
    # ════════════════════════════════════════════════════════════
    "il_chicago": Zone(
        "il_chicago",
        "Chicago Metro",
        "IL",
        (41.6, -88.2, 42.15, -87.5),
        "high",
        "Chicago, Oak Brook, Schaumburg, Evanston, Naperville",
    ),
    "il_rest": Zone(
        "il_rest",
        "Illinois (rest)",
        "IL",
        (38.6, -90.7, 40.5, -88.0),
        "low",
        "Springfield, Peoria, Champaign",
    ),
    # ════════════════════════════════════════════════════════════
    # MASSACHUSETTS (3 zones)
    # ════════════════════════════════════════════════════════════
    "ma_boston": Zone(
        "ma_boston",
        "Boston Metro",
        "MA",
        (42.2, -71.35, 42.55, -70.85),
        "high",
        "Boston, Cambridge, Newton, Quincy, North Shore",
    ),
    "ma_cape_islands": Zone(
        "ma_cape_islands",
        "Cape Cod & Islands",
        "MA",
        (41.2, -70.85, 42.1, -69.9),
        "high",
        "Cape Cod, Martha's Vineyard, Nantucket",
    ),
    "ma_berkshires": Zone(
        "ma_berkshires",
        "Berkshires",
        "MA",
        (42.0, -73.55, 42.75, -72.95),
        "medium",
        "Lenox, Stockbridge, Great Barrington, Williamstown",
    ),
    # ════════════════════════════════════════════════════════════
    # GEORGIA (3 zones)
    # ════════════════════════════════════════════════════════════
    "ga_atlanta": Zone(
        "ga_atlanta",
        "Atlanta Metro",
        "GA",
        (33.55, -84.7, 34.1, -84.0),
        "high",
        "Atlanta, Buckhead, Midtown, Sandy Springs, Alpharetta",
    ),
    "ga_savannah": Zone(
        "ga_savannah",
        "Savannah & Coastal",
        "GA",
        (31.85, -81.4, 32.25, -80.85),
        "medium",
        "Savannah, Tybee Island",
    ),
    "ga_golden_isles": Zone(
        "ga_golden_isles",
        "Golden Isles",
        "GA",
        (30.85, -81.6, 31.4, -81.25),
        "medium",
        "Sea Island, St. Simons, Jekyll Island, Cumberland Island",
    ),
    # ════════════════════════════════════════════════════════════
    # COLORADO (4 zones)
    # ════════════════════════════════════════════════════════════
    "co_denver": Zone(
        "co_denver",
        "Denver Metro",
        "CO",
        (39.55, -105.3, 40.0, -104.65),
        "high",
        "Denver, Boulder, Cherry Creek, DTC",
    ),
    "co_aspen_vail": Zone(
        "co_aspen_vail",
        "Aspen / Vail / Beaver Creek",
        "CO",
        (39.1, -107.0, 39.85, -106.0),
        "high",
        "Aspen, Snowmass, Vail, Beaver Creek, Avon",
    ),
    "co_telluride_durango": Zone(
        "co_telluride_durango",
        "Telluride & Southwest",
        "CO",
        (37.15, -108.0, 38.05, -107.55),
        "medium",
        "Telluride, Mountain Village, Durango",
    ),
    "co_springs": Zone(
        "co_springs",
        "Colorado Springs & Front Range South",
        "CO",
        (38.6, -105.0, 39.05, -104.6),
        "low",
        "Colorado Springs, Manitou Springs, Broadmoor",
    ),
    # ════════════════════════════════════════════════════════════
    # ARIZONA (4 zones)
    # ════════════════════════════════════════════════════════════
    "az_phoenix_scottsdale": Zone(
        "az_phoenix_scottsdale",
        "Phoenix / Scottsdale",
        "AZ",
        (33.25, -112.4, 33.85, -111.6),
        "high",
        "Phoenix, Scottsdale, Paradise Valley, Tempe, Mesa",
    ),
    "az_tucson": Zone(
        "az_tucson",
        "Tucson",
        "AZ",
        (32.0, -111.15, 32.5, -110.7),
        "medium",
        "Tucson, Oro Valley, Catalina Foothills",
    ),
    "az_sedona": Zone(
        "az_sedona",
        "Sedona & Verde Valley",
        "AZ",
        (34.65, -112.0, 35.05, -111.65),
        "high",
        "Sedona, Oak Creek, Cottonwood",
    ),
    "az_grand_canyon": Zone(
        "az_grand_canyon",
        "Grand Canyon & Northern AZ",
        "AZ",
        (35.0, -112.3, 36.3, -111.4),
        "medium",
        "Grand Canyon, Flagstaff, Williams",
    ),
    # ════════════════════════════════════════════════════════════
    # WASHINGTON (2 zones)
    # ════════════════════════════════════════════════════════════
    "wa_seattle": Zone(
        "wa_seattle",
        "Seattle Metro",
        "WA",
        (47.35, -122.5, 47.85, -121.95),
        "high",
        "Seattle, Bellevue, Kirkland, Redmond, Tacoma",
    ),
    "wa_rest": Zone(
        "wa_rest",
        "Washington (rest)",
        "WA",
        (46.0, -120.5, 48.5, -117.0),
        "low",
        "Spokane, Walla Walla, Olympic Peninsula",
    ),
    # ════════════════════════════════════════════════════════════
    # PENNSYLVANIA (3 zones)
    # ════════════════════════════════════════════════════════════
    "pa_philadelphia": Zone(
        "pa_philadelphia",
        "Philadelphia Metro",
        "PA",
        (39.85, -75.4, 40.2, -74.95),
        "high",
        "Philadelphia, Center City, King of Prussia, Main Line",
    ),
    "pa_pittsburgh": Zone(
        "pa_pittsburgh",
        "Pittsburgh Metro",
        "PA",
        (40.3, -80.15, 40.65, -79.75),
        "medium",
        "Pittsburgh, Cranberry, Monroeville",
    ),
    "pa_poconos": Zone(
        "pa_poconos",
        "Poconos & Lehigh Valley",
        "PA",
        (40.5, -75.7, 41.4, -74.85),
        "low",
        "Poconos, Bethlehem, Allentown",
    ),
    # ════════════════════════════════════════════════════════════
    # NEW JERSEY (3 zones)
    # ════════════════════════════════════════════════════════════
    "nj_north": Zone(
        "nj_north",
        "Northern NJ",
        "NJ",
        (40.5, -74.6, 41.1, -73.9),
        "high",
        "Jersey City, Newark, Hoboken, Paramus, Morristown",
    ),
    "nj_atlantic_city": Zone(
        "nj_atlantic_city",
        "Atlantic City & South Shore",
        "NJ",
        (39.0, -74.95, 39.55, -74.3),
        "medium",
        "Atlantic City, Cape May, Ocean City",
    ),
    "nj_jersey_shore": Zone(
        "nj_jersey_shore",
        "Jersey Shore (north)",
        "NJ",
        (39.85, -74.35, 40.5, -73.95),
        "medium",
        "Asbury Park, Long Branch, Spring Lake, Red Bank",
    ),
    # ════════════════════════════════════════════════════════════
    # NORTH CAROLINA (4 zones)
    # ════════════════════════════════════════════════════════════
    "nc_charlotte": Zone(
        "nc_charlotte",
        "Charlotte Metro",
        "NC",
        (35.0, -81.0, 35.45, -80.55),
        "high",
        "Charlotte, Uptown, SouthPark, Ballantyne",
    ),
    "nc_raleigh_durham": Zone(
        "nc_raleigh_durham",
        "Raleigh / Durham / Triangle",
        "NC",
        (35.7, -79.05, 36.15, -78.4),
        "high",
        "Raleigh, Durham, Chapel Hill, Cary, RTP",
    ),
    "nc_asheville": Zone(
        "nc_asheville",
        "Asheville & Smokies",
        "NC",
        (35.35, -82.85, 35.85, -82.3),
        "high",
        "Asheville, Biltmore, Black Mountain, Hendersonville",
    ),
    "nc_outer_banks": Zone(
        "nc_outer_banks",
        "Outer Banks & Coast",
        "NC",
        (35.05, -76.05, 36.6, -75.4),
        "medium",
        "Nags Head, Duck, Corolla, Hatteras, Wilmington",
    ),
    # ════════════════════════════════════════════════════════════
    # TENNESSEE (3 zones)
    # ════════════════════════════════════════════════════════════
    "tn_nashville": Zone(
        "tn_nashville",
        "Nashville Metro",
        "TN",
        (35.95, -87.05, 36.4, -86.5),
        "high",
        "Nashville, Franklin, Brentwood",
    ),
    "tn_memphis": Zone(
        "tn_memphis",
        "Memphis",
        "TN",
        (34.95, -90.2, 35.3, -89.7),
        "medium",
        "Memphis, Germantown, Collierville",
    ),
    "tn_smokies": Zone(
        "tn_smokies",
        "Knoxville & Smokies",
        "TN",
        (35.55, -84.1, 36.05, -83.3),
        "medium",
        "Gatlinburg, Pigeon Forge, Knoxville, Sevierville",
    ),
    # ════════════════════════════════════════════════════════════
    # VIRGINIA (4 zones)
    # ════════════════════════════════════════════════════════════
    "va_nova": Zone(
        "va_nova",
        "Northern Virginia / DC Metro West",
        "VA",
        (38.65, -77.55, 39.1, -77.0),
        "high",
        "Arlington, Alexandria, Tysons, Reston, Fairfax",
    ),
    "va_virginia_beach": Zone(
        "va_virginia_beach",
        "Virginia Beach & Hampton Roads",
        "VA",
        (36.7, -76.55, 37.1, -75.95),
        "medium",
        "Virginia Beach, Norfolk, Williamsburg",
    ),
    "va_richmond": Zone(
        "va_richmond", "Richmond", "VA", (37.4, -77.7, 37.75, -77.25), "low", "Richmond"
    ),
    "va_charlottesville": Zone(
        "va_charlottesville",
        "Charlottesville & Shenandoah",
        "VA",
        (37.95, -78.65, 38.4, -78.25),
        "low",
        "Charlottesville, Wine Country",
    ),
    # ════════════════════════════════════════════════════════════
    # WASHINGTON DC (1 zone)
    # ════════════════════════════════════════════════════════════
    "dc_washington": Zone(
        "dc_washington",
        "Washington DC",
        "DC",
        (38.79, -77.12, 38.99, -76.9),
        "high",
        "Downtown DC, Georgetown, Dupont, Capitol Hill",
    ),
    # ════════════════════════════════════════════════════════════
    # MARYLAND (2 zones)
    # ════════════════════════════════════════════════════════════
    "md_baltimore": Zone(
        "md_baltimore",
        "Baltimore Metro & DC Suburbs",
        "MD",
        (38.85, -77.25, 39.45, -76.45),
        "medium",
        "Baltimore, Bethesda, Annapolis, Silver Spring, National Harbor",
    ),
    "md_eastern_shore": Zone(
        "md_eastern_shore",
        "Eastern Shore & Ocean City",
        "MD",
        (38.0, -75.85, 38.85, -75.05),
        "low",
        "Ocean City, St. Michaels, Easton",
    ),
    # ════════════════════════════════════════════════════════════
    # SOUTH CAROLINA (3 zones)
    # ════════════════════════════════════════════════════════════
    "sc_charleston": Zone(
        "sc_charleston",
        "Charleston",
        "SC",
        (32.65, -80.15, 33.0, -79.75),
        "high",
        "Charleston, Mt. Pleasant, Kiawah, Isle of Palms",
    ),
    "sc_hilton_head": Zone(
        "sc_hilton_head",
        "Hilton Head & Bluffton",
        "SC",
        (32.1, -81.0, 32.4, -80.6),
        "high",
        "Hilton Head Island, Bluffton, Daufuskie",
    ),
    "sc_myrtle_beach": Zone(
        "sc_myrtle_beach",
        "Myrtle Beach & Grand Strand",
        "SC",
        (33.4, -79.2, 33.95, -78.6),
        "medium",
        "Myrtle Beach, North Myrtle, Pawleys Island",
    ),
    # ════════════════════════════════════════════════════════════
    # LOUISIANA (2 zones)
    # ════════════════════════════════════════════════════════════
    "la_new_orleans": Zone(
        "la_new_orleans",
        "New Orleans Metro",
        "LA",
        (29.85, -90.25, 30.15, -89.85),
        "high",
        "French Quarter, CBD, Garden District, Metairie",
    ),
    "la_baton_rouge": Zone(
        "la_baton_rouge",
        "Baton Rouge & Lafayette",
        "LA",
        (30.15, -91.35, 30.6, -90.95),
        "low",
        "Baton Rouge",
    ),
    # ════════════════════════════════════════════════════════════
    # MICHIGAN (3 zones)
    # ════════════════════════════════════════════════════════════
    "mi_detroit": Zone(
        "mi_detroit",
        "Detroit Metro",
        "MI",
        (42.2, -83.4, 42.7, -82.85),
        "medium",
        "Detroit, Troy, Dearborn, Birmingham, Auburn Hills",
    ),
    "mi_grand_rapids": Zone(
        "mi_grand_rapids",
        "Grand Rapids & West MI",
        "MI",
        (42.85, -85.85, 43.1, -85.4),
        "low",
        "Grand Rapids, Holland",
    ),
    "mi_traverse_mackinac": Zone(
        "mi_traverse_mackinac",
        "Northern MI Resorts",
        "MI",
        (44.6, -86.0, 45.95, -84.4),
        "medium",
        "Traverse City, Mackinac Island, Petoskey",
    ),
    # ════════════════════════════════════════════════════════════
    # OHIO (3 zones)
    # ════════════════════════════════════════════════════════════
    "oh_cleveland": Zone(
        "oh_cleveland",
        "Cleveland",
        "OH",
        (41.35, -81.85, 41.65, -81.4),
        "low",
        "Cleveland, Beachwood, Westlake",
    ),
    "oh_columbus": Zone(
        "oh_columbus",
        "Columbus",
        "OH",
        (39.85, -83.2, 40.15, -82.75),
        "medium",
        "Columbus, Dublin, New Albany",
    ),
    "oh_cincinnati": Zone(
        "oh_cincinnati",
        "Cincinnati",
        "OH",
        (39.05, -84.7, 39.35, -84.3),
        "low",
        "Cincinnati, Mason, Blue Ash",
    ),
    # ════════════════════════════════════════════════════════════
    # MISSOURI (3 zones)
    # ════════════════════════════════════════════════════════════
    "mo_st_louis": Zone(
        "mo_st_louis",
        "St. Louis",
        "MO",
        (38.55, -90.55, 38.85, -90.15),
        "low",
        "St. Louis, Clayton, Chesterfield",
    ),
    "mo_kansas_city": Zone(
        "mo_kansas_city",
        "Kansas City",
        "MO",
        (38.9, -94.7, 39.25, -94.4),
        "low",
        "Kansas City, Country Club Plaza, Overland Park",
    ),
    "mo_branson": Zone(
        "mo_branson",
        "Branson & Lake of the Ozarks",
        "MO",
        (36.55, -93.4, 38.4, -92.5),
        "low",
        "Branson, Lake of the Ozarks",
    ),
    # ════════════════════════════════════════════════════════════
    # UTAH (3 zones)
    # ════════════════════════════════════════════════════════════
    "ut_salt_lake": Zone(
        "ut_salt_lake",
        "Salt Lake City",
        "UT",
        (40.55, -112.1, 40.95, -111.7),
        "medium",
        "Salt Lake City, Sandy, Cottonwood Canyons",
    ),
    "ut_park_city_deer_valley": Zone(
        "ut_park_city_deer_valley",
        "Park City & Deer Valley",
        "UT",
        (40.55, -111.65, 40.85, -111.4),
        "high",
        "Park City, Deer Valley, Canyons",
    ),
    "ut_southern": Zone(
        "ut_southern",
        "Southern Utah / Zion / Moab",
        "UT",
        (37.0, -114.05, 38.7, -109.4),
        "medium",
        "St. George, Zion, Bryce, Moab",
    ),
    # ════════════════════════════════════════════════════════════
    # OREGON (3 zones)
    # ════════════════════════════════════════════════════════════
    "or_portland": Zone(
        "or_portland",
        "Portland Metro",
        "OR",
        (45.35, -123.0, 45.7, -122.4),
        "medium",
        "Portland, Lake Oswego, Hillsboro",
    ),
    "or_bend": Zone(
        "or_bend",
        "Bend & Central OR",
        "OR",
        (43.85, -121.55, 44.2, -121.1),
        "medium",
        "Bend, Sunriver, Sisters",
    ),
    "or_coast": Zone(
        "or_coast",
        "Oregon Coast",
        "OR",
        (43.3, -124.4, 46.0, -123.7),
        "low",
        "Cannon Beach, Newport, Lincoln City",
    ),
    # ════════════════════════════════════════════════════════════
    # WISCONSIN (2 zones)
    # ════════════════════════════════════════════════════════════
    "wi_milwaukee_madison": Zone(
        "wi_milwaukee_madison",
        "Milwaukee & Madison",
        "WI",
        (42.95, -89.6, 43.25, -87.85),
        "low",
        "Milwaukee, Madison",
    ),
    "wi_door_county": Zone(
        "wi_door_county",
        "Door County & Lake Geneva",
        "WI",
        (42.55, -88.5, 45.3, -87.0),
        "low",
        "Door County, Lake Geneva, Kohler",
    ),
    # ════════════════════════════════════════════════════════════
    # MINNESOTA (1 zone)
    # ════════════════════════════════════════════════════════════
    "mn_twin_cities": Zone(
        "mn_twin_cities",
        "Twin Cities",
        "MN",
        (44.8, -93.55, 45.15, -92.95),
        "medium",
        "Minneapolis, St. Paul, Bloomington",
    ),
    # ════════════════════════════════════════════════════════════
    # INDIANA (1 zone)
    # ════════════════════════════════════════════════════════════
    "in_indianapolis": Zone(
        "in_indianapolis",
        "Indianapolis",
        "IN",
        (39.6, -86.35, 39.95, -85.95),
        "low",
        "Indianapolis, Carmel, Fishers",
    ),
    # ════════════════════════════════════════════════════════════
    # KENTUCKY (2 zones)
    # ════════════════════════════════════════════════════════════
    "ky_louisville": Zone(
        "ky_louisville",
        "Louisville",
        "KY",
        (38.1, -85.85, 38.35, -85.5),
        "low",
        "Louisville, downtown",
    ),
    "ky_lexington": Zone(
        "ky_lexington",
        "Lexington & Bourbon Country",
        "KY",
        (37.95, -84.65, 38.15, -84.35),
        "low",
        "Lexington, Horse Country",
    ),
    # ════════════════════════════════════════════════════════════
    # ALABAMA (2 zones)
    # ════════════════════════════════════════════════════════════
    "al_birmingham": Zone(
        "al_birmingham",
        "Birmingham & Huntsville",
        "AL",
        (33.35, -87.0, 34.85, -86.5),
        "low",
        "Birmingham, Huntsville",
    ),
    "al_gulf_shores": Zone(
        "al_gulf_shores",
        "Gulf Shores & Mobile",
        "AL",
        (30.2, -88.15, 30.75, -87.55),
        "low",
        "Gulf Shores, Orange Beach, Mobile, Point Clear",
    ),
    # ════════════════════════════════════════════════════════════
    # MISSISSIPPI (1 zone)
    # ════════════════════════════════════════════════════════════
    "ms_gulf_coast": Zone(
        "ms_gulf_coast",
        "Mississippi Gulf Coast",
        "MS",
        (30.3, -89.4, 30.5, -88.4),
        "low",
        "Biloxi, Gulfport, Bay St. Louis",
    ),
    # ════════════════════════════════════════════════════════════
    # ARKANSAS (1 zone)
    # ════════════════════════════════════════════════════════════
    "ar_main": Zone(
        "ar_main",
        "Little Rock & Bentonville",
        "AR",
        (34.65, -94.4, 36.45, -92.15),
        "low",
        "Little Rock, Bentonville, Hot Springs",
    ),
    # ════════════════════════════════════════════════════════════
    # OKLAHOMA (1 zone)
    # ════════════════════════════════════════════════════════════
    "ok_main": Zone(
        "ok_main",
        "Oklahoma City & Tulsa",
        "OK",
        (35.35, -97.7, 36.25, -95.75),
        "low",
        "Oklahoma City, Tulsa",
    ),
    # ════════════════════════════════════════════════════════════
    # KANSAS (1 zone)
    # ════════════════════════════════════════════════════════════
    "ks_main": Zone(
        "ks_main",
        "Kansas (Wichita & KC suburbs)",
        "KS",
        (37.55, -97.55, 39.1, -94.55),
        "low",
        "Wichita, Overland Park, Olathe",
    ),
    # ════════════════════════════════════════════════════════════
    # NEBRASKA (1 zone)
    # ════════════════════════════════════════════════════════════
    "ne_main": Zone(
        "ne_main",
        "Omaha & Lincoln",
        "NE",
        (40.7, -96.85, 41.4, -95.85),
        "low",
        "Omaha, Lincoln",
    ),
    # ════════════════════════════════════════════════════════════
    # IOWA (1 zone)
    # ════════════════════════════════════════════════════════════
    "ia_main": Zone(
        "ia_main",
        "Des Moines & Iowa City",
        "IA",
        (41.55, -93.85, 41.75, -91.35),
        "low",
        "Des Moines, Iowa City, Cedar Rapids",
    ),
    # ════════════════════════════════════════════════════════════
    # NORTH DAKOTA / SOUTH DAKOTA (combined low-priority)
    # ════════════════════════════════════════════════════════════
    "nd_main": Zone(
        "nd_main",
        "North Dakota",
        "ND",
        (46.7, -101.0, 48.0, -96.7),
        "low",
        "Fargo, Bismarck",
    ),
    "sd_main": Zone(
        "sd_main",
        "South Dakota & Black Hills",
        "SD",
        (43.5, -103.85, 44.4, -96.6),
        "low",
        "Sioux Falls, Rapid City, Deadwood",
    ),
    # ════════════════════════════════════════════════════════════
    # MONTANA (2 zones)
    # ════════════════════════════════════════════════════════════
    "mt_glacier_whitefish": Zone(
        "mt_glacier_whitefish",
        "Glacier & Whitefish",
        "MT",
        (48.0, -114.6, 48.95, -113.3),
        "medium",
        "Whitefish, Kalispell, Glacier National Park",
    ),
    "mt_yellowstone_bozeman": Zone(
        "mt_yellowstone_bozeman",
        "Bozeman & Yellowstone gateway",
        "MT",
        (44.85, -111.55, 45.95, -110.6),
        "medium",
        "Bozeman, Big Sky, West Yellowstone",
    ),
    # ════════════════════════════════════════════════════════════
    # WYOMING (2 zones)
    # ════════════════════════════════════════════════════════════
    "wy_jackson_hole": Zone(
        "wy_jackson_hole",
        "Jackson Hole & Tetons",
        "WY",
        (43.4, -111.05, 44.15, -110.4),
        "high",
        "Jackson, Teton Village, Wilson",
    ),
    "wy_yellowstone": Zone(
        "wy_yellowstone",
        "Yellowstone & Cody",
        "WY",
        (44.0, -110.95, 45.0, -108.9),
        "low",
        "Yellowstone, Cody",
    ),
    # ════════════════════════════════════════════════════════════
    # IDAHO (2 zones)
    # ════════════════════════════════════════════════════════════
    "id_sun_valley": Zone(
        "id_sun_valley",
        "Sun Valley & Coeur d'Alene",
        "ID",
        (43.45, -116.8, 47.85, -114.15),
        "medium",
        "Sun Valley, Ketchum, Coeur d'Alene, Boise",
    ),
    "id_main": Zone(
        "id_main",
        "Boise",
        "ID",
        (43.5, -116.4, 43.75, -116.05),
        "low",
        "Boise downtown",
    ),
    # ════════════════════════════════════════════════════════════
    # NEW MEXICO (2 zones)
    # ════════════════════════════════════════════════════════════
    "nm_santa_fe_taos": Zone(
        "nm_santa_fe_taos",
        "Santa Fe & Taos",
        "NM",
        (35.5, -106.1, 36.55, -105.4),
        "medium",
        "Santa Fe, Taos, Tesuque",
    ),
    "nm_albuquerque": Zone(
        "nm_albuquerque",
        "Albuquerque",
        "NM",
        (34.95, -106.75, 35.25, -106.45),
        "low",
        "Albuquerque",
    ),
    # ════════════════════════════════════════════════════════════
    # ALASKA (1 zone)
    # ════════════════════════════════════════════════════════════
    "ak_main": Zone(
        "ak_main",
        "Alaska (Anchorage / Denali / SE)",
        "AK",
        (55.0, -151.0, 65.0, -130.0),
        "low",
        "Anchorage, Denali, Juneau, Ketchikan",
    ),
    # ════════════════════════════════════════════════════════════
    # CONNECTICUT (1 zone)
    # ════════════════════════════════════════════════════════════
    "ct_main": Zone(
        "ct_main",
        "Connecticut (Greenwich to Mystic)",
        "CT",
        (41.0, -73.75, 41.85, -71.85),
        "medium",
        "Greenwich, Stamford, New Haven, Mystic, Hartford",
    ),
    # ════════════════════════════════════════════════════════════
    # RHODE ISLAND (1 zone)
    # ════════════════════════════════════════════════════════════
    "ri_main": Zone(
        "ri_main",
        "Rhode Island (Newport & Providence)",
        "RI",
        (41.35, -71.55, 41.95, -71.2),
        "medium",
        "Newport, Providence, Watch Hill",
    ),
    # ════════════════════════════════════════════════════════════
    # NEW HAMPSHIRE (1 zone)
    # ════════════════════════════════════════════════════════════
    "nh_main": Zone(
        "nh_main",
        "New Hampshire (White Mtns & Lakes)",
        "NH",
        (43.4, -71.95, 44.55, -71.0),
        "low",
        "North Conway, Bretton Woods, Lake Winnipesaukee",
    ),
    # ════════════════════════════════════════════════════════════
    # VERMONT (1 zone)
    # ════════════════════════════════════════════════════════════
    "vt_main": Zone(
        "vt_main",
        "Vermont (Stowe / Woodstock)",
        "VT",
        (43.55, -73.0, 44.7, -72.4),
        "medium",
        "Stowe, Woodstock, Manchester, Burlington",
    ),
    # ════════════════════════════════════════════════════════════
    # MAINE (1 zone)
    # ════════════════════════════════════════════════════════════
    "me_main": Zone(
        "me_main",
        "Maine (Coast & Bar Harbor)",
        "ME",
        (43.3, -70.85, 44.55, -67.95),
        "medium",
        "Portland, Kennebunkport, Bar Harbor, Camden",
    ),
    # ════════════════════════════════════════════════════════════
    # DELAWARE (1 zone)
    # ════════════════════════════════════════════════════════════
    "de_main": Zone(
        "de_main",
        "Delaware Beaches",
        "DE",
        (38.55, -75.25, 38.85, -75.0),
        "low",
        "Rehoboth Beach, Bethany Beach, Lewes",
    ),
    # ════════════════════════════════════════════════════════════
    # WEST VIRGINIA (1 zone)
    # ════════════════════════════════════════════════════════════
    "wv_main": Zone(
        "wv_main",
        "West Virginia (Greenbrier & Snowshoe)",
        "WV",
        (37.75, -80.6, 38.55, -79.85),
        "low",
        "Greenbrier, Snowshoe, White Sulphur Springs",
    ),
    # ════════════════════════════════════════════════════════════════
    # CARIBBEAN MARKET (21 territories — secondary market per scorer)
    # ════════════════════════════════════════════════════════════════
    # These use ISO 3166-1 alpha-2 country codes in the `state` field,
    # which slots in alongside US state codes (both are 2 chars).
    # zones_by_state() works on these without modification.
    #
    # PASTE this entire block into ZONES dict in app/services/zones_registry.py
    # right before the closing `}` of the dict.
    # ════════════════════════════════════════════════════════════
    # CARIBBEAN — secondary market
    # ════════════════════════════════════════════════════════════
    "bs_bahamas": Zone(
        "bs_bahamas",
        "Bahamas",
        "BS",
        (20.9, -80.5, 27.3, -72.7),
        "high",
        "Nassau, Paradise Island, Exuma, Eleuthera, Harbour Island",
    ),
    "jm_jamaica": Zone(
        "jm_jamaica",
        "Jamaica",
        "JM",
        (17.7, -78.4, 18.6, -76.1),
        "high",
        "Montego Bay, Ocho Rios, Negril, Kingston",
    ),
    "do_dominican_republic": Zone(
        "do_dominican_republic",
        "Dominican Republic",
        "DO",
        (17.4, -72.05, 19.95, -68.3),
        "high",
        "Punta Cana, Cap Cana, La Romana, Santo Domingo, Casa de Campo",
    ),
    "pr_puerto_rico": Zone(
        "pr_puerto_rico",
        "Puerto Rico",
        "PR",
        (17.85, -67.35, 18.55, -65.2),
        "high",
        "San Juan, Dorado, Vieques, Culebra",
    ),
    "ky_cayman_islands": Zone(
        "ky_cayman_islands",
        "Cayman Islands",
        "CY",
        (19.2, -81.45, 19.78, -79.7),
        "high",
        "Grand Cayman, Seven Mile Beach, Cayman Brac, Little Cayman",
    ),
    "tc_turks_caicos": Zone(
        "tc_turks_caicos",
        "Turks and Caicos",
        "TC",
        (21.2, -72.55, 21.95, -71.1),
        "high",
        "Providenciales, Grace Bay, Parrot Cay, Ambergris Cay",
    ),
    "bm_bermuda": Zone(
        "bm_bermuda",
        "Bermuda",
        "BM",
        (32.24, -64.9, 32.4, -64.65),
        "high",
        "Hamilton, Tucker's Town, Southampton, St. George's",
    ),
    "vi_us_virgin_islands": Zone(
        "vi_us_virgin_islands",
        "US Virgin Islands",
        "VI",
        (17.65, -65.1, 18.45, -64.55),
        "medium",
        "St. Thomas, St. John, St. Croix",
    ),
    "vg_british_virgin_islands": Zone(
        "vg_british_virgin_islands",
        "British Virgin Islands",
        "VG",
        (18.3, -64.8, 18.55, -64.25),
        "medium",
        "Tortola, Virgin Gorda, Necker Island, Peter Island, Oil Nut Bay",
    ),
    "bb_barbados": Zone(
        "bb_barbados",
        "Barbados",
        "BB",
        (13.0, -59.75, 13.4, -59.35),
        "high",
        "Bridgetown, Sandy Lane, Holetown, Christ Church, St. James",
    ),
    "aw_aruba": Zone(
        "aw_aruba",
        "Aruba",
        "AW",
        (12.4, -70.1, 12.7, -69.85),
        "medium",
        "Oranjestad, Palm Beach, Eagle Beach",
    ),
    "cw_curacao": Zone(
        "cw_curacao",
        "Curaçao",
        "CW",
        (12.0, -69.2, 12.4, -68.7),
        "low",
        "Willemstad, Westpunt",
    ),
    "lc_saint_lucia": Zone(
        "lc_saint_lucia",
        "Saint Lucia",
        "LC",
        (13.7, -61.1, 14.15, -60.85),
        "high",
        "Castries, Sugar Beach, Marigot Bay, Soufrière",
    ),
    "ag_antigua_barbuda": Zone(
        "ag_antigua_barbuda",
        "Antigua and Barbuda",
        "AG",
        (16.95, -62.0, 17.75, -61.65),
        "high",
        "St. John's, Jumby Bay, Hermitage Bay, Curtain Bluff, Carlisle Bay",
    ),
    "ai_anguilla": Zone(
        "ai_anguilla",
        "Anguilla",
        "AI",
        (18.15, -63.2, 18.3, -62.95),
        "high",
        "The Valley, Cap Juluca, Meads Bay, Shoal Bay",
    ),
    "kn_st_kitts_nevis": Zone(
        "kn_st_kitts_nevis",
        "St. Kitts and Nevis",
        "KN",
        (17.05, -62.9, 17.45, -62.5),
        "medium",
        "Basseterre, Belle Mont Farm, Charlestown Nevis",
    ),
    "sx_sint_maarten": Zone(
        "sx_sint_maarten",
        "St. Martin / Sint Maarten",
        "SX",
        (18.0, -63.2, 18.15, -63.0),
        "medium",
        "Philipsburg, Marigot, Le Barthélemy area",
    ),
    "gd_grenada": Zone(
        "gd_grenada",
        "Grenada",
        "GD",
        (11.95, -61.85, 12.55, -61.55),
        "low",
        "St. George's, Grand Anse",
    ),
    "dm_dominica": Zone(
        "dm_dominica",
        "Dominica",
        "DM",
        (15.2, -61.5, 15.65, -61.25),
        "low",
        "Roseau, Secret Bay",
    ),
    "tt_trinidad_tobago": Zone(
        "tt_trinidad_tobago",
        "Trinidad and Tobago",
        "TT",
        (10.0, -61.95, 11.4, -60.5),
        "low",
        "Port of Spain, Tobago",
    ),
    "vc_st_vincent_grenadines": Zone(
        "vc_st_vincent_grenadines",
        "St. Vincent & Grenadines",
        "VC",
        (12.55, -61.5, 13.4, -61.1),
        "low",
        "Kingstown, Mustique, Bequia, Canouan, Petit St. Vincent",
    ),
}


# ════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════


def zones_by_priority(priority: str) -> List[Zone]:
    return [z for z in ZONES.values() if z.priority == priority]


def zones_by_state(state: str) -> List[Zone]:
    return [z for z in ZONES.values() if z.state == state.upper()]


def all_states_covered() -> List[str]:
    return sorted({z.state for z in ZONES.values()})


def summary():
    total = len(ZONES)
    high = len(zones_by_priority("high"))
    med = len(zones_by_priority("medium"))
    low = len(zones_by_priority("low"))
    states = len(all_states_covered())
    return {
        "total_zones": total,
        "high_priority": high,
        "medium_priority": med,
        "low_priority": low,
        "states_covered": states,
    }


if __name__ == "__main__":
    s = summary()
    print("=== ZONES REGISTRY ===")
    print(f"Total zones:       {s['total_zones']}")
    print(f"  high priority:   {s['high_priority']}")
    print(f"  medium priority: {s['medium_priority']}")
    print(f"  low priority:    {s['low_priority']}")
    print(f"States covered:    {s['states_covered']}")
    print()
    print("=== BY STATE ===")
    for st in all_states_covered():
        zs = zones_by_state(st)
        print(f"  {st}: {len(zs):2d} zones  ({', '.join(z.name for z in zs)})")
