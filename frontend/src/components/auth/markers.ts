// MARKERS — hotel lead points across USA + Caribbean for the auth-page radar map.
// Each entry can be a labeled active lead (with a hotel name + status) or a
// cool background dot (city only, no label).
//
// `delay` is the seconds offset within a 24-second master cycle; values are
// hand-tuned per geographic cluster so adjacent labels never overlap in time.

export type MarkerTier = 'hot' | 'warm' | 'cool'
export type LabelPos   = 'up' | 'down' | 'left' | 'right'

export interface Marker {
  city: string
  lng: number
  lat: number
  tier: MarkerTier
  /** Hotel name shown in brass when label is visible. Omit for cool dots. */
  lead?: string
  /** "CITY · STATE · STATUS" line under the lead name. */
  note?: string
  /** Direction the label flies in from. */
  pos?: LabelPos
  /** Animation delay in seconds (0–24). */
  delay?: number
}

export const MARKERS: Marker[] = [
  // ───── NORTHEAST CORRIDOR (delays 0,4,8,12,16,20) ─────
  { city: 'NEW YORK · NY',     lng: -74.00, lat: 40.71, tier: 'warm', lead: 'AMAN NEW YORK',              note: 'NEW YORK · NY · RENOVATION',          pos: 'left',  delay: 0  },
  { city: 'BOSTON · MA',       lng: -71.06, lat: 42.36, tier: 'hot',  lead: 'RAFFLES BOSTON',             note: 'BOSTON · MA · OPENING Q3 2026',       pos: 'up',    delay: 4  },
  { city: 'PHILADELPHIA · PA', lng: -75.16, lat: 39.95, tier: 'warm', lead: 'FOUR SEASONS PHILADELPHIA',  note: 'PHILADELPHIA · PA · RENOVATION',      pos: 'left',  delay: 8  },
  { city: 'WASHINGTON · DC',   lng: -77.04, lat: 38.91, tier: 'warm', lead: 'PARK HYATT WASHINGTON',      note: 'WASHINGTON · DC · RENOVATION',        pos: 'left',  delay: 12 },
  { city: 'NEWPORT · RI',      lng: -71.31, lat: 41.49, tier: 'warm', lead: 'OCEAN HOUSE',                note: 'WATCH HILL · RI · RENOVATION',        pos: 'right', delay: 16 },
  { city: 'BAR HARBOR · ME',   lng: -68.20, lat: 44.39, tier: 'warm', lead: 'BAR HARBOR INN',             note: 'BAR HARBOR · ME · PROCUREMENT',       pos: 'up',    delay: 20 },

  // ───── SOUTHEAST / FLORIDA (delays 2,6,10,14,18,22) ─────
  { city: 'CHARLESTON · SC',   lng: -79.93, lat: 32.78, tier: 'hot',  lead: 'CONRAD CHARLESTON',          note: 'CHARLESTON · SC · PRE-OPENING Q3 2026', pos: 'left',  delay: 2  },
  { city: 'PALMETTO BLUFF',    lng: -80.86, lat: 32.24, tier: 'warm', lead: 'MONTAGE PALMETTO BLUFF',     note: 'BLUFFTON · SC · RENOVATION',          pos: 'left',  delay: 6  },
  { city: 'SAVANNAH · GA',     lng: -81.10, lat: 32.08, tier: 'warm', lead: 'MANDARIN ORIENTAL SAVANNAH', note: 'SAVANNAH · GA · PRE-OPENING 2027',    pos: 'down',  delay: 10 },
  { city: 'ATLANTA · GA',      lng: -84.39, lat: 33.75, tier: 'warm', lead: 'ST. REGIS ATLANTA',          note: 'ATLANTA · GA · REOPENING',            pos: 'left',  delay: 14 },
  { city: 'MIAMI · FL',        lng: -80.19, lat: 25.76, tier: 'hot',  lead: 'AMAN MIAMI BEACH',           note: 'MIAMI BEACH · FL · PRE-OPENING Q1 2027', pos: 'right', delay: 18 },
  { city: 'KEY WEST · FL',     lng: -81.80, lat: 24.55, tier: 'warm', lead: 'OCEAN KEY RESORT',           note: 'KEY WEST · FL · RENOVATION',          pos: 'down',  delay: 22 },

  // ───── GULF / TX / TN (delays 1,5,9,13) ─────
  { city: 'NEW ORLEANS · LA',  lng: -90.07, lat: 29.95, tier: 'warm', lead: 'WINDSOR COURT',              note: 'NEW ORLEANS · LA · RENOVATION',       pos: 'down',  delay: 1  },
  { city: 'AUSTIN · TX',       lng: -97.74, lat: 30.27, tier: 'warm', lead: 'PROPER AUSTIN',              note: 'AUSTIN · TX · PRE-OPENING 2026',      pos: 'down',  delay: 5  },
  { city: 'DALLAS · TX',       lng: -96.80, lat: 32.78, tier: 'warm', lead: 'ROSEWOOD MANSION',           note: 'DALLAS · TX · REOPENING',             pos: 'up',    delay: 9  },
  { city: 'NASHVILLE · TN',    lng: -86.78, lat: 36.16, tier: 'hot',  lead: '1 HOTEL NASHVILLE',          note: 'NASHVILLE · TN · PRE-OPENING 2027',   pos: 'up',    delay: 13 },

  // ───── MIDWEST / MOUNTAIN (delays 3,7,11,15) ─────
  { city: 'CHICAGO · IL',      lng: -87.65, lat: 41.88, tier: 'warm', lead: 'PENINSULA CHICAGO',          note: 'CHICAGO · IL · RENOVATION',           pos: 'up',    delay: 3  },
  { city: 'ASPEN · CO',        lng: -106.82,lat: 39.19, tier: 'warm', lead: 'ST. REGIS ASPEN',            note: 'ASPEN · CO · RENOVATION',             pos: 'down',  delay: 7  },
  { city: 'JACKSON · WY',      lng: -110.76,lat: 43.48, tier: 'warm', lead: 'AMANGANI',                   note: 'JACKSON HOLE · WY · RENOVATION',      pos: 'up',    delay: 11 },
  { city: 'SEDONA · AZ',       lng: -111.76,lat: 34.87, tier: 'warm', lead: 'ENCHANTMENT RESORT',         note: 'SEDONA · AZ · RENOVATION',            pos: 'down',  delay: 15 },

  // ───── WEST COAST (delays 17, 21, 5.5, 11.5) ─────
  { city: 'NAPA · CA',         lng: -122.46,lat: 38.50, tier: 'warm', lead: 'AUBERGE DU SOLEIL',          note: 'NAPA VALLEY · CA · RENOVATION',       pos: 'up',    delay: 17 },
  { city: 'LOS ANGELES · CA',  lng: -118.40,lat: 34.07, tier: 'warm', lead: 'WALDORF BEVERLY HILLS',      note: 'BEVERLY HILLS · CA · PROCUREMENT',    pos: 'down',  delay: 21 },
  { city: 'SANTA BARBARA · CA',lng: -119.71,lat: 34.41, tier: 'warm', lead: 'ROSEWOOD MIRAMAR BEACH',     note: 'MONTECITO · CA · RENOVATION',         pos: 'down',  delay: 5.5},
  { city: 'SAN DIEGO · CA',    lng: -117.21,lat: 32.93, tier: 'warm', lead: 'FAIRMONT GRAND DEL MAR',     note: 'SAN DIEGO · CA · PROCUREMENT',        pos: 'down',  delay: 11.5},

  // ───── CARIBBEAN MAIN BAND (delays 2.5, 8.5, 14.5, 20.5) ─────
  { city: 'GRAND CAYMAN',      lng: -81.40, lat: 19.30, tier: 'warm', lead: 'RITZ-CARLTON GRAND CAYMAN',  note: 'SEVEN MILE BEACH · KY · RENOVATION',  pos: 'up',    delay: 2.5 },
  { city: 'MONTEGO BAY · JA',  lng: -77.92, lat: 18.47, tier: 'warm', lead: 'HALF MOON RESORT',           note: 'MONTEGO BAY · JM · PROCUREMENT',      pos: 'down',  delay: 8.5 },
  { city: 'PORT ANTONIO · JA', lng: -76.45, lat: 18.19, tier: 'warm', lead: 'GEEJAM HOTEL',               note: 'PORT ANTONIO · JM · RENOVATION',      pos: 'down',  delay: 14.5},
  { city: 'TURKS & CAICOS',    lng: -72.30, lat: 21.84, tier: 'warm', lead: 'AMANYARA',                   note: 'PROVIDENCIALES · TC · PROCUREMENT',   pos: 'up',    delay: 20.5},

  // ───── BAHAMAS (delays 6.5, 12.5) ─────
  { city: 'NASSAU · BS',       lng: -77.40, lat: 25.04, tier: 'warm', lead: 'ROSEWOOD BAHA MAR',          note: 'NASSAU · BS · REOPENING',             pos: 'up',    delay: 6.5 },
  { city: 'EXUMA · BS',        lng: -75.83, lat: 23.62, tier: 'warm', lead: 'GRAND ISLE RESORT',          note: 'GREAT EXUMA · BS · PRE-OPENING',      pos: 'right', delay: 12.5},

  // ───── PR / VI / LEEWARDS (delays 4.5, 10.5, 16.5, 22.5, 0.5) ─────
  { city: 'SAN JUAN · PR',     lng: -66.11, lat: 18.47, tier: 'hot',  lead: 'VANDERBILT SAN JUAN',        note: 'SAN JUAN · PR · PRE-OPENING 2027',    pos: 'right', delay: 4.5 },
  { city: 'PUNTA CANA · DR',   lng: -68.40, lat: 18.58, tier: 'hot',  lead: 'AMAN PUNTA CANA',            note: 'PUNTA CANA · DO · PRE-OPENING 2026',  pos: 'down',  delay: 10.5},
  { city: 'ST. THOMAS · VI',   lng: -64.92, lat: 18.34, tier: 'warm', lead: 'RITZ-CARLTON ST. THOMAS',    note: 'ST. THOMAS · VI · REOPENING',         pos: 'right', delay: 16.5},
  { city: 'ST. BARTHS',        lng: -62.83, lat: 17.91, tier: 'warm', lead: 'LE BARTHÉLEMY',              note: 'GUSTAVIA · BL · RENOVATION',          pos: 'right', delay: 22.5},
  { city: 'ANTIGUA',           lng: -61.84, lat: 17.16, tier: 'warm', lead: 'JUMBY BAY ISLAND',           note: 'LONG ISLAND · AG · PROCUREMENT',      pos: 'right', delay: 0.5 },

  // ───── COOL BACKGROUND (no labels) ─────
  { city: 'Seattle',         lng: -122.33, lat: 47.61, tier: 'cool' },
  { city: 'Portland OR',     lng: -122.68, lat: 45.52, tier: 'cool' },
  { city: 'San Francisco',   lng: -122.42, lat: 37.78, tier: 'cool' },
  { city: 'Las Vegas',       lng: -115.14, lat: 36.17, tier: 'cool' },
  { city: 'Phoenix',         lng: -112.07, lat: 33.45, tier: 'cool' },
  { city: 'Salt Lake City',  lng: -111.89, lat: 40.76, tier: 'cool' },
  { city: 'Denver',          lng: -104.99, lat: 39.74, tier: 'cool' },
  { city: 'Santa Fe',        lng: -105.94, lat: 35.69, tier: 'cool' },
  { city: 'San Antonio',     lng: -98.49,  lat: 29.42, tier: 'cool' },
  { city: 'Houston',         lng: -95.37,  lat: 29.76, tier: 'cool' },
  { city: 'Memphis',         lng: -90.05,  lat: 35.15, tier: 'cool' },
  { city: 'Minneapolis',     lng: -93.27,  lat: 44.98, tier: 'cool' },
  { city: 'Kansas City',     lng: -94.58,  lat: 39.10, tier: 'cool' },
  { city: 'St Louis',        lng: -90.20,  lat: 38.63, tier: 'cool' },
  { city: 'Indianapolis',    lng: -86.16,  lat: 39.77, tier: 'cool' },
  { city: 'Detroit',         lng: -83.05,  lat: 42.33, tier: 'cool' },
  { city: 'Cleveland',       lng: -81.69,  lat: 41.50, tier: 'cool' },
  { city: 'Birmingham',      lng: -86.80,  lat: 33.52, tier: 'cool' },
  { city: 'Tampa',           lng: -82.46,  lat: 27.95, tier: 'cool' },
  { city: 'Orlando',         lng: -81.38,  lat: 28.54, tier: 'cool' },
  { city: 'Jacksonville',    lng: -81.66,  lat: 30.33, tier: 'cool' },
  { city: 'Raleigh',         lng: -78.64,  lat: 35.78, tier: 'cool' },
  { city: 'Charlotte',       lng: -80.84,  lat: 35.23, tier: 'cool' },
  { city: 'Norfolk',         lng: -76.29,  lat: 36.85, tier: 'cool' },
  { city: 'Pittsburgh',      lng: -79.99,  lat: 40.44, tier: 'cool' },
]

/** Project geographic coordinates into the map's SVG / overlay space. */
export const project = (lng: number, lat: number): [number, number] => [
  (lng + 130) * (100 / 70),
  (50 - lat) * (100 / 40),
]

/** Simplified USA continental silhouette in the projected coordinate space. */
export const USA_PATH = `
  M 5,4 L 4.5,15 L 4.6,22 L 6.5,28 L 10,30.5 L 12.5,33 L 13.8,36
  L 16,39.5 L 18.5,42.5 L 21,44 L 25,44.5 L 30,46.5 L 35,46
  L 40,49 L 44,55 L 47.5,59.5 L 50,53.5 L 53,52.5 L 57.5,52.5
  L 60,50 L 62.5,49.5 L 64.5,50 L 66.5,51 L 67.5,55 L 68.8,58.5
  L 70.8,60.5 L 72,57 L 71,52 L 70.5,47.5 L 71.5,43.5 L 73.5,40
  L 76,37.5 L 77.5,34.5 L 78,31 L 79,27 L 80,23.5 L 82,21.5
  L 83.5,19.5 L 85,17.5 L 87,14.5 L 89,12 L 90,9 L 89,6
  L 86.5,5 L 80,5 L 70,6.5 L 60,5.5 L 50,4.5 L 40,4
  L 25,4 L 12,4 Z
`

/** Caribbean islands as positioned ellipses (rough). */
export interface CaribIsland {
  cx: number
  cy: number
  rx: number
  ry: number
  rot?: number
}

export const CARIBBEAN: CaribIsland[] = [
  { cx: 70,   cy: 71,   rx: 8,   ry: 1.6, rot: 16 },   // Cuba
  { cx: 84,   cy: 78,   rx: 4.2, ry: 1.5, rot: 6  },   // Hispaniola
  { cx: 91.3, cy: 78.5, rx: 1.4, ry: 0.7         },    // Puerto Rico
  { cx: 74.5, cy: 79.5, rx: 1.5, ry: 0.7         },    // Jamaica
  { cx: 73.5, cy: 61.5, rx: 1.6, ry: 0.5, rot: -25 },  // Grand Bahama
  { cx: 74.5, cy: 63.5, rx: 1.2, ry: 1.0         },    // New Providence
  { cx: 76.5, cy: 64.5, rx: 1.4, ry: 0.4, rot: -45 },  // Eleuthera
  { cx: 77.5, cy: 67.5, rx: 1.0, ry: 0.4, rot: -50 },  // Long Island
  { cx: 69.5, cy: 77,   rx: 0.8, ry: 0.3         },    // Grand Cayman
]
