"""
Lead Scoring Service - calculates lead score based on rules
"""
from datetime import datetime, timedelta
from app.config import settings


# Luxury hotel brands
LUXURY_BRANDS = [
    "four seasons", "ritz-carlton", "ritz carlton", "st. regis", "st regis",
    "park hyatt", "mandarin oriental", "aman", "rosewood", "bulgari",
    "peninsula", "waldorf astoria", "edition", "montage", "fairmont",
    "sofitel", "jw marriott", "w hotel", "conrad", "intercontinental"
]

# Florida cities
FLORIDA_CITIES = [
    "miami", "orlando", "tampa", "jacksonville", "fort lauderdale",
    "west palm beach", "naples", "sarasota", "clearwater", "key west",
    "boca raton", "palm beach", "fort myers", "daytona", "pensacola",
    "st. petersburg", "st petersburg", "gainesville", "tallahassee"
]

# Caribbean locations
CARIBBEAN_LOCATIONS = [
    "bahamas", "jamaica", "puerto rico", "dominican republic", "aruba",
    "barbados", "cayman islands", "turks and caicos", "st. lucia", "st lucia",
    "antigua", "bermuda", "curacao", "grenada", "martinique", "guadeloupe",
    "us virgin islands", "british virgin islands", "trinidad", "tobago",
    "st. martin", "st martin", "st. thomas", "st thomas", "nassau", "cancun",
    "punta cana", "montego bay", "negril", "ocho rios"
]


def calculate_lead_score(lead_data: dict) -> dict:
    """
    Calculate lead score (0-100) based on multiple factors
    
    Args:
        lead_data: Dictionary with lead information
        
    Returns:
        Dictionary with score and breakdown
    """
    score = 0
    breakdown = {}
    
    # Extract data (case-insensitive)
    hotel_name = (lead_data.get("hotel_name") or "").lower()
    brand = (lead_data.get("brand") or "").lower()
    city = (lead_data.get("city") or "").lower()
    state = (lead_data.get("state") or "").lower()
    country = (lead_data.get("country") or "").lower()
    room_count = lead_data.get("room_count") or 0
    contact_email = lead_data.get("contact_email") or ""
    contact_phone = lead_data.get("contact_phone") or ""
    opening_date = lead_data.get("projected_opening_date")
    description = (lead_data.get("description") or "").lower()
    
    # Combine text for searching
    all_text = f"{hotel_name} {brand} {description}"
    location_text = f"{city} {state} {country}"
    
    # 1. Florida Location (+15)
    is_florida = state == "florida" or state == "fl" or any(c in city for c in FLORIDA_CITIES)
    if is_florida:
        score += settings.score_florida
        breakdown["florida_location"] = settings.score_florida
    
    # 2. Caribbean Location (+15)
    is_caribbean = any(loc in location_text for loc in CARIBBEAN_LOCATIONS)
    if is_caribbean:
        score += settings.score_caribbean
        breakdown["caribbean_location"] = settings.score_caribbean
    
    # 3. Luxury Brand (+20)
    is_luxury = any(brand_name in all_text for brand_name in LUXURY_BRANDS)
    if is_luxury:
        score += settings.score_luxury_brand
        breakdown["luxury_brand"] = settings.score_luxury_brand
    
    # 4. Room Count 100+ (+10)
    if room_count and room_count >= 100:
        score += settings.score_room_count_100
        breakdown["room_count_100_plus"] = settings.score_room_count_100
    
    # 5. Has Direct Contact (+15)
    has_contact = bool(contact_email) or bool(contact_phone)
    if has_contact:
        score += settings.score_has_contact
        breakdown["has_contact"] = settings.score_has_contact
    
    # 6. Opening Soon - within 6 months (+10)
    if opening_date:
        if isinstance(opening_date, str):
            try:
                opening_date = datetime.strptime(opening_date, "%Y-%m-%d").date()
            except ValueError:
                opening_date = None
        
        if opening_date:
            six_months_later = datetime.now().date() + timedelta(days=180)
            if opening_date <= six_months_later:
                score += settings.score_opening_soon
                breakdown["opening_soon"] = settings.score_opening_soon
    
    # 7. Bonus points for quality indicators
    bonus = 0
    
    # Has website
    if lead_data.get("hotel_website"):
        bonus += 5
        breakdown["has_website"] = 5
    
    # Has both email AND phone
    if contact_email and contact_phone:
        bonus += 5
        breakdown["has_email_and_phone"] = 5
    
    score += bonus
    
    # Cap at 100
    score = min(score, 100)
    
    return {
        "score": score,
        "breakdown": breakdown,
        "is_florida": is_florida,
        "is_caribbean": is_caribbean,
        "is_luxury": is_luxury
    }


def get_score_tier(score: int) -> str:
    """Get tier label based on score"""
    if score >= 80:
        return "Hot Lead"
    elif score >= 60:
        return "Warm Lead"
    elif score >= 40:
        return "Cool Lead"
    else:
        return "Cold Lead"