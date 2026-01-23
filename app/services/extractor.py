"""
Extractor Service - uses Ollama AI to extract lead data from text
"""
import ollama
import json
import re
import spacy
from typing import Optional
from app.config import settings

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    nlp = None


def extract_with_ollama(text: str) -> dict:
    """
    Use Ollama LLM to extract structured hotel data from text
    
    Args:
        text: Raw text from scraped article
        
    Returns:
        Dictionary with extracted fields
    """
    prompt = f"""Extract hotel information from the following text. Return ONLY valid JSON with these fields:
- hotel_name: Name of the hotel
- brand: Hotel brand/chain (e.g., Four Seasons, Hilton, Marriott)
- city: City location
- state: State/Province
- country: Country (default USA if not mentioned)
- projected_opening_date: Opening date in YYYY-MM-DD format if possible
- room_count: Number of rooms (integer)
- contact_first_name: First name of contact person
- contact_last_name: Last name of contact person
- contact_title: Job title of contact
- contact_email: Email address
- contact_phone: Phone number
- description: Brief description of the hotel

If a field is not found, use null.

Text:
{text}

JSON:"""

    try:
        response = ollama.generate(
            model=settings.ollama_model,
            prompt=prompt,
            format="json"
        )
        
        # Parse response
        result = json.loads(response["response"])
        return result
        
    except Exception as e:
        print(f"Ollama extraction error: {e}")
        return {}


def extract_emails(text: str) -> list:
    """Extract email addresses using regex"""
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return list(set(re.findall(pattern, text)))


def extract_phones(text: str) -> list:
    """Extract phone numbers using regex"""
    patterns = [
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # (123) 456-7890
        r'\+1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # +1-123-456-7890
    ]
    phones = []
    for pattern in patterns:
        phones.extend(re.findall(pattern, text))
    return list(set(phones))


def extract_dates(text: str) -> list:
    """Extract potential opening dates"""
    patterns = [
        r'(?:opening|opens|open|launching|launch)\s+(?:in\s+)?([A-Z][a-z]+\s+\d{4})',  # Opening in June 2026
        r'(?:Q[1-4])\s+(\d{4})',  # Q2 2026
        r'(?:spring|summer|fall|winter)\s+(\d{4})',  # Summer 2026
        r'(\d{4})\s+(?:opening|launch)',  # 2026 opening
    ]
    dates = []
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        dates.extend(matches)
    return list(set(dates))


def extract_room_count(text: str) -> Optional[int]:
    """Extract room count from text"""
    patterns = [
        r'(\d+)[-\s]?room',  # 200-room or 200 room
        r'(\d+)\s+(?:guest\s+)?rooms',  # 200 rooms or 200 guest rooms
        r'rooms:\s*(\d+)',  # rooms: 200
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def extract_with_spacy(text: str) -> dict:
    """
    Use spaCy NER to extract entities
    
    Args:
        text: Raw text
        
    Returns:
        Dictionary with extracted entities
    """
    if not nlp:
        return {}
    
    doc = nlp(text)
    
    entities = {
        "organizations": [],
        "locations": [],
        "persons": [],
        "dates": []
    }
    
    for ent in doc.ents:
        if ent.label_ == "ORG":
            entities["organizations"].append(ent.text)
        elif ent.label_ in ["GPE", "LOC"]:
            entities["locations"].append(ent.text)
        elif ent.label_ == "PERSON":
            entities["persons"].append(ent.text)
        elif ent.label_ == "DATE":
            entities["dates"].append(ent.text)
    
    return entities


def extract_lead_data(text: str) -> dict:
    """
    Main extraction function - combines Ollama + regex + spaCy
    
    Args:
        text: Raw text from scraped article
        
    Returns:
        Dictionary with all extracted lead data
    """
    # Start with Ollama extraction
    lead_data = extract_with_ollama(text)
    
    # Fill in missing fields with regex extraction
    if not lead_data.get("contact_email"):
        emails = extract_emails(text)
        if emails:
            lead_data["contact_email"] = emails[0]
    
    if not lead_data.get("contact_phone"):
        phones = extract_phones(text)
        if phones:
            lead_data["contact_phone"] = phones[0]
    
    if not lead_data.get("room_count"):
        room_count = extract_room_count(text)
        if room_count:
            lead_data["room_count"] = room_count
    
    # Use spaCy for additional context
    spacy_entities = extract_with_spacy(text)
    
    # If no contact name from Ollama, try spaCy
    if not lead_data.get("contact_first_name") and spacy_entities.get("persons"):
        person = spacy_entities["persons"][0]
        parts = person.split()
        if len(parts) >= 2:
            lead_data["contact_first_name"] = parts[0]
            lead_data["contact_last_name"] = " ".join(parts[1:])
        elif len(parts) == 1:
            lead_data["contact_last_name"] = parts[0]
    
    return lead_data