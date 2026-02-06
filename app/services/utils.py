"""
SMART LEAD HUNTER - Shared Utilities
=====================================
Common functions used across modules to prevent logic divergence.
"""

import re


def normalize_hotel_name(name: str) -> str:
    """Normalize hotel name for deduplication.
    
    Strips special characters, lowercases, and collapses whitespace.
    
    Used by:
    - orchestrator.py (save_leads_to_database)
    - scraping_tasks.py (_save_lead_impl)
    - Any future dedup logic
    
    Examples:
        "Ritz-Carlton Miami" → "ritzcarlton miami"
        "Four Seasons® Orlando" → "four seasons orlando"
        "  The St. Regis  " → "the st regis"
    """
    if not name:
        return ""
    # Remove all non-alphanumeric except spaces, then collapse whitespace
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9\s]', '', name.lower())).strip()