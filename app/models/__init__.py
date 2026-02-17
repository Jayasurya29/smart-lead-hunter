"""
Models package - export all models
"""

from app.models.potential_lead import PotentialLead
from app.models.source import Source
from app.models.scrape_log import ScrapeLog

from app.models.failed_domain import FailedDomain

__all__ = ["PotentialLead", "Source", "ScrapeLog", "FailedDomain"]
