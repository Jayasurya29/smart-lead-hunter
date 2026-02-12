"""
SMART LEAD HUNTER - Application Configuration
==============================================
Loads settings from .env file with FREE AI stack support

AI Providers (in order of priority):
1. Google Gemini (PRIMARY - $300 free credits)
2. Ollama Local (BACKUP - runs locally, unlimited)
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # -------------------------------------------------------------------------
    # DATABASE
    # -------------------------------------------------------------------------
    database_url: str

    # -------------------------------------------------------------------------
    # REDIS
    # -------------------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # -------------------------------------------------------------------------
    # AI EXTRACTION - FREE STACK
    # -------------------------------------------------------------------------

    # Google Gemini (PRIMARY - $300 free credits, best quality)
    # Get your key at: https://aistudio.google.com/apikey
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.0-flash"

    # Ollama (BACKUP - runs locally, unlimited)
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:3b"

    # -------------------------------------------------------------------------
    # INSIGHTLY CRM
    # -------------------------------------------------------------------------
    insightly_api_key: str = ""
    insightly_pod: str = "na1"

    @property
    def insightly_api_url(self) -> str:
        return f"https://api.{self.insightly_pod}.insightly.com/v3.1"

    # -------------------------------------------------------------------------
    # SCRAPING SETTINGS
    # -------------------------------------------------------------------------
    scrape_delay: float = 2.0  # Seconds between requests
    max_depth: int = 2  # How deep to crawl
    user_agent: str = "SmartLeadHunter/1.0 (Hotel Research Bot)"

    # -------------------------------------------------------------------------
    # LEAD SCORING WEIGHTS (100 points total)
    # -------------------------------------------------------------------------
    # These match your scorer.py settings
    score_brand_tier_max: int = 25  # Ultra-luxury = 25, Luxury = 20, etc.
    score_location_max: int = 20  # Florida = 20, Caribbean = 15
    score_timing_max: int = 25  # 2026 = 25, 2027 = 18
    score_room_count_max: int = 15  # 500+ = 15, 300+ = 12
    score_contact_max: int = 8  # Name + Email + Phone = 8
    score_new_build_max: int = 4  # New construction bonus
    score_existing_client_max: int = 3  # Already a customer

    # -------------------------------------------------------------------------
    # LEAD CLASSIFICATION THRESHOLDS
    # -------------------------------------------------------------------------
    hot_lead_threshold: int = 70  # Score >= 70 = hot lead
    warm_lead_threshold: int = 50  # Score >= 50 = warm lead
    min_score_threshold: int = 20  # Score < 20 = filtered out

    # -------------------------------------------------------------------------
    # ENVIRONMENT
    # -------------------------------------------------------------------------
    environment: str = "production"
    debug: bool = False

    # -------------------------------------------------------------------------
    # PYDANTIC CONFIG
    # -------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # -------------------------------------------------------------------------
    # HELPER METHODS
    # -------------------------------------------------------------------------

    @property
    def has_gemini(self) -> bool:
        """Check if Gemini API is configured"""
        return bool(self.gemini_api_key and len(self.gemini_api_key) > 10)

    @property
    def has_insightly(self) -> bool:
        """Check if Insightly is configured"""
        return bool(
            self.insightly_api_key
            and self.insightly_api_key != "your-insightly-api-key"
        )

    def get_ai_status(self) -> dict:
        """Get status of AI providers (sync version for CLI/startup only)"""
        import httpx

        status = {
            "gemini": {
                "configured": self.has_gemini,
                "model": self.gemini_model,
                "available": self.has_gemini,
                "cost": "$0 (free credits)",
                "priority": 1,
            },
            "ollama": {
                "configured": True,
                "model": self.ollama_model,
                "available": False,
                "cost": "$0 (local)",
                "priority": 2,
            },
        }

        # Check Ollama availability
        try:
            response = httpx.get(f"{self.ollama_url}/api/tags", timeout=5.0)
            status["ollama"]["available"] = response.status_code == 200
        except Exception:
            pass

        return status

    async def get_ai_status_async(self) -> dict:
        """Get status of AI providers (async version for use in FastAPI endpoints)"""
        import httpx

        status = {
            "gemini": {
                "configured": self.has_gemini,
                "model": self.gemini_model,
                "available": self.has_gemini,
                "cost": "$0 (free credits)",
                "priority": 1,
            },
            "ollama": {
                "configured": True,
                "model": self.ollama_model,
                "available": False,
                "cost": "$0 (local)",
                "priority": 2,
            },
        }

        # Check Ollama availability without blocking the event loop
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.ollama_url}/api/tags")
                status["ollama"]["available"] = response.status_code == 200
        except Exception:
            pass

        return status

    def get_best_ai_provider(self) -> str:
        """Get the best available AI provider"""
        if self.has_gemini:
            return "gemini"
        else:
            return "ollama"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings()


# =============================================================================
# QUICK STATUS CHECK
# =============================================================================


def print_config_status():
    """Print configuration status for debugging"""
    s = get_settings()
    ai_status = s.get_ai_status()

    print("\n" + "=" * 60)
    print("SMART LEAD HUNTER - CONFIGURATION STATUS")
    print("=" * 60)

    print("\n📊 DATABASE")
    print(f"   URL: {s.database_url[:50]}...")

    print("\n🤖 AI EXTRACTION (FREE STACK)")

    # Gemini status (PRIMARY)
    gemini = ai_status["gemini"]
    gemini_icon = "✅" if gemini["available"] else "❌"
    print(f"   {gemini_icon} Gemini API ({gemini['model']}) - PRIMARY")
    print(f"      Configured: {gemini['configured']}")
    print(f"      Cost: {gemini['cost']}")
    if not gemini["configured"]:
        print("      ⚠️  Get FREE key at: https://aistudio.google.com/apikey")

    # Ollama status (BACKUP)
    ollama = ai_status["ollama"]
    ollama_icon = "✅" if ollama["available"] else "⚠️"
    print(f"   {ollama_icon} Ollama Local ({ollama['model']}) - BACKUP")
    print(f"      Running: {ollama['available']}")
    print(f"      Cost: {ollama['cost']}")
    if not ollama["available"]:
        print("      ℹ️  Start with: ollama serve")

    print(f"\n   🎯 Best Available: {s.get_best_ai_provider().upper()}")

    print("\n📤 CRM")
    insightly_icon = "✅" if s.has_insightly else "⚠️"
    print(
        f"   {insightly_icon} Insightly: {'Configured' if s.has_insightly else 'Not configured'}"
    )

    print("\n💰 TOTAL AI COST: $0/month")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    print_config_status()
