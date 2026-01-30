"""
SMART LEAD HUNTER - Application Configuration
==============================================
Loads settings from .env file with FREE AI stack support

AI Providers (in order of priority):
1. Groq API (FREE - Llama 3.3 70B)
2. Ollama Local (FREE - any model)
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
    
    # Groq API (PRIMARY - FREE tier, no card required)
    # Get your key at: https://console.groq.com/
    groq_api_key: Optional[str] = None
    groq_model: str = "llama-3.3-70b-versatile"  # Best free model
    
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
    score_brand_tier_max: int = 25      # Ultra-luxury = 25, Luxury = 20, etc.
    score_location_max: int = 20        # Florida = 20, Caribbean = 15
    score_timing_max: int = 25          # 2026 = 25, 2027 = 18
    score_room_count_max: int = 15      # 500+ = 15, 300+ = 12
    score_contact_max: int = 8          # Name + Email + Phone = 8
    score_new_build_max: int = 4        # New construction bonus
    score_existing_client_max: int = 3  # Already a customer
    
    # -------------------------------------------------------------------------
    # ENVIRONMENT
    # -------------------------------------------------------------------------
    environment: str = "development"
    debug: bool = True
    
    # -------------------------------------------------------------------------
    # PYDANTIC CONFIG
    # -------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    # -------------------------------------------------------------------------
    # HELPER METHODS
    # -------------------------------------------------------------------------
    
    @property
    def has_groq(self) -> bool:
        """Check if Groq API is configured"""
        return bool(self.groq_api_key and self.groq_api_key != "your-free-groq-api-key-here")
    
    @property
    def has_insightly(self) -> bool:
        """Check if Insightly is configured"""
        return bool(self.insightly_api_key and self.insightly_api_key != "your-insightly-api-key")
    
    def get_ai_status(self) -> dict:
        """Get status of AI providers"""
        import httpx
        
        status = {
            "groq": {
                "configured": self.has_groq,
                "model": self.groq_model,
                "available": False,
                "cost": "$0 (free tier)"
            },
            "ollama": {
                "configured": True,
                "model": self.ollama_model,
                "available": False,
                "cost": "$0 (local)"
            }
        }
        
        # Check Ollama availability
        try:
            response = httpx.get(f"{self.ollama_url}/api/tags", timeout=5.0)
            status["ollama"]["available"] = response.status_code == 200
        except:
            pass
        
        # Groq is available if configured (it's a cloud service)
        status["groq"]["available"] = self.has_groq
        
        return status


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
    
    # Groq status
    groq = ai_status["groq"]
    groq_icon = "✅" if groq["available"] else "❌"
    print(f"   {groq_icon} Groq API ({groq['model']})")
    print(f"      Configured: {groq['configured']}")
    print(f"      Cost: {groq['cost']}")
    if not groq["configured"]:
        print(f"      ⚠️  Get FREE key at: https://console.groq.com/")
    
    # Ollama status
    ollama = ai_status["ollama"]
    ollama_icon = "✅" if ollama["available"] else "⚠️"
    print(f"   {ollama_icon} Ollama Local ({ollama['model']})")
    print(f"      Running: {ollama['available']}")
    print(f"      Cost: {ollama['cost']}")
    if not ollama["available"]:
        print(f"      ℹ️  Start with: ollama serve")
    
    print("\n📤 CRM")
    insightly_icon = "✅" if s.has_insightly else "⚠️"
    print(f"   {insightly_icon} Insightly: {'Configured' if s.has_insightly else 'Not configured'}")
    
    print("\n💰 TOTAL AI COST: $0/month")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    print_config_status()