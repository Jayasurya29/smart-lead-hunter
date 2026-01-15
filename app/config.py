"""
Application configuration - loads settings from .env file
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Database
    database_url: str = "postgresql://postgres:leadhunter123@localhost:5432/smart_lead_hunter"
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    
    # Ollama
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:3b"
    
    # Insightly
    insightly_api_key: str = ""
    insightly_pod: str = "na1"
    
    @property
    def insightly_api_url(self) -> str:
        return f"https://api.{self.insightly_pod}.insightly.com/v3.1"
    
    # Scraping settings
    scrape_delay: float = 2.0  # seconds between requests
    max_depth: int = 3  # how deep to crawl
    
    # Lead scoring weights
    score_florida: int = 15
    score_caribbean: int = 15
    score_luxury_brand: int = 20
    score_room_count_100: int = 10
    score_has_contact: int = 15
    score_opening_soon: int = 10
    
    # Environment
    environment: str = "development"
    debug: bool = True
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Export settings instance
settings = get_settings()