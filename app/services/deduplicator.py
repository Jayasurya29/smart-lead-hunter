"""
Lead Deduplication Service
--------------------------
Uses pgvector for semantic similarity matching to detect duplicates

Features:
- Fuzzy matching using text embeddings
- Catches variations like "Four Seasons Naples" vs "The Four Seasons - Naples FL"
- Configurable similarity threshold
- Fast vector search using pgvector index

Dependencies:
- sentence-transformers (for generating embeddings)
- pgvector (PostgreSQL extension)
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import PotentialLead

logger = logging.getLogger(__name__)


# Similarity threshold (0.0 to 1.0)
# Higher = stricter matching (fewer false positives, more duplicates slip through)
# Lower = looser matching (more false positives, fewer duplicates slip through)
DEFAULT_SIMILARITY_THRESHOLD = 0.85


@dataclass
class DuplicateMatch:
    """Represents a potential duplicate match"""
    lead_id: int
    hotel_name: str
    city: Optional[str]
    state: Optional[str]
    similarity_score: float
    match_reason: str


class Deduplicator:
    """
    Deduplication service using multiple strategies:
    1. Exact match on normalized hotel name + city
    2. Vector similarity using pgvector embeddings
    3. Fuzzy text matching as fallback
    
    Usage:
        dedup = Deduplicator(db_session)
        
        # Check if lead is duplicate
        is_dup, matches = await dedup.check_duplicate(
            hotel_name="Four Seasons Naples",
            city="Naples",
            state="Florida"
        )
        
        if is_dup:
            print(f"Found {len(matches)} potential duplicates")
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.model = None
        self._model_loaded = False
    
    def _load_embedding_model(self):
        """Lazy load the sentence transformer model"""
        if not self._model_loaded:
            try:
                from sentence_transformers import SentenceTransformer
                # Using a small, fast model suitable for short text
                self.model = SentenceTransformer('all-MiniLM-L6-v2')
                self._model_loaded = True
                logger.info("Embedding model loaded successfully")
            except ImportError:
                logger.warning(
                    "sentence-transformers not installed. "
                    "Install with: pip install sentence-transformers"
                )
                self.model = None
            except Exception as e:
                logger.error(f"Failed to load embedding model: {e}")
                self.model = None
    
    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding vector for text
        
        Args:
            text: Text to embed (hotel name + location)
        
        Returns:
            384-dimensional embedding vector or None
        """
        self._load_embedding_model()
        
        if self.model is None:
            return None
        
        try:
            # Normalize text
            normalized = self._normalize_text(text)
            # Generate embedding
            embedding = self.model.encode(normalized, convert_to_numpy=True)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return None
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text for better matching
        
        - Lowercase
        - Remove extra whitespace
        - Remove common prefixes/suffixes
        - Standardize punctuation
        """
        if not text:
            return ""
        
        normalized = text.lower().strip()
        
        # Remove common hotel prefixes
        prefixes = ["the ", "hotel ", "resort "]
        for prefix in prefixes:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
        
        # Remove common suffixes
        suffixes = [" hotel", " resort", " & spa", " spa", " suites"]
        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
        
        # Standardize separators
        normalized = normalized.replace(" - ", " ")
        normalized = normalized.replace(", ", " ")
        normalized = normalized.replace("  ", " ")
        
        return normalized.strip()
    
    def _build_search_text(
        self, 
        hotel_name: str, 
        city: Optional[str] = None,
        state: Optional[str] = None
    ) -> str:
        """Build combined text for embedding search"""
        parts = [hotel_name]
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        return " ".join(parts)
    
    async def check_duplicate(
        self,
        hotel_name: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        threshold: float = DEFAULT_SIMILARITY_THRESHOLD
    ) -> Tuple[bool, List[DuplicateMatch]]:
        """
        Check if a lead is a duplicate using multiple strategies
        
        Args:
            hotel_name: Name of the hotel
            city: City location
            state: State/region
            threshold: Similarity threshold (0.0-1.0)
        
        Returns:
            Tuple of (is_duplicate, list of matches)
        """
        matches = []
        
        # Strategy 1: Exact match on normalized name + city
        exact_matches = await self._check_exact_match(hotel_name, city)
        matches.extend(exact_matches)
        
        # Strategy 2: Vector similarity search (if embeddings available)
        vector_matches = await self._check_vector_similarity(
            hotel_name, city, state, threshold
        )
        
        # Add vector matches that aren't already in exact matches
        existing_ids = {m.lead_id for m in matches}
        for vm in vector_matches:
            if vm.lead_id not in existing_ids:
                matches.append(vm)
        
        # Strategy 3: Fuzzy text matching as fallback
        if not matches:
            fuzzy_matches = await self._check_fuzzy_match(hotel_name, city)
            matches.extend(fuzzy_matches)
        
        is_duplicate = len(matches) > 0
        
        if is_duplicate:
            logger.info(
                f"Duplicate check for '{hotel_name}': "
                f"Found {len(matches)} potential matches"
            )
        
        return is_duplicate, matches
    
    async def _check_exact_match(
        self, 
        hotel_name: str, 
        city: Optional[str]
    ) -> List[DuplicateMatch]:
        """Check for exact matches on normalized hotel name"""
        matches = []
        normalized_name = self._normalize_text(hotel_name)
        
        try:
            # Query for exact normalized name match
            query = self.db.query(PotentialLead).filter(
                PotentialLead.hotel_name_normalized == normalized_name
            )
            
            if city:
                normalized_city = self._normalize_text(city)
                query = query.filter(
                    PotentialLead.city.ilike(f"%{normalized_city}%")
                )
            
            results = query.limit(5).all()
            
            for lead in results:
                matches.append(DuplicateMatch(
                    lead_id=lead.id,
                    hotel_name=lead.hotel_name,
                    city=lead.city,
                    state=lead.state,
                    similarity_score=1.0,
                    match_reason="Exact name match"
                ))
                
        except Exception as e:
            logger.error(f"Exact match query failed: {e}")
        
        return matches
    
    async def _check_vector_similarity(
        self,
        hotel_name: str,
        city: Optional[str],
        state: Optional[str],
        threshold: float
    ) -> List[DuplicateMatch]:
        """Check for similar leads using pgvector"""
        matches = []
        
        # Generate embedding for search
        search_text = self._build_search_text(hotel_name, city, state)
        embedding = self.generate_embedding(search_text)
        
        if embedding is None:
            return matches
        
        try:
            # pgvector cosine similarity search
            # Note: pgvector uses <=> for cosine distance (1 - similarity)
            # So we search for distance < (1 - threshold)
            distance_threshold = 1 - threshold
            
            sql = text("""
                SELECT 
                    id,
                    hotel_name,
                    city,
                    state,
                    1 - (embedding <=> :embedding::vector) as similarity
                FROM potential_leads
                WHERE embedding IS NOT NULL
                AND (1 - (embedding <=> :embedding::vector)) >= :threshold
                ORDER BY embedding <=> :embedding::vector
                LIMIT 5
            """)
            
            result = self.db.execute(
                sql, 
                {
                    "embedding": str(embedding),
                    "threshold": threshold
                }
            )
            
            for row in result:
                matches.append(DuplicateMatch(
                    lead_id=row.id,
                    hotel_name=row.hotel_name,
                    city=row.city,
                    state=row.state,
                    similarity_score=float(row.similarity),
                    match_reason=f"Vector similarity ({row.similarity:.2%})"
                ))
                
        except Exception as e:
            logger.error(f"Vector similarity search failed: {e}")
        
        return matches
    
    async def _check_fuzzy_match(
        self, 
        hotel_name: str, 
        city: Optional[str]
    ) -> List[DuplicateMatch]:
        """
        Fallback fuzzy matching using PostgreSQL similarity functions
        
        Uses pg_trgm extension for trigram similarity
        """
        matches = []
        normalized_name = self._normalize_text(hotel_name)
        
        try:
            # Using PostgreSQL trigram similarity
            # Requires: CREATE EXTENSION pg_trgm;
            sql = text("""
                SELECT 
                    id,
                    hotel_name,
                    city,
                    state,
                    similarity(hotel_name_normalized, :name) as sim_score
                FROM potential_leads
                WHERE similarity(hotel_name_normalized, :name) > 0.4
                ORDER BY sim_score DESC
                LIMIT 5
            """)
            
            result = self.db.execute(sql, {"name": normalized_name})
            
            for row in result:
                # Additional city check if provided
                if city and row.city:
                    city_match = self._normalize_text(city) in self._normalize_text(row.city)
                    if not city_match:
                        continue
                
                matches.append(DuplicateMatch(
                    lead_id=row.id,
                    hotel_name=row.hotel_name,
                    city=row.city,
                    state=row.state,
                    similarity_score=float(row.sim_score),
                    match_reason=f"Fuzzy text match ({row.sim_score:.2%})"
                ))
                
        except Exception as e:
            # pg_trgm might not be installed
            logger.warning(f"Fuzzy match failed (pg_trgm may not be installed): {e}")
        
        return matches
    
    async def update_embedding(self, lead_id: int) -> bool:
        """
        Update the embedding for a specific lead
        
        Call this when a lead is created or updated
        """
        try:
            lead = self.db.query(PotentialLead).filter(
                PotentialLead.id == lead_id
            ).first()
            
            if not lead:
                return False
            
            # Build search text and generate embedding
            search_text = self._build_search_text(
                lead.hotel_name, 
                lead.city, 
                lead.state
            )
            embedding = self.generate_embedding(search_text)
            
            if embedding:
                # Update the embedding column
                self.db.execute(
                    text("UPDATE potential_leads SET embedding = :embedding WHERE id = :id"),
                    {"embedding": str(embedding), "id": lead_id}
                )
                self.db.commit()
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to update embedding for lead {lead_id}: {e}")
            return False
    
    async def bulk_update_embeddings(self, batch_size: int = 100) -> Dict[str, int]:
        """
        Update embeddings for all leads that don't have one
        
        Returns:
            Stats on updated/failed/skipped
        """
        stats = {"updated": 0, "failed": 0, "skipped": 0}
        
        try:
            # Get leads without embeddings
            leads = self.db.query(PotentialLead).filter(
                PotentialLead.embedding.is_(None)
            ).limit(batch_size).all()
            
            for lead in leads:
                success = await self.update_embedding(lead.id)
                if success:
                    stats["updated"] += 1
                else:
                    stats["failed"] += 1
            
            logger.info(
                f"Bulk embedding update: {stats['updated']} updated, "
                f"{stats['failed']} failed"
            )
            
        except Exception as e:
            logger.error(f"Bulk embedding update failed: {e}")
        
        return stats


# Convenience function for quick duplicate checks
async def is_duplicate(
    db: Session,
    hotel_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD
) -> bool:
    """
    Quick check if a lead is a duplicate
    
    Usage:
        if await is_duplicate(db, "Four Seasons Naples", "Naples", "FL"):
            print("Duplicate found!")
    """
    dedup = Deduplicator(db)
    is_dup, _ = await dedup.check_duplicate(hotel_name, city, state, threshold)
    return is_dup