from typing import Dict
import logging
import time
from ..core.redis_manager import redis_manager
from ..schemas.counter import VisitCount

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis backend and in-memory cache"""
        # In-memory cache: page_id -> (count, expiration_timestamp)
        self.cache: Dict[str, tuple[int, float]] = {}
        self.cache_ttl = 5.0  # 5 seconds TTL
        print(f"VisitCounterService initialized with cache TTL of {self.cache_ttl} seconds")

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in Redis.
        
        Args:
            page_id: Unique identifier for the page.
        """
        # Create a Redis key for this page
        redis_key = f"visit_counter:{page_id}"
        
        # Always write directly to Redis
        new_count = await redis_manager.increment(redis_key)
        print(f"Incremented visit count for {page_id} to {new_count}")
        
        # Invalidate the cache entry for this page
        if page_id in self.cache:
            del self.cache[page_id]

    async def get_visit_count(self, page_id: str) -> VisitCount:
        """
        Get current visit count for a page, using cache if available.
        
        Args:
            page_id: Unique identifier for the page.
            
        Returns:
            VisitCount: Current visit count with source information.
        """
        current_time = time.time()
        
        # Check if we have a valid cache entry
        if page_id in self.cache:
            count, expiration = self.cache[page_id]
            
            # If the cache entry is still valid
            if current_time < expiration:
                return VisitCount(visits=count, served_via="in_memory")
            
            # If expired, remove it
            del self.cache[page_id]
        
        # Cache miss or expired - get from Redis
        redis_key = f"visit_counter:{page_id}"
        count = await redis_manager.get(redis_key)
        
        # Update the cache
        self.cache[page_id] = (count, current_time + self.cache_ttl)
        
        # Return the count with Redis as the source
        return VisitCount(visits=count, served_via="redis")

visit_counter_service = VisitCounterService()
