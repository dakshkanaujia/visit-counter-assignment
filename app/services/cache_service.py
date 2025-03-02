import time
import logging
from typing import Dict, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheService:
    def __init__(self, ttl_seconds: int = 5):
        """
        Initialize the cache service with a specified TTL
        
        Args:
            ttl_seconds: Time-to-live in seconds for cached items
        """
        self.cache: Dict[str, Tuple[Any, float]] = {}  # key -> (value, expiration_timestamp)
        self.ttl_seconds = ttl_seconds
        print(f"Cache service initialized with TTL of {ttl_seconds} seconds")
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a value in the cache with the configured TTL
        
        Args:
            key: Cache key
            value: Value to cache
        """
        expiration = time.time() + self.ttl_seconds
        self.cache[key] = (value, expiration)
        print(f"Cache SET: {key} = {value} (expires in {self.ttl_seconds} seconds)")
    
    def get(self, key: str) -> Tuple[Any, bool]:
        """
        Get a value from the cache if it exists and hasn't expired
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Tuple of (value, hit_status) where hit_status is True if cache hit, False if miss
        """
        if key not in self.cache:
            print(f"Cache MISS (key not found): {key}")
            return None, False
        
        value, expiration = self.cache[key]
        
        # Check if the cached value has expired
        if time.time() > expiration:
            # Remove expired item
            del self.cache[key]
            print(f"Cache MISS (expired): {key}")
            return None, False
        
        print(f"Cache HIT: {key} = {value}")
        return value, True
    
    def invalidate(self, key: str) -> None:
        """
        Invalidate a specific cache entry
        
        Args:
            key: Cache key to invalidate
        """
        if key in self.cache:
            del self.cache[key]
            print(f"Cache INVALIDATED: {key}")
    
    def clear(self) -> None:
        """Clear all cache entries"""
        self.cache.clear()
        print("Cache CLEARED")

# Create a singleton instance
cache_service = CacheService() 