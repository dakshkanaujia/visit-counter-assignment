from typing import Dict
from ..core.redis_manager import redis_manager
from ..schemas.counter import VisitCount

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis backend"""
        # No need for in-memory storage anymore
        pass

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in Redis.
        
        Args:
            page_id: Unique identifier for the page.
        """
        # Create a Redis key for this page
        redis_key = f"visit_counter:{page_id}"
        await redis_manager.increment(redis_key)

    async def get_visit_count(self, page_id: str) -> VisitCount:
        """
        Get current visit count for a page from Redis.
        
        Args:
            page_id: Unique identifier for the page.
            
        Returns:
            VisitCount: Current visit count with source information.
        """
        # Create a Redis key for this page
        redis_key = f"visit_counter:{page_id}"
        count = await redis_manager.get(redis_key)
        
        # Return the count with source information
        return VisitCount(visits=count, served_via="redis")

visit_counter_service = VisitCounterService()
