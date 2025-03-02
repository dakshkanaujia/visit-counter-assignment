from typing import Dict, DefaultDict
import logging
import time
import asyncio
from collections import defaultdict
from ..core.redis_manager import redis_manager
from ..schemas.counter import VisitCount

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis backend, in-memory cache, and write batching"""
        # In-memory cache: page_id -> (count, expiration_timestamp)
        self.cache: Dict[str, tuple[int, float]] = {}
        self.cache_ttl = 5.0  # 5 seconds TTL
        
        # Write batching buffer: page_id -> pending_count
        self.write_buffer: DefaultDict[str, int] = defaultdict(int)
        self.last_flush_time = time.time()
        self.flush_interval = 30.0  # 30 seconds between flushes
        
        # Start background flush task
        self.flush_task = asyncio.create_task(self._periodic_flush())
        
        logger.info("VisitCounterService initialized with write batching (flush interval: 30s)")

    async def _periodic_flush(self):
        """Background task to periodically flush the write buffer to Redis"""
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush_buffer()
            except asyncio.CancelledError:
                # Task was cancelled, exit gracefully
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {str(e)}")
    
    async def flush_buffer(self):
        """Flush all pending writes in the buffer to Redis"""
        if not self.write_buffer:
            logger.info("Write buffer is empty, nothing to flush")
            return
        
        logger.info(f"Flushing write buffer with {len(self.write_buffer)} entries")
        
        # Create a copy of the buffer to work with
        buffer_copy = dict(self.write_buffer)
        
        # Clear the original buffer
        self.write_buffer.clear()
        
        # Update Redis for each page in the buffer
        for page_id, count in buffer_copy.items():
            if count > 0:
                redis_key = f"visit_counter:{page_id}"
                try:
                    # Increment Redis by the accumulated count
                    await redis_manager.increment(redis_key, count)
                    logger.info(f"Flushed {count} visits for {page_id} to Redis")
                    
                    # Invalidate cache for this page
                    if page_id in self.cache:
                        del self.cache[page_id]
                except Exception as e:
                    # If there's an error, put the count back in the buffer
                    logger.error(f"Error flushing {page_id} to Redis: {str(e)}")
                    self.write_buffer[page_id] += count
        
        self.last_flush_time = time.time()
        logger.info("Buffer flush completed")

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in the write buffer.
        
        Args:
            page_id: Unique identifier for the page.
        """
        # Add to the write buffer instead of writing directly to Redis
        self.write_buffer[page_id] += 1
        logger.info(f"Incremented visit count for {page_id} in write buffer (pending: {self.write_buffer[page_id]})")
        
        # Invalidate the cache entry for this page
        if page_id in self.cache:
            del self.cache[page_id]

    async def get_visit_count(self, page_id: str) -> VisitCount:
        """
        Get current visit count for a page, combining Redis count with pending writes.
        
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
                logger.info(f"Cache hit for {page_id}: {count}")
                return VisitCount(visits=count, served_via="in_memory")
            
            # If expired, remove it
            del self.cache[page_id]
        
        # If it's been a long time since the last flush, flush now
        time_since_flush = current_time - self.last_flush_time
        if time_since_flush > self.flush_interval:
            logger.info(f"Flush interval exceeded ({time_since_flush:.1f}s), flushing buffer before read")
            await self.flush_buffer()
        
        # Get the persisted count from Redis
        redis_key = f"visit_counter:{page_id}"
        redis_count = await redis_manager.get(redis_key)
        logger.info(f"Retrieved count {redis_count} for {page_id} from Redis")
        
        # Add any pending writes from the buffer
        pending_count = self.write_buffer.get(page_id, 0)
        total_count = redis_count + pending_count
        
        if pending_count > 0:
            logger.info(f"Added {pending_count} pending writes for {page_id}, total: {total_count}")
        
        # Update the cache with the combined count
        self.cache[page_id] = (total_count, current_time + self.cache_ttl)
        
        # Return the total count with Redis as the source
        return VisitCount(visits=total_count, served_via="redis")

    async def cleanup(self):
        """Clean up resources when shutting down"""
        # Cancel the periodic flush task
        if hasattr(self, 'flush_task'):
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining writes
        await self.flush_buffer()

# Create a singleton instance
visit_counter_service = VisitCounterService()
