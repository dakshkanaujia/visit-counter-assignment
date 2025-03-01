import redis
from typing import Dict, List, Optional, Any
from .consistent_hash import ConsistentHash
from .config import settings

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from comma-separated string
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
        # Initialize connection pools for each Redis node
        for node in redis_nodes:
            self.connection_pools[node] = redis.ConnectionPool.from_url(
                url=node,
                password=settings.REDIS_PASSWORD,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.redis_clients[node] = redis.Redis(connection_pool=self.connection_pools[node])

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        node = self.consistent_hash.get_node(key)
        if node not in self.redis_clients:
            raise Exception(f"No Redis client available for node {node}")
        return self.redis_clients[node]

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        try:
            redis_client = await self.get_connection(key)
            return redis_client.incrby(key, amount)
        except Exception as e:
            # In a production system, you might want to implement retries here
            raise Exception(f"Failed to increment key {key}: {str(e)}")

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Value of the key or None if not found
        """
        try:
            redis_client = await self.get_connection(key)
            value = redis_client.get(key)
            return int(value) if value is not None else 0
        except Exception as e:
            # In a production system, you might want to implement retries here
            raise Exception(f"Failed to get key {key}: {str(e)}")

# Create a singleton instance
redis_manager = RedisManager()
