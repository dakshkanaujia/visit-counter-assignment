import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.hash_ring = {}  # Maps hash values to node names
        self.sorted_keys = []  # Sorted list of hash values
        
        # Add each node to the hash ring
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str) -> None:
        """
        Add a new node to the hash ring
        
        Args:
            node: Node identifier to add
        """
        # Create virtual nodes for the physical node
        for i in range(self.virtual_nodes):
            # Create a unique key for each virtual node
            virtual_node_key = f"{node}:{i}"
            # Calculate the hash of the virtual node
            hash_value = self._hash(virtual_node_key)
            # Map the hash to the physical node
            self.hash_ring[hash_value] = node
            # Add the hash to the sorted keys
            self.sorted_keys.append(hash_value)
        
        # Keep the keys sorted for binary search
        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring
        
        Args:
            node: Node identifier to remove
        """
        # Find all virtual nodes for this physical node
        keys_to_remove = []
        for hash_value, mapped_node in self.hash_ring.items():
            if mapped_node == node:
                keys_to_remove.append(hash_value)
        
        # Remove the virtual nodes
        for hash_value in keys_to_remove:
            del self.hash_ring[hash_value]
            self.sorted_keys.remove(hash_value)

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
        if not self.hash_ring:
            raise Exception("Hash ring is empty")
        
        # Calculate the hash of the key
        hash_value = self._hash(key)
        
        # Find the first node in the ring that comes after the key's hash
        index = bisect(self.sorted_keys, hash_value) % len(self.sorted_keys)
        
        # Return the node at that position
        return self.hash_ring[self.sorted_keys[index]]
    
    def _hash(self, key: str) -> int:
        """
        Calculate the hash of a key
        
        Args:
            key: The key to hash
            
        Returns:
            The hash value as an integer
        """
        # Use MD5 for consistent hashing
        md5 = hashlib.md5()
        md5.update(key.encode('utf-8'))
        return int(md5.hexdigest(), 16)
    