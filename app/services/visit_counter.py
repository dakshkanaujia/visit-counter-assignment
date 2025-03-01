from typing import Dict

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with an in-memory dictionary"""
        self.visit_counts: Dict[str, int] = {}  # In-memory storage

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page.
        
        Args:
            page_id: Unique identifier for the page.
        """
        self.visit_counts[page_id] = self.visit_counts.get(page_id, 0) + 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page.
        
        Args:
            page_id: Unique identifier for the page.
            
        Returns:
            int: Current visit count.
        """
        return self.visit_counts.get(page_id, 0)

visit_counter_service = VisitCounterService()
