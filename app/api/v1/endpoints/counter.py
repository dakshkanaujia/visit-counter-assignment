from fastapi import APIRouter, HTTPException
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()
counter_service = VisitCounterService()

@router.post("/visit/{page_id}")
async def record_visit(page_id: str):
    """Record a visit for a website"""
    try:
        await counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(page_id: str):
    """Get visit count for a website"""
    try:
        result = await counter_service.get_visit_count(page_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/test-cache/{page_id}")
async def test_cache(page_id: str):
    """Test endpoint to check the caching mechanism"""
    # First request - should come from Redis
    first_result = await counter_service.get_visit_count(page_id)
    
    # Second request - should come from cache
    second_result = await counter_service.get_visit_count(page_id)
    
    return {
        "first_request": {
            "visits": first_result.visits,
            "served_via": first_result.served_via
        },
        "second_request": {
            "visits": second_result.visits,
            "served_via": second_result.served_via
        }
    }