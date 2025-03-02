from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .core.config import settings
from .api.v1.api import api_router
from .services.visit_counter import visit_counter_service

app = FastAPI(title="Visit Counter Service")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    return {"status": "healthy"}

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources when the application shuts down"""
    await visit_counter_service.cleanup()

# Include API router
app.include_router(api_router, prefix=settings.API_PREFIX) 