"""Main FastAPI application for Fraud Generator."""
import time
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.models.transaction import GenerateRequest, GenerateResponse
from app.routers import generator, health
from app.services.llm_service import llm_service
from app.services.validation_service import validation_service
from app.services.storage_service import storage_service
from app.services.kafka_service import kafka_service

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    # Startup
    logger.info("Starting Fraud Generator API...")
    await llm_service.initialize()
    await storage_service.initialize_s3()
    await storage_service.initialize_db()
    await kafka_service.initialize()
    logger.info("Fraud Generator API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Fraud Generator API...")


# Create FastAPI app
app = FastAPI(
    title="Fraud Generator API",
    description="API for generating synthetic fraud transactions",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(generator.router, prefix="/v1/generator", tags=["Generator"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Fraud Generator API",
        "version": "1.0.0",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )
