"""Health check endpoints."""
from fastapi import APIRouter
from datetime import datetime

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "fraud-generator-api"
    }


@router.get("/health/ready")
async def readiness_check():
    """Readiness check endpoint."""
    # Check dependencies
    checks = {
        "api": True,
        "llm_service": False,
        "storage": False,
        "kafka": False
    }
    
    try:
        from app.services.llm_service import llm_service
        checks["llm_service"] = llm_service._initialized
    except:
        pass
    
    try:
        from app.services.storage_service import storage_service
        checks["storage"] = storage_service._s3_initialized or storage_service._db_initialized
    except:
        pass
    
    try:
        from app.services.kafka_service import kafka_service
        checks["kafka"] = kafka_service._initialized
    except:
        pass
    
    ready = all(checks.values())
    
    return {
        "status": "ready" if ready else "not_ready",
        "checks": checks,
        "timestamp": datetime.now().isoformat()
    }
