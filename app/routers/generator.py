"""Generator endpoints for fraud transaction generation."""
import time
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.models.transaction import GenerateRequest, GenerateResponse
from app.models.batch import BatchCreate
from app.services.llm_service import llm_service
from app.services.validation_service import validation_service
from app.services.storage_service import storage_service
from app.services.kafka_service import kafka_service

logger = logging.getLogger(__name__)
router = APIRouter()


def generate_batch_id() -> str:
    """Generate a unique batch ID."""
    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    return f"gen_{timestamp}"


@router.post("/preview", response_model=GenerateResponse)
async def preview_transactions(request: GenerateRequest):
    """
    Preview a small sample of generated transactions (max 100).
    Fast endpoint for testing and validation.
    """
    start_time = time.time()
    
    # Limit preview to 100 transactions
    if request.count > 100:
        request.count = 100
    
    try:
        # Generate transactions
        transactions = await llm_service.generate_transactions(request)
        
        # Validate schema
        transactions = validation_service.validate_schema(transactions)
        
        # Validate business rules
        transactions = validation_service.validate_business_rules(transactions)
        
        # Deduplicate
        transactions, _ = validation_service.deduplicate(transactions)
        
        # Count fraud vs legit
        fraudulent_count = sum(1 for tx in transactions if tx.is_fraud)
        legit_count = len(transactions) - fraudulent_count
        
        batch_id = generate_batch_id()
        
        # Add batch_id to transactions
        for tx in transactions:
            tx.batch_id = batch_id
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        return GenerateResponse(
            batch_id=batch_id,
            generated=len(transactions),
            fraudulent=fraudulent_count,
            legit=legit_count,
            latency_ms=latency_ms,
            transactions=transactions[:100]  # Return first 100 for preview
        )
    except Exception as e:
        logger.error(f"Error in preview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate", response_model=GenerateResponse)
async def generate_transactions(
    request: GenerateRequest,
    background_tasks: BackgroundTasks
):
    """
    Generate a full batch of synthetic transactions.
    Transactions are persisted to database and S3, and optionally published to Kafka.
    """
    start_time = time.time()
    batch_id = generate_batch_id()
    
    try:
        # Validate request
        if request.count > 100000:
            raise HTTPException(
                status_code=400,
                detail="Maximum count is 100,000 transactions"
            )
        
        logger.info(f"Starting generation for batch {batch_id}: {request.count} transactions")
        
        # Generate transactions
        transactions = await llm_service.generate_transactions(request)
        logger.info(f"Generated {len(transactions)} raw transactions")
        
        # Validate schema
        transactions = validation_service.validate_schema(transactions)
        logger.info(f"After schema validation: {len(transactions)} transactions")
        
        # Deduplicate
        transactions, _ = validation_service.deduplicate(transactions)
        logger.info(f"After deduplication: {len(transactions)} transactions")
        
        # Validate business rules
        transactions = validation_service.validate_business_rules(transactions)
        logger.info(f"After business rules: {len(transactions)} transactions")
        
        # Statistical validation
        stats = validation_service.validate_distribution(transactions)
        logger.info(f"Distribution stats: {stats}")
        
        # Add batch_id to transactions
        for tx in transactions:
            tx.batch_id = batch_id
        
        # Count fraud vs legit
        fraudulent_count = sum(1 for tx in transactions if tx.is_fraud)
        legit_count = len(transactions) - fraudulent_count
        
        # Export to S3
        s3_uri = None
        try:
            s3_uri = await storage_service.export_to_s3(
                transactions,
                batch_id,
                format="parquet"
            )
            logger.info(f"Exported to S3: {s3_uri}")
        except Exception as e:
            logger.error(f"Error exporting to S3: {e}")
        
        # Save to database (async in background)
        background_tasks.add_task(
            storage_service.save_to_database,
            transactions,
            batch_id
        )
        
        # Publish to Kafka (async in background)
        background_tasks.add_task(
            kafka_service.publish_transactions,
            transactions,
            batch_id
        )
        
        # Save batch metadata
        batch_create = BatchCreate(
            batch_id=batch_id,
            seed=request.seed,
            scenarios=request.scenarios,
            count=request.count,
            fraud_ratio=request.fraud_ratio,
            generated_count=len(transactions),
            fraudulent_count=fraudulent_count,
            legit_count=legit_count,
            s3_uri=s3_uri,
            metadata={
                "stats": stats,
                "latency_ms": int((time.time() - start_time) * 1000)
            }
        )
        
        # Save batch record (async)
        background_tasks.add_task(
            save_batch_record,
            batch_create
        )
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        logger.info(
            f"Generation completed for batch {batch_id}: "
            f"{len(transactions)} transactions in {latency_ms}ms"
        )
        
        return GenerateResponse(
            batch_id=batch_id,
            generated=len(transactions),
            fraudulent=fraudulent_count,
            legit=legit_count,
            s3_uri=s3_uri,
            latency_ms=latency_ms
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in generate: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def save_batch_record(batch_create: BatchCreate):
    """Save batch record to database."""
    try:
        await storage_service.initialize_db()
        if storage_service._db_initialized:
            if hasattr(storage_service, 'supabase'):
                batch_dict = batch_create.dict()
                batch_dict['created_at'] = datetime.now().isoformat()
                storage_service.supabase.table('synthetic_batches').insert(batch_dict).execute()
            else:
                # Use SQLAlchemy
                from sqlalchemy import text
                with storage_service.db_engine.connect() as conn:
                    conn.execute(text(
                        f"INSERT INTO synthetic_batches "
                        f"(batch_id, created_at, seed, count, fraud_ratio, "
                        f"generated_count, fraudulent_count, legit_count, s3_uri) "
                        f"VALUES "
                        f"('{batch_create.batch_id}', NOW(), {batch_create.seed or 'NULL'}, "
                        f"{batch_create.count}, {batch_create.fraud_ratio}, "
                        f"{batch_create.generated_count}, {batch_create.fraudulent_count}, "
                        f"{batch_create.legit_count}, "
                        f"{'NULL' if not batch_create.s3_uri else f\"'{batch_create.s3_uri}'\"})"
                    ))
                    conn.commit()
    except Exception as e:
        logger.error(f"Error saving batch record: {e}")
