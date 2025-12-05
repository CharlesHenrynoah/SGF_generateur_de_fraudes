"""Kafka service for publishing synthetic transaction events."""
import json
import logging
from typing import List
from app.config import settings
from app.models.transaction import Transaction

logger = logging.getLogger(__name__)


class KafkaService:
    """Service for publishing events to Kafka."""
    
    def __init__(self):
        self.producer = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize Kafka producer."""
        if self._initialized:
            return
        
        try:
            from kafka import KafkaProducer
            
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                security_protocol=settings.kafka_security_protocol,
                acks='all',
                retries=3
            )
            self._initialized = True
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            self._initialized = False
    
    async def publish_transactions(
        self,
        transactions: List[Transaction],
        batch_id: str
    ) -> int:
        """Publish transactions to Kafka topic."""
        await self.initialize()
        
        if not self._initialized:
            logger.warning("Kafka not initialized, skipping publish")
            return 0
        
        published_count = 0
        
        try:
            for tx in transactions:
                # Convert transaction to dict
                tx_dict = tx.dict()
                # Convert datetime to ISO string
                if 'timestamp' in tx_dict:
                    tx_dict['timestamp'] = tx_dict['timestamp'].isoformat()
                # Convert enums to strings
                if 'transaction_type' in tx_dict:
                    tx_dict['transaction_type'] = tx_dict['transaction_type'].value
                if 'fraud_scenarios' in tx_dict:
                    tx_dict['fraud_scenarios'] = [s.value for s in tx_dict['fraud_scenarios']]
                tx_dict['batch_id'] = batch_id
                
                # Publish to Kafka
                future = self.producer.send(
                    settings.kafka_topic_synthetic_tx,
                    key=tx.transaction_id,
                    value=tx_dict
                )
                
                # Wait for send to complete (optional, can be async)
                try:
                    future.get(timeout=1)
                    published_count += 1
                except Exception as e:
                    logger.warning(f"Error publishing transaction {tx.transaction_id}: {e}")
            
            # Flush remaining messages
            self.producer.flush()
            
            logger.info(f"Published {published_count} transactions to Kafka")
            return published_count
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            return 0
    
    async def publish_generation_request(self, request_data: dict):
        """Publish generation request to Kafka for audit."""
        await self.initialize()
        
        if not self._initialized:
            return
        
        try:
            self.producer.send(
                settings.kafka_topic_generate_requests,
                key=request_data.get('batch_id', 'unknown'),
                value=request_data
            )
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error publishing generation request: {e}")


# Global instance
kafka_service = KafkaService()
