"""Storage service for persisting synthetic transactions."""
import logging
import io
import json
from typing import List, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from app.config import settings
from app.models.transaction import Transaction
import hashlib

logger = logging.getLogger(__name__)


class StorageService:
    """Service for storing transactions in various backends."""
    
    def __init__(self):
        self.s3_client = None
        self.db_pool = None
        self._s3_initialized = False
        self._db_initialized = False
    
    async def initialize_s3(self):
        """Initialize S3/MinIO client."""
        if self._s3_initialized:
            return
        
        try:
            import boto3
            from botocore.config import Config
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=settings.s3_endpoint_url,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name=settings.s3_region,
                use_ssl=settings.s3_use_ssl,
                config=Config(signature_version='s3v4')
            )
            
            # Create bucket if it doesn't exist
            try:
                self.s3_client.head_bucket(Bucket=settings.s3_bucket_name)
            except:
                self.s3_client.create_bucket(Bucket=settings.s3_bucket_name)
                logger.info(f"Created bucket: {settings.s3_bucket_name}")
            
            self._s3_initialized = True
            logger.info("S3 client initialized")
        except Exception as e:
            logger.error(f"Error initializing S3: {e}")
            self._s3_initialized = False
    
    async def initialize_db(self):
        """Initialize database connection."""
        if self._db_initialized:
            return
        
        # Priorité à la connection string PostgreSQL directe
        if settings.database_url:
            try:
                from sqlalchemy import create_engine, text
                from sqlalchemy.pool import NullPool
                
                # Créer l'engine avec connection pooling
                self.db_engine = create_engine(
                    settings.database_url,
                    poolclass=NullPool,  # Pas de pooling pour Supabase
                    connect_args={
                        "connect_timeout": 10,
                        "sslmode": "require"  # Supabase nécessite SSL
                    }
                )
                
                # Tester la connexion
                with self.db_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self._db_initialized = True
                logger.info("Database engine initialized (PostgreSQL direct connection)")
                
                # Essayer aussi Supabase client si les clés sont disponibles
                if settings.supabase_url and (settings.supabase_service_key or settings.supabase_key):
                    try:
                        from supabase import create_client, Client
                        self.supabase: Client = create_client(
                            settings.supabase_url,
                            settings.supabase_service_key or settings.supabase_key
                        )
                        logger.info("Supabase client also initialized")
                    except Exception as supabase_error:
                        logger.warning(f"Supabase client initialization failed (using direct PostgreSQL): {supabase_error}")
                
            except Exception as db_error:
                logger.error(f"Error initializing database engine: {db_error}")
                # Fallback to Supabase client only
                if settings.supabase_url and (settings.supabase_service_key or settings.supabase_key):
                    try:
                        from supabase import create_client, Client
                        self.supabase: Client = create_client(
                            settings.supabase_url,
                            settings.supabase_service_key or settings.supabase_key
                        )
                        self._db_initialized = True
                        logger.info("Database client initialized (Supabase client only)")
                    except Exception as supabase_error:
                        logger.error(f"Error initializing Supabase client: {supabase_error}")
                        self._db_initialized = False
                else:
                    self._db_initialized = False
        elif settings.supabase_url and (settings.supabase_service_key or settings.supabase_key):
            # Utiliser uniquement le client Supabase
            try:
                from supabase import create_client, Client
                self.supabase: Client = create_client(
                    settings.supabase_url,
                    settings.supabase_service_key or settings.supabase_key
                )
                self._db_initialized = True
                logger.info("Database client initialized (Supabase client only)")
            except Exception as e:
                logger.error(f"Error initializing Supabase client: {e}")
                self._db_initialized = False
        else:
            logger.warning("No database configuration found (DATABASE_URL or SUPABASE_URL)")
            self._db_initialized = False
    
    async def export_to_s3(
        self,
        transactions: List[Transaction],
        batch_id: str,
        format: str = "parquet"
    ) -> str:
        """Export transactions to S3/MinIO."""
        await self.initialize_s3()
        
        if not self._s3_initialized:
            raise Exception("S3 client not initialized")
        
        # Convert transactions to DataFrame
        df = pd.DataFrame([tx.dict() for tx in transactions])
        
        # Convert timestamp to string for JSON compatibility
        if 'timestamp' in df.columns:
            df['timestamp'] = df['timestamp'].apply(lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x))
        
        # Generate file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if format == "parquet":
            filename = f"{batch_id}_{timestamp}.parquet"
            key = f"synthetic/{filename}"
            
            # Convert to Parquet
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                buffer,
                settings.s3_bucket_name,
                key,
                ExtraArgs={'ContentType': 'application/parquet'}
            )
        else:  # CSV
            filename = f"{batch_id}_{timestamp}.csv"
            key = f"synthetic/{filename}"
            
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=settings.s3_bucket_name,
                Key=key,
                Body=buffer.getvalue().encode('utf-8'),
                ContentType='text/csv'
            )
        
        # Generate S3 URI
        s3_uri = f"s3://{settings.s3_bucket_name}/{key}"
        logger.info(f"Exported {len(transactions)} transactions to {s3_uri}")
        
        return s3_uri
    
    async def save_to_database(
        self,
        transactions: List[Transaction],
        batch_id: str
    ) -> int:
        """Save transactions to database."""
        await self.initialize_db()
        
        if not self._db_initialized:
            logger.warning("Database not initialized, skipping save")
            return 0
        
        try:
            # Convert transactions to dict format
            tx_dicts = []
            for tx in transactions:
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
                tx_dicts.append(tx_dict)
            
            # Insert in batches
            batch_size = 1000
            inserted_count = 0
            
            for i in range(0, len(tx_dicts), batch_size):
                batch = tx_dicts[i:i + batch_size]
                try:
                    # Préférer Supabase client si disponible
                    if hasattr(self, 'supabase') and self.supabase:
                        result = self.supabase.table('synthetic_transactions').insert(batch).execute()
                        inserted_count += len(result.data) if result.data else len(batch)
                    elif hasattr(self, 'db_engine') and self.db_engine:
                        # Use SQLAlchemy avec insertion optimisée
                        from sqlalchemy import text
                        
                        # Insertion transaction par transaction avec gestion d'erreurs
                        with self.db_engine.connect() as conn:
                            for tx in batch:
                                try:
                                    conn.execute(text("""
                                        INSERT INTO synthetic_transactions 
                                        (transaction_id, user_id, merchant_id, amount, currency, 
                                         transaction_type, timestamp, country, city, ip_address, 
                                         device_id, card_last4, is_fraud, fraud_scenarios, 
                                         explanation, batch_id, metadata)
                                        VALUES (:transaction_id, :user_id, :merchant_id, :amount, 
                                                :currency, :transaction_type, :timestamp, :country, 
                                                :city, :ip_address, :device_id, :card_last4, 
                                                :is_fraud, :fraud_scenarios::text[], :explanation, 
                                                :batch_id, :metadata::jsonb)
                                        ON CONFLICT (transaction_id) DO NOTHING
                                    """), {
                                        'transaction_id': tx['transaction_id'],
                                        'user_id': tx['user_id'],
                                        'merchant_id': tx.get('merchant_id'),
                                        'amount': float(tx['amount']),
                                        'currency': tx['currency'],
                                        'transaction_type': tx['transaction_type'],
                                        'timestamp': tx['timestamp'],
                                        'country': tx['country'],
                                        'city': tx.get('city'),
                                        'ip_address': tx.get('ip_address'),
                                        'device_id': tx.get('device_id'),
                                        'card_last4': tx.get('card_last4'),
                                        'is_fraud': tx['is_fraud'],
                                        'fraud_scenarios': tx.get('fraud_scenarios', []),
                                        'explanation': tx.get('explanation'),
                                        'batch_id': batch_id,
                                        'metadata': json.dumps(tx.get('metadata', {}))
                                    })
                                    inserted_count += 1
                                except Exception as tx_error:
                                    logger.warning(f"Error inserting transaction {tx.get('transaction_id', 'unknown')}: {tx_error}")
                                    continue
                            
                            conn.commit()
                    else:
                        logger.error("No database connection available")
                        return 0
                        
                except Exception as e:
                    logger.error(f"Error inserting batch {i}: {e}", exc_info=True)
                    continue
            
            logger.info(f"Inserted {inserted_count} transactions into database")
            return inserted_count
        except Exception as e:
            logger.error(f"Error saving to database: {e}", exc_info=True)
            return 0
    
    def compute_sha256_hash(self, transactions: List[Transaction]) -> str:
        """Compute SHA256 hash of transactions for audit."""
        # Sort transactions by ID for consistent hashing
        sorted_txs = sorted(transactions, key=lambda x: x.transaction_id)
        tx_data = json.dumps([tx.dict() for tx in sorted_txs], sort_keys=True, default=str)
        return hashlib.sha256(tx_data.encode()).hexdigest()


# Global instance
storage_service = StorageService()
