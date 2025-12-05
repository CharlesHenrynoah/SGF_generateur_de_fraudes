"""Configuration settings for the Fraud Generator application."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8010
    api_workers: int = 4
    log_level: str = "INFO"
    
    # LLM Configuration (OpenAI)
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"  # Modèle économique
    llm_temperature: float = 0.7
    llm_batch_size: int = 100  # Plus petit pour OpenAI (coût)
    llm_max_tokens: int = 2000
    llm_use_openai: bool = True  # Utiliser OpenAI au lieu de modèle local
    
    # Database (Supabase)
    database_url: str = ""
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_service_key: str = ""
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    
    # S3/MinIO
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket_name: str = "synthetic-fraud"
    s3_region: str = "us-east-1"
    s3_use_ssl: bool = False
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_synthetic_tx: str = "synthetic_tx_events"
    kafka_topic_generate_requests: str = "synthetic_generate_requests"
    kafka_security_protocol: str = "PLAINTEXT"
    
    # Security
    jwt_secret_key: str = "your-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    encryption_key: str = "your-encryption-key-32-bytes"
    
    # Feature Store
    feature_store_url: str = "http://localhost:8080"
    
    # Rate Limiting
    rate_limit_per_minute: int = 100
    rate_limit_per_hour: int = 1000
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
