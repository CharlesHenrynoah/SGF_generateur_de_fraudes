-- Database schema for Fraud Generator

-- Synthetic transactions table
CREATE TABLE IF NOT EXISTS synthetic_transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    merchant_id VARCHAR(255),
    amount DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    transaction_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    country VARCHAR(3) NOT NULL,
    city VARCHAR(255),
    ip_address VARCHAR(45),
    device_id VARCHAR(255),
    card_last4 VARCHAR(4),
    is_fraud BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_scenarios TEXT[],  -- Array of fraud scenario strings
    explanation TEXT,
    batch_id VARCHAR(255),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_transaction_id ON synthetic_transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_user_id ON synthetic_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_batch_id ON synthetic_transactions(batch_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_timestamp ON synthetic_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_is_fraud ON synthetic_transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_synthetic_tx_created_at ON synthetic_transactions(created_at);

-- Synthetic batches table (WORM - Write Once Read Many)
CREATE TABLE IF NOT EXISTS synthetic_batches (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    seed INTEGER,
    scenarios TEXT[],  -- Array of fraud scenario strings
    count INTEGER NOT NULL,
    fraud_ratio DECIMAL(5, 4) NOT NULL,
    generated_count INTEGER NOT NULL,
    fraudulent_count INTEGER NOT NULL,
    legit_count INTEGER NOT NULL,
    s3_uri TEXT,
    sha256_hash VARCHAR(64),  -- SHA256 hash for audit
    metadata JSONB DEFAULT '{}'
);

-- Indexes for batches
CREATE INDEX IF NOT EXISTS idx_synthetic_batches_batch_id ON synthetic_batches(batch_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_batches_created_at ON synthetic_batches(created_at);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_synthetic_transactions_updated_at
    BEFORE UPDATE ON synthetic_transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- View for fraud statistics
CREATE OR REPLACE VIEW fraud_statistics AS
SELECT
    DATE_TRUNC('day', timestamp) as date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraudulent_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as fraud_percentage,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount
FROM synthetic_transactions
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY date DESC;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
