# Exemples d'utilisation

## Exemples de requêtes API

### 1. Preview rapide (10 transactions)

```bash
curl -X POST "http://localhost:8010/v1/generator/preview" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 10,
    "fraud_ratio": 0.2,
    "scenarios": ["card_testing"],
    "currency": "USD",
    "countries": ["US"]
  }'
```

### 2. Génération avec scénarios multiples

```bash
curl -X POST "http://localhost:8010/v1/generator/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 1000,
    "fraud_ratio": 0.15,
    "scenarios": [
      "card_testing",
      "account_takeover",
      "identity_theft"
    ],
    "currency": "EUR",
    "countries": ["FR", "DE", "IT"],
    "seed": 42
  }'
```

### 3. Génération avec plage de dates

```bash
curl -X POST "http://localhost:8010/v1/generator/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 5000,
    "fraud_ratio": 0.08,
    "scenarios": ["merchant_fraud", "phishing"],
    "currency": "GBP",
    "countries": ["GB"],
    "start_date": "2025-01-01T00:00:00Z",
    "end_date": "2025-01-31T23:59:59Z"
  }'
```

### 4. Génération massive (50k transactions)

```bash
curl -X POST "http://localhost:8010/v1/generator/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 50000,
    "fraud_ratio": 0.1,
    "scenarios": [],
    "currency": "USD",
    "countries": ["US", "CA", "MX"],
    "seed": 12345
  }'
```

## Exemples Python

### Utilisation avec requests

```python
import requests
import json
from datetime import datetime, timedelta

API_URL = "http://localhost:8010"

# Preview
response = requests.post(
    f"{API_URL}/v1/generator/preview",
    json={
        "count": 100,
        "fraud_ratio": 0.1,
        "scenarios": ["card_testing"],
        "currency": "USD",
        "countries": ["US"]
    }
)

data = response.json()
print(f"Generated {data['generated']} transactions")
print(f"Fraudulent: {data['fraudulent']}")
print(f"Legit: {data['legit']}")

# Afficher les transactions
for tx in data.get('transactions', [])[:5]:
    print(f"{tx['transaction_id']}: {tx['amount']} {tx['currency']} - Fraud: {tx['is_fraud']}")
```

### Utilisation avec httpx (async)

```python
import httpx
import asyncio

async def generate_transactions():
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(
            "http://localhost:8010/v1/generator/generate",
            json={
                "count": 10000,
                "fraud_ratio": 0.12,
                "scenarios": ["identity_theft", "money_laundering"],
                "currency": "EUR",
                "countries": ["FR", "DE"]
            }
        )
        return response.json()

result = asyncio.run(generate_transactions())
print(f"Batch ID: {result['batch_id']}")
print(f"S3 URI: {result['s3_uri']}")
```

## Exemples de requêtes SQL

### Statistiques par batch

```sql
SELECT 
    batch_id,
    created_at,
    generated_count,
    fraudulent_count,
    ROUND(100.0 * fraudulent_count / generated_count, 2) as fraud_percentage
FROM synthetic_batches
ORDER BY created_at DESC
LIMIT 10;
```

### Transactions frauduleuses récentes

```sql
SELECT 
    transaction_id,
    user_id,
    amount,
    currency,
    transaction_type,
    fraud_scenarios,
    explanation,
    timestamp
FROM synthetic_transactions
WHERE is_fraud = true
ORDER BY timestamp DESC
LIMIT 100;
```

### Distribution des montants par scénario

```sql
SELECT 
    unnest(fraud_scenarios) as scenario,
    COUNT(*) as count,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM synthetic_transactions
WHERE is_fraud = true
GROUP BY scenario
ORDER BY count DESC;
```

## Exemples de consommation Kafka

### Python consumer

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'synthetic_tx_events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    print(f"Transaction: {tx['transaction_id']}")
    print(f"Amount: {tx['amount']} {tx['currency']}")
    print(f"Fraud: {tx['is_fraud']}")
    if tx['is_fraud']:
        print(f"Scenarios: {tx['fraud_scenarios']}")
    print("-" * 50)
```

## Exemples de téléchargement depuis S3

### Python avec boto3

```python
import boto3
import pandas as pd
from io import BytesIO

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# Télécharger un fichier Parquet
bucket = 'synthetic-fraud'
key = 'synthetic/gen_2025_01_30_143022_20250130_143045.parquet'

obj = s3_client.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj['Body'].read()))

print(f"Loaded {len(df)} transactions")
print(df.head())
print(f"Fraud ratio: {df['is_fraud'].mean():.2%}")
```

## Tests de charge

### Avec Locust

```python
# locustfile.py
from locust import HttpUser, task, between

class FraudGeneratorUser(HttpUser):
    wait_time = between(1, 3)
    
    @task(3)
    def preview(self):
        self.client.post(
            "/v1/generator/preview",
            json={
                "count": 100,
                "fraud_ratio": 0.1,
                "scenarios": ["card_testing"],
                "currency": "USD",
                "countries": ["US"]
            }
        )
    
    @task(1)
    def generate(self):
        self.client.post(
            "/v1/generator/generate",
            json={
                "count": 1000,
                "fraud_ratio": 0.1,
                "scenarios": [],
                "currency": "USD",
                "countries": ["US"]
            },
            timeout=300
        )
```

Lancer avec:
```bash
locust -f locustfile.py --host=http://localhost:8010
```
