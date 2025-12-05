import asyncio
import json
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import sys
import os

# Ajouter le dossier parent au path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.dataset_service import dataset_service
from app.services.llm_service import llm_service
from app.config import settings
from app.models.transaction import GenerateRequest, FraudScenario

# Configuration pour la sortie JSON colorée
try:
    from rich.console import Console
    from rich.syntax import Syntax
    console = Console()
except ImportError:
    console = None

async def generate_complex_transaction(is_fraud: bool):
    """
    Génère une transaction complexe basée sur un exemple réel du CSV (RAG).
    """
    # 1. Récupérer une "graine" du dataset réel
    if is_fraud:
        seed_data = dataset_service.get_random_fraud()
    else:
        seed_data = dataset_service.get_random_legit()

    # 2. Préparer le prompt avec le format STRICT demandé
    prompt = f"""
    You are a financial fraud simulation engine. 
    Generate a JSON object representing a transaction analysis response.
    
    BASE DATA (Use this as context but enrich it significantly):
    - Amount: {seed_data['amount']}
    - Type: {seed_data['transaction_type']}
    - Merchant Category: {seed_data['merchant_category']}
    - Country: {seed_data['country']}
    - Hour: {seed_data['hour']}
    - Is Fraud: {is_fraud}
    
    OUTPUT FORMAT (Strict JSON):
    {{
      "decision_id": "dec_...",
      "decision": "{'DENY' if is_fraud else 'ALLOW'}", 
      "score": {round(random.uniform(0.85, 0.99) if is_fraud else random.uniform(0.01, 0.15), 2)},
      "rule_hits": ["list", "of", "rules"],
      "reasons": ["Human readable reasons"],
      "latency_ms": {random.randint(20, 100)},
      "model_version": "gbdt_v1.2.3",
      "sla": {{ "p95_budget_ms": 100 }},
      "transaction_context": {{
        "tenant_id": "bank-fr-001",
        "idempotency_key": "tx-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}",
        "event": {{
          "type": "card_payment",
          "id": "evt_{str(uuid.uuid4())[:12]}",
          "ts": "{datetime.now().isoformat()}Z",
          "amount": {seed_data['amount']},
          "currency": "EUR",
          "merchant": {{
            "id": "merch_{str(uuid.uuid4())[:8]}",
            "name": "Generate a realistic name based on category '{seed_data['merchant_category']}'",
            "mcc": "Generate valid 4-digit MCC",
            "country": "{seed_data['country']}"
          }},
          "card": {{
            "card_id": "card_tok_{str(uuid.uuid4())[:8]}",
            "type": "{'virtual' if seed_data['transaction_type'] == 'Online' else 'physical'}",
            "user_id": "user_{seed_data['user_id']}"
          }},
          "context": {{
            "ip": "Generate realistic IP",
            "geo": "{seed_data['country']}",
            "device_id": "dev_{str(uuid.uuid4())[:8]}",
            "channel": "{'web' if seed_data['transaction_type'] == 'Online' else 'pos'}"
          }},
          "security": {{
            "auth_method": "3ds/pin/none",
            "aml_flag": {str(is_fraud).lower()}
          }},
          "kyc": {{
            "status": "verified",
            "level": "standard",
            "confidence": 0.95
          }}
        }}
      }}
    }}
    
    INSTRUCTIONS:
    - If decision is DENY, provide strong reasons in 'reasons' and 'rule_hits' related to fraud.
    - If decision is ALLOW, reasons should explain why it's safe.
    - Make merchant name realistic for the category '{seed_data['merchant_category']}' in country '{seed_data['country']}'.
    - Ensure JSON is valid.
    """

    # 3. Appel OpenAI
    try:
        response = await llm_service.openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": "You are a data generator. Output ONLY valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"Error generating: {e}")
        return None

async def main():
    # 1. Init services
    dataset_service.initialize()  # Synchronous load in this specific implementation
    await llm_service.initialize()
    
    print("🚀 Génération de transactions basées sur vos données CSV (RAG)...")
    
    # 2. Générer quelques exemples
    # Exemple 1: Transaction Légitime (ALLOW)
    print("\n--- Exemple 1: Transaction Légitime (basée sur CSV) ---")
    legit_tx = await generate_complex_transaction(is_fraud=False)
    if console:
        console.print(Syntax(json.dumps(legit_tx, indent=2), "json", theme="monokai", word_wrap=True))
    else:
        print(json.dumps(legit_tx, indent=2))

    # Exemple 2: Transaction Frauduleuse (DENY)
    print("\n--- Exemple 2: Transaction Frauduleuse (basée sur CSV) ---")
    fraud_tx = await generate_complex_transaction(is_fraud=True)
    if console:
        console.print(Syntax(json.dumps(fraud_tx, indent=2), "json", theme="monokai", word_wrap=True))
    else:
        print(json.dumps(fraud_tx, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
