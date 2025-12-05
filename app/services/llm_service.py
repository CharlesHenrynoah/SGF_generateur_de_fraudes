"""LLM service for generating synthetic fraud transactions using OpenAI API."""
import asyncio
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import random
import numpy as np
from app.config import settings
from app.models.transaction import Transaction, TransactionType, FraudScenario, GenerateRequest

logger = logging.getLogger(__name__)


class LLMService:
    """Service for generating synthetic transactions using OpenAI API."""
    
    def __init__(self):
        self.openai_client = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the OpenAI client."""
        if self._initialized:
            return
        
        try:
            if settings.llm_use_openai and settings.openai_api_key:
                from openai import AsyncOpenAI
                
                self.openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
                self._initialized = True
                logger.info(f"OpenAI client initialized with model: {settings.openai_model}")
            else:
                logger.warning("OpenAI API key not configured, using rule-based generation")
                self._initialized = False
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {e}")
            self._initialized = False
    
    async def generate_transactions(
        self,
        request: GenerateRequest,
        batch_size: Optional[int] = None
    ) -> List[Transaction]:
        """Generate synthetic transactions based on request."""
        if not self._initialized:
            await self.initialize()
        
        if batch_size is None:
            batch_size = settings.llm_batch_size
        
        transactions = []
        num_fraud = int(request.count * request.fraud_ratio)
        num_legit = request.count - num_fraud
        
        # Set seed for reproducibility
        if request.seed is not None:
            random.seed(request.seed)
            np.random.seed(request.seed)
        
        # Generate legitimate transactions
        logger.info(f"Generating {num_legit} legitimate transactions")
        legit_txs = await self._generate_batch(
            count=num_legit,
            is_fraud=False,
            scenarios=[],
            request=request,
            batch_size=batch_size
        )
        transactions.extend(legit_txs)
        
        # Generate fraudulent transactions
        logger.info(f"Generating {num_fraud} fraudulent transactions")
        fraud_txs = await self._generate_batch(
            count=num_fraud,
            is_fraud=True,
            scenarios=request.scenarios if request.scenarios else list(FraudScenario),
            request=request,
            batch_size=batch_size
        )
        transactions.extend(fraud_txs)
        
        # Shuffle transactions
        random.shuffle(transactions)
        
        return transactions
    
    async def _generate_batch(
        self,
        count: int,
        is_fraud: bool,
        scenarios: List[FraudScenario],
        request: GenerateRequest,
        batch_size: int
    ) -> List[Transaction]:
        """Generate a batch of transactions."""
        transactions = []
        
        # Generate in batches to optimize OpenAI API calls
        for i in range(0, count, batch_size):
            batch_count = min(batch_size, count - i)
            
            if self._initialized and settings.llm_use_openai:
                # Use OpenAI API
                batch_txs = await self._generate_with_openai(
                    count=batch_count,
                    is_fraud=is_fraud,
                    scenarios=scenarios,
                    request=request,
                    batch_offset=i
                )
            else:
                # Fallback to rule-based generation
                batch_txs = await self._generate_rule_based_batch(
                    count=batch_count,
                    is_fraud=is_fraud,
                    scenarios=scenarios,
                    request=request,
                    batch_offset=i
                )
            
            transactions.extend(batch_txs)
            
            # Small delay to respect rate limits
            if i + batch_size < count:
                await asyncio.sleep(0.1)
        
        return transactions
    
    async def _generate_with_openai(
        self,
        count: int,
        is_fraud: bool,
        scenarios: List[FraudScenario],
        request: GenerateRequest,
        batch_offset: int
    ) -> List[Transaction]:
        """Generate transactions using OpenAI API."""
        try:
            # Build prompt
            prompt = self._build_prompt(count, is_fraud, scenarios, request)
            
            # Call OpenAI API
            try:
                response = await self.openai_client.chat.completions.create(
                    model=settings.openai_model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a data generator that creates realistic financial transaction data in JSON format. Always return valid JSON with a 'transactions' array."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    temperature=settings.llm_temperature,
                    max_tokens=settings.llm_max_tokens,
                    response_format={"type": "json_object"}
                )
                
                # Parse response
                content = response.choices[0].message.content
                data = json.loads(content)
                
                # Convert to Transaction objects
                transactions = []
                tx_list = data.get("transactions", [])
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing OpenAI JSON response: {e}")
                tx_list = []
            except Exception as e:
                logger.error(f"Error calling OpenAI API: {e}")
                raise
            
            for idx, tx_data in enumerate(tx_list):
                try:
                    tx = self._parse_transaction(tx_data, request, batch_offset + idx)
                    transactions.append(tx)
                except Exception as e:
                    logger.warning(f"Error parsing transaction {idx}: {e}")
                    # Generate fallback transaction
                    tx = self._generate_rule_based_transaction(
                        is_fraud, scenarios, request, batch_offset + idx
                    )
                    transactions.append(tx)
            
            # If OpenAI didn't generate enough, fill with rule-based
            while len(transactions) < count:
                idx = len(transactions)
                tx = self._generate_rule_based_transaction(
                    is_fraud, scenarios, request, batch_offset + idx
                )
                transactions.append(tx)
            
            return transactions[:count]
            
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            # Fallback to rule-based generation
            return await self._generate_rule_based_batch(
                count, is_fraud, scenarios, request, batch_offset
            )
    
    def _build_prompt(
        self,
        count: int,
        is_fraud: bool,
        scenarios: List[FraudScenario],
        request: GenerateRequest
    ) -> str:
        """Build prompt for OpenAI API."""
        scenario_names = [s.value for s in scenarios] if scenarios else ["any"]
        
        prompt = f"""Generate {count} realistic financial transactions as a JSON array.

Requirements:
- Transaction type: {"FRAUDULENT" if is_fraud else "LEGITIMATE"}
- Fraud scenarios: {", ".join(scenario_names) if is_fraud else "N/A"}
- Currency: {request.currency}
- Countries: {", ".join(request.countries)}
- Each transaction must include: transaction_id, user_id, merchant_id (optional), amount, currency, transaction_type, timestamp (ISO format), country, city (optional), ip_address, device_id (optional), card_last4 (optional), is_fraud, fraud_scenarios (array), explanation

Return format:
{{
  "transactions": [
    {{
      "transaction_id": "tx_1234567890",
      "user_id": "user_987654321",
      "merchant_id": "merchant_123",
      "amount": 150.50,
      "currency": "{request.currency}",
      "transaction_type": "purchase",
      "timestamp": "2025-01-30T10:30:00Z",
      "country": "{request.countries[0] if request.countries else 'US'}",
      "city": "New York",
      "ip_address": "192.168.1.1",
      "device_id": "device_abc123",
      "card_last4": "1234",
      "is_fraud": {str(is_fraud).lower()},
      "fraud_scenarios": {json.dumps([s.value for s in scenarios]) if is_fraud else "[]"},
      "explanation": "Description of the transaction"
    }}
  ]
}}

Generate exactly {count} transactions. Make them realistic and diverse."""
        
        return prompt
    
    def _parse_transaction(
        self,
        tx_data: dict,
        request: GenerateRequest,
        index: int
    ) -> Transaction:
        """Parse transaction data from OpenAI response."""
        # Parse timestamp
        if isinstance(tx_data.get("timestamp"), str):
            timestamp = datetime.fromisoformat(tx_data["timestamp"].replace("Z", "+00:00"))
        else:
            timestamp = datetime.now() - timedelta(days=random.randint(0, 30))
        
        # Parse fraud scenarios
        fraud_scenarios = []
        if tx_data.get("fraud_scenarios"):
            for s in tx_data["fraud_scenarios"]:
                try:
                    fraud_scenarios.append(FraudScenario(s))
                except:
                    pass
        
        # Parse transaction type
        try:
            tx_type = TransactionType(tx_data.get("transaction_type", "purchase"))
        except:
            tx_type = TransactionType.PURCHASE
        
        return Transaction(
            transaction_id=tx_data.get("transaction_id", f"tx_{int(timestamp.timestamp())}_{index}"),
            user_id=tx_data.get("user_id", f"user_{random.randint(100000, 999999)}"),
            merchant_id=tx_data.get("merchant_id"),
            amount=float(tx_data.get("amount", random.uniform(1, 500))),
            currency=tx_data.get("currency", request.currency),
            transaction_type=tx_type,
            timestamp=timestamp,
            country=tx_data.get("country", random.choice(request.countries) if request.countries else "US"),
            city=tx_data.get("city"),
            ip_address=tx_data.get("ip_address", f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"),
            device_id=tx_data.get("device_id"),
            card_last4=tx_data.get("card_last4"),
            is_fraud=tx_data.get("is_fraud", False),
            fraud_scenarios=fraud_scenarios,
            explanation=tx_data.get("explanation", "Generated transaction"),
            metadata={
                "generated_at": datetime.now().isoformat(),
                "generation_method": "openai"
            }
        )
    
    async def _generate_rule_based_batch(
        self,
        count: int,
        is_fraud: bool,
        scenarios: List[FraudScenario],
        request: GenerateRequest,
        batch_offset: int
    ) -> List[Transaction]:
        """Generate transactions using rule-based approach (fallback)."""
        transactions = []
        for i in range(count):
            tx = self._generate_rule_based_transaction(
                is_fraud=is_fraud,
                scenarios=scenarios,
                request=request,
                index=batch_offset + i
            )
            transactions.append(tx)
        return transactions
    
    def _generate_rule_based_transaction(
        self,
        is_fraud: bool,
        scenarios: List[FraudScenario],
        request: GenerateRequest,
        index: int
    ) -> Transaction:
        """Generate a transaction using rule-based approach."""
        # Generate timestamp
        if request.start_date and request.end_date:
            time_range = (request.end_date - request.start_date).total_seconds()
            random_seconds = random.uniform(0, time_range)
            timestamp = request.start_date + timedelta(seconds=random_seconds)
        else:
            timestamp = datetime.now() - timedelta(days=random.randint(0, 30))
        
        # Generate user and merchant IDs
        user_id = f"user_{random.randint(100000, 999999)}"
        merchant_id = f"merchant_{random.randint(100, 999)}" if random.random() > 0.3 else None
        
        # Generate amount (fraudulent transactions tend to be larger)
        if is_fraud:
            amount = random.uniform(100, 10000) if random.random() > 0.5 else random.uniform(0.01, 10)
        else:
            amount = random.uniform(1, 500)
        
        # Select country
        country = random.choice(request.countries) if request.countries else "US"
        
        # Generate transaction type
        tx_type = random.choice(list(TransactionType))
        
        # Generate transaction ID
        tx_id = f"tx_{int(timestamp.timestamp())}_{index}_{random.randint(1000, 9999)}"
        
        # Fraud-specific attributes
        fraud_scenarios_list = []
        explanation = "Normal transaction"
        
        if is_fraud and scenarios:
            selected_scenario = random.choice(scenarios)
            fraud_scenarios_list = [selected_scenario]
            
            # Adjust transaction based on scenario
            if selected_scenario == FraudScenario.CARD_TESTING:
                amount = random.uniform(0.01, 5)
                explanation = "Suspicious card testing pattern: multiple small transactions"
            elif selected_scenario == FraudScenario.ACCOUNT_TAKEOVER:
                amount = random.uniform(500, 5000)
                explanation = "Account takeover: unusual transaction pattern from new device"
            elif selected_scenario == FraudScenario.IDENTITY_THEFT:
                amount = random.uniform(1000, 10000)
                explanation = "Identity theft: transaction inconsistent with user profile"
            elif selected_scenario == FraudScenario.MERCHANT_FRAUD:
                amount = random.uniform(50, 2000)
                explanation = "Merchant fraud: suspicious merchant activity"
            elif selected_scenario == FraudScenario.MONEY_LAUNDERING:
                amount = random.uniform(5000, 50000)
                explanation = "Money laundering: structured transaction pattern"
            elif selected_scenario == FraudScenario.PHISHING:
                amount = random.uniform(100, 1000)
                explanation = "Phishing: transaction from compromised account"
            elif selected_scenario == FraudScenario.CHARGEBACK_FRAUD:
                amount = random.uniform(200, 2000)
                explanation = "Chargeback fraud: transaction likely to be disputed"
        
        # Generate optional fields
        ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        device_id = f"device_{random.randint(100000, 999999)}" if random.random() > 0.2 else None
        card_last4 = f"{random.randint(1000, 9999)}" if random.random() > 0.3 else None
        city = random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Paris", "London", "Tokyo"]) if random.random() > 0.4 else None
        
        return Transaction(
            transaction_id=tx_id,
            user_id=user_id,
            merchant_id=merchant_id,
            amount=round(amount, 2),
            currency=request.currency,
            transaction_type=tx_type,
            timestamp=timestamp,
            country=country,
            city=city,
            ip_address=ip_address,
            device_id=device_id,
            card_last4=card_last4,
            is_fraud=is_fraud,
            fraud_scenarios=fraud_scenarios_list,
            explanation=explanation,
            metadata={
                "generated_at": datetime.now().isoformat(),
                "generation_method": "rule_based"
            }
        )


# Global instance
llm_service = LLMService()
