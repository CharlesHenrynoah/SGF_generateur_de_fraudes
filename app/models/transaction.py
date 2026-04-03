"""Modèles Pydantic pour transactions synthétiques et requêtes de génération."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FraudScenario(str, Enum):
    CARD_TESTING = "card_testing"
    ACCOUNT_TAKEOVER = "account_takeover"
    IDENTITY_THEFT = "identity_theft"
    MERCHANT_FRAUD = "merchant_fraud"
    MONEY_LAUNDERING = "money_laundering"
    PHISHING = "phishing"
    CHARGEBACK_FRAUD = "chargeback_fraud"


class TransactionType(str, Enum):
    PURCHASE = "purchase"
    TRANSFER = "transfer"
    WITHDRAWAL = "withdrawal"
    PAYMENT = "payment"
    REFUND = "refund"


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    merchant_id: Optional[str] = None
    amount: float
    currency: str
    transaction_type: TransactionType
    timestamp: datetime
    country: str
    city: Optional[str] = None
    ip_address: Optional[str] = None
    device_id: Optional[str] = None
    card_last4: Optional[str] = None
    is_fraud: bool = False
    fraud_scenarios: List[FraudScenario] = Field(default_factory=list)
    explanation: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
    batch_id: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        return self.model_dump(**kwargs)


class GenerateRequest(BaseModel):
    count: int = Field(ge=1, le=100_000)
    fraud_ratio: float = Field(ge=0.0, le=1.0)
    scenarios: List[FraudScenario] = Field(default_factory=list)
    currency: str = "USD"
    countries: List[str] = Field(default_factory=lambda: ["US"])
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    seed: Optional[int] = None

    @field_validator("countries", mode="before")
    @classmethod
    def non_empty_countries(cls, v: Any) -> Any:
        if isinstance(v, list) and len(v) == 0:
            return ["US"]
        return v


class GenerateResponse(BaseModel):
    batch_id: str
    generated: int
    fraudulent: int
    legit: int
    latency_ms: int
    s3_uri: Optional[str] = None
    transactions: Optional[List[Transaction]] = None
