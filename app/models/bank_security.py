"""Formats alignés sur bank-security : scoring (/v1/score) et labels (case-service)."""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# --- §1 decision-engine : corps plat POST /v1/score ---


class MerchantIn(BaseModel):
    id: str
    name: str
    mcc: str
    country: str


class CardIn(BaseModel):
    card_id: str
    user_id: str
    type: str
    bin: Optional[str] = None


class ContextIn(BaseModel):
    ip: str
    geo: str
    device_id: str
    channel: str
    user_agent: Optional[str] = None
    proxy_vpn_flag: Optional[bool] = None


class ScoreRequestFlat(BaseModel):
    """Requête scoring — implémentation decision-engine (plat)."""

    event_id: str
    tenant_id: str = "default"
    amount: float = Field(gt=0)
    currency: str = "EUR"
    merchant: MerchantIn
    card: CardIn
    context: ContextIn
    has_initial_2fa: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


# --- §3 OpenAPI : corps imbriqué ---


class EventOpenAPI(BaseModel):
    type: Literal["card_payment"] = "card_payment"
    id: str
    ts: str
    amount: float
    currency: str
    merchant: MerchantIn
    card: CardIn
    context: ContextIn
    security: Optional[Dict[str, Any]] = None
    kyc: Optional[Dict[str, Any]] = None
    # Vélocité / signaux additionnels (ex. tx_count_1h) pour tests pipeline
    metadata: Optional[Dict[str, Any]] = None


class OpenAPIScoreRequest(BaseModel):
    """Requête scoring — spec OpenAPI (tenant_id, idempotency_key, event)."""

    tenant_id: str
    idempotency_key: str
    event: EventOpenAPI


# --- §2 Réponse decision-engine ---


class ScoreResponseDTO(BaseModel):
    event_id: str
    decision_id: str
    decision: Literal["ALLOW", "CHALLENGE", "DENY"]
    score: Optional[float] = Field(None, ge=0, le=1)
    reasons: List[str] = Field(default_factory=list)
    rule_hits: List[str] = Field(default_factory=list)
    latency_ms: int
    model_version: str
    requires_2fa: bool = False


# --- §4 case-service : label ---


class LabelCreateDTO(BaseModel):
    event_id: str
    label: Literal["fraud", "legit", "chargeback", "fp"]
    source: Literal["analyst", "customer", "chargeback_system"]
    confidence: Optional[float] = Field(None, ge=0, le=1)
