"""Génération RAG au format bank-security (OpenAPI, decision-engine, labels)."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Literal, Optional
from app.models.bank_security import (
    LabelCreateDTO,
    OpenAPIScoreRequest,
    ScoreRequestFlat,
    ScoreResponseDTO,
)
from app.services.llm_service import llm_service

logger = logging.getLogger(__name__)

RagFormat = Literal["openapi", "flat", "response", "label", "hybrid"]


def _ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.utcnow().microsecond:06d}Z"


def _fraud_pattern_instructions(seed: Dict[str, Any], is_fraud: bool) -> str:
    if not is_fraud:
        return "This must look like a NORMAL low-risk payment (typical amount, business hours, low-risk geography)."
    pat = seed.get("fraud_pattern", "mixed")
    vel = seed.get("velocity_hint", {})
    return f"""
FRAUD / ABUSE SIMULATION (must be obvious to a risk engine):
- Pattern focus: {pat}
- Prefer HIGH amount (large ticket) OR many small hits in a short window (velocity) as appropriate.
- Use HIGH-RISK geography when seed country is unusual: merchant.country and context.geo = {seed.get("country")} (align both).
- Set event.ts to an UNUSUAL local hour (e.g. 01:00–05:00 or 23:00–23:59 UTC-style) when simulating odd-hour fraud; seed hour hint={seed.get("hour")}.
- Encode ABNORMAL VELOCITY in event.metadata as JSON object, e.g. {json.dumps(vel) if vel else '{"tx_count_1h": 35}'} — include tx_count_15m / tx_count_1h / distinct_merchants_1h as relevant.
- security.aml_flag should often be true for fraud cases; kyc may show elevated risk if useful.
"""


def _prompt_openapi(seed: Dict[str, Any], is_fraud: bool) -> str:
    fraud_block = _fraud_pattern_instructions(seed, is_fraud)
    return f"""You are a test data generator for a fraud scoring API.
Generate ONE JSON object that is ONLY the HTTP request body for POST /v1/score (OpenAPI spec).

Rules:
- Root keys EXACTLY: "tenant_id", "idempotency_key", "event" (no other root keys).
- event.type must be "card_payment".
- currency: 3 uppercase letters. merchant.mcc: exactly 4 digits as string. merchant.country and context.geo: 2-letter country codes.
- context.channel must be one of: app, web, pos, atm.
- card.type: physical or virtual.
- Include optional "security" and "kyc" objects like in the spec.
- Include optional "metadata" on event for velocity / extra signals (object with numeric counters).
{fraud_block}
- Amount and narrative should be realistic; base context on:
  amount_hint={seed["amount"]}, channel_hint={"web" if seed["transaction_type"] == "Online" else "pos"},
  country={seed["country"]}, merchant_category={seed["merchant_category"]}, fraud_context={is_fraud}.

Example shape (replace with realistic values):
{{
  "tenant_id": "bank-fr-001",
  "idempotency_key": "tx-YYYYMMDD-xxxxxxxx",
  "event": {{
    "type": "card_payment",
    "id": "evt_...",
    "ts": "{_ts()}",
    "amount": 45.5,
    "currency": "EUR",
    "merchant": {{"id": "...", "name": "...", "mcc": "5411", "country": "FR"}},
    "card": {{"card_id": "...", "type": "physical", "user_id": "..."}},
    "context": {{"ip": "...", "geo": "FR", "device_id": "...", "channel": "pos"}},
    "security": {{"auth_method": "pin", "aml_flag": false}},
    "kyc": {{"status": "verified", "level": "standard", "confidence": 0.95}}
  }}
}}

Output ONLY valid JSON, no markdown."""


def _prompt_flat(seed: Dict[str, Any], is_fraud: bool) -> str:
    fraud_block = _fraud_pattern_instructions(seed, is_fraud)
    return f"""You are a test data generator for a fraud decision-engine.
Generate ONE JSON object: the flat request body for POST /v1/score (ScoreRequest).

Rules:
- Root keys: event_id, tenant_id, amount (> 0), currency, merchant {{id, name, mcc, country}},
  card {{card_id, user_id, type, bin?}}, context {{ip, geo, device_id, channel, user_agent?, proxy_vpn_flag?}},
  has_initial_2fa, metadata {{}}.
- NO nested "event" wrapper. NO idempotency_key at root.
- context.channel: one of app, web, pos, atm.
- card.type: physical or virtual.
- Put velocity counters (tx_count_1h, etc.) inside metadata when simulating fraud.
{fraud_block}

Base hints: amount={seed["amount"]}, country={seed["country"]}, category={seed["merchant_category"]},
fraud_like={is_fraud}.

Output ONLY valid JSON, no markdown."""


def _prompt_score_response(seed: Dict[str, Any], is_fraud: bool) -> str:
    decision = "DENY" if is_fraud else "ALLOW"
    score_hi = "0.85-0.99" if is_fraud else "0.01-0.15"
    return f"""Generate ONE JSON object: the response body from the scoring engine (ScoreResponse).

Required fields: event_id, decision_id, decision (ALLOW|CHALLENGE|DENY), score (0-1), reasons (array),
rule_hits (array), latency_ms, model_version, requires_2fa (boolean).

Use decision={decision} and score in range {score_hi}.
event_id must be a plausible evt_ id linked to this context: amount={seed["amount"]}, country={seed["country"]}.

Output ONLY valid JSON, no markdown."""


def _prompt_label(seed: Dict[str, Any], is_fraud: bool) -> str:
    lab = "fraud" if is_fraud else "legit"
    return f"""Generate ONE JSON object for POST /v1/cases/{{case_id}}/label (LabelCreate).

Fields: event_id (string), label (fraud|legit|chargeback|fp), source (analyst|customer|chargeback_system), confidence (0-1, optional).

Use label="{lab}", source="analyst", confidence between 0.85 and 0.99.
event_id should look like evt_... and be consistent with context amount={seed["amount"]}.

Output ONLY valid JSON, no markdown."""


def _prompt_hybrid(seed: Dict[str, Any], is_fraud: bool) -> str:
    return f"""
    You are a financial fraud simulation engine.
    Generate a JSON object representing a transaction analysis response (legacy hybrid format).

    BASE DATA:
    - Amount: {seed['amount']}
    - Type: {seed['transaction_type']}
    - Merchant Category: {seed['merchant_category']}
    - Country: {seed['country']}
    - Hour: {seed['hour']}
    - Is Fraud: {is_fraud}

    OUTPUT FORMAT (Strict JSON):
    {{
      "decision_id": "dec_{str(uuid.uuid4())[:12]}",
      "decision": "{'DENY' if is_fraud else 'ALLOW'}",
      "score": 0.5,
      "rule_hits": ["example_rule"],
      "reasons": ["Human readable reasons"],
      "latency_ms": 50,
      "model_version": "gbdt_v1.2.3",
      "sla": {{ "p95_budget_ms": 100 }},
      "transaction_context": {{
        "tenant_id": "bank-fr-001",
        "idempotency_key": "tx-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}",
        "event": {{
          "type": "card_payment",
          "id": "evt_{str(uuid.uuid4())[:12]}",
          "ts": "{datetime.now().isoformat()}Z",
          "amount": {seed['amount']},
          "currency": "EUR",
          "merchant": {{
            "id": "merch_{str(uuid.uuid4())[:8]}",
            "name": "Realistic merchant for category {seed['merchant_category']}",
            "mcc": "5411",
            "country": "{seed['country']}"
          }},
          "card": {{
            "card_id": "card_tok_{str(uuid.uuid4())[:8]}",
            "type": "{'virtual' if seed['transaction_type'] == 'Online' else 'physical'}",
            "user_id": "user_{seed['user_id']}"
          }},
          "context": {{
            "ip": "82.64.123.45",
            "geo": "{seed['country']}",
            "device_id": "dev_{str(uuid.uuid4())[:8]}",
            "channel": "{'web' if seed['transaction_type'] == 'Online' else 'pos'}"
          }},
          "security": {{
            "auth_method": "pin",
            "aml_flag": {json.dumps(is_fraud)}
          }},
          "kyc": {{
            "status": "verified",
            "level": "standard",
            "confidence": 0.95
          }}
        }}
      }}
    }}

    Replace score with realistic value; ensure JSON is valid. Output ONLY JSON."""


def _validate_payload(data: Dict[str, Any], fmt: RagFormat) -> Dict[str, Any]:
    """Valide et normalise via Pydantic."""
    if fmt == "openapi":
        return OpenAPIScoreRequest.model_validate(data).model_dump(mode="json")
    if fmt == "flat":
        return ScoreRequestFlat.model_validate(data).model_dump(mode="json")
    if fmt == "response":
        return ScoreResponseDTO.model_validate(data).model_dump(mode="json")
    if fmt == "label":
        return LabelCreateDTO.model_validate(data).model_dump(mode="json")
    return data


async def generate_rag_payload(is_fraud: bool, fmt: RagFormat = "openapi") -> Optional[Dict[str, Any]]:
    """
    Génère un JSON selon le format bank-security.
    - openapi: corps OpenAPI §3 uniquement
    - flat: ScoreRequest plat §1
    - response: ScoreResponse §2
    - label: LabelCreate §4
    - hybrid: ancien format (decision + transaction_context)
    """
    from app.services.dataset_service import dataset_service

    if is_fraud:
        seed_data = dataset_service.get_random_fraud()
    else:
        seed_data = dataset_service.get_random_legit()

    if fmt == "openapi":
        prompt = _prompt_openapi(seed_data, is_fraud)
    elif fmt == "flat":
        prompt = _prompt_flat(seed_data, is_fraud)
    elif fmt == "response":
        prompt = _prompt_score_response(seed_data, is_fraud)
    elif fmt == "label":
        prompt = _prompt_label(seed_data, is_fraud)
    else:
        prompt = _prompt_hybrid(seed_data, is_fraud)

    try:
        raw = await llm_service.chat_json_completion(
            "You are a data generator. Output ONLY valid JSON, no markdown.",
            prompt,
        )
        if fmt != "hybrid":
            try:
                return _validate_payload(raw, fmt)
            except Exception as ve:
                logger.warning("Validation Pydantic échouée, retour brut: %s", ve)
                return raw
        return raw
    except Exception as e:
        logger.error("generate_rag_payload: %s", e)
        return None


def sql_row_from_rag(data: Dict[str, Any], fmt: RagFormat, is_fraud: bool) -> Dict[str, Any]:
    """Construit les champs pour synthetic_transactions à partir du payload généré."""
    batch_stub = f"rag_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    if fmt == "openapi":
        ev = data["event"]
        return {
            "transaction_id": ev["id"],
            "user_id": ev["card"]["user_id"],
            "merchant_id": ev["merchant"]["id"],
            "amount": ev["amount"],
            "currency": ev["currency"],
            "transaction_type": ev["type"],
            "timestamp": ev["ts"].replace("Z", "+00:00") if "Z" in ev["ts"] else ev["ts"],
            "country": ev["merchant"]["country"],
            "city": None,
            "ip_address": ev["context"]["ip"],
            "device_id": ev["context"]["device_id"],
            "card_last4": None,
            "is_fraud": is_fraud,
            "fraud_scenarios": [],
            "explanation": json.dumps({"format": "openapi", "tenant_id": data.get("tenant_id")}),
            "batch_id": batch_stub,
            "metadata": data,
        }

    if fmt == "flat":
        return {
            "transaction_id": data["event_id"],
            "user_id": data["card"]["user_id"],
            "merchant_id": data["merchant"]["id"],
            "amount": data["amount"],
            "currency": data["currency"],
            "transaction_type": "card_payment",
            "timestamp": datetime.utcnow().isoformat(),
            "country": data["merchant"]["country"],
            "city": None,
            "ip_address": data["context"]["ip"],
            "device_id": data["context"]["device_id"],
            "card_last4": None,
            "is_fraud": is_fraud,
            "fraud_scenarios": [],
            "explanation": json.dumps({"format": "flat", "tenant_id": data.get("tenant_id")}),
            "batch_id": batch_stub,
            "metadata": data,
        }

    if fmt == "response":
        return {
            "transaction_id": data["event_id"],
            "user_id": "unknown",
            "merchant_id": "unknown",
            "amount": 0.0,
            "currency": "EUR",
            "transaction_type": "card_payment",
            "timestamp": datetime.utcnow().isoformat(),
            "country": "FR",
            "city": None,
            "ip_address": None,
            "device_id": None,
            "card_last4": None,
            "is_fraud": data.get("decision") == "DENY",
            "fraud_scenarios": data.get("rule_hits") or [],
            "explanation": ", ".join(data.get("reasons") or []),
            "batch_id": batch_stub,
            "metadata": data,
        }

    if fmt == "label":
        return {
            "transaction_id": data["event_id"],
            "user_id": "unknown",
            "merchant_id": "unknown",
            "amount": 0.0,
            "currency": "EUR",
            "transaction_type": "card_payment",
            "timestamp": datetime.utcnow().isoformat(),
            "country": "FR",
            "city": None,
            "ip_address": None,
            "device_id": None,
            "card_last4": None,
            "is_fraud": data.get("label") == "fraud",
            "fraud_scenarios": [data["label"]] if data.get("label") else [],
            "explanation": json.dumps({"source": data.get("source"), "confidence": data.get("confidence")}),
            "batch_id": batch_stub,
            "metadata": data,
        }

    # hybrid
    tx = data
    ctx = tx["transaction_context"]["event"]
    return {
        "transaction_id": ctx["id"],
        "user_id": ctx["card"]["user_id"],
        "merchant_id": ctx["merchant"]["id"],
        "amount": ctx["amount"],
        "currency": ctx["currency"],
        "transaction_type": ctx["type"],
        "timestamp": ctx["ts"].replace("Z", "") if isinstance(ctx["ts"], str) else str(ctx["ts"]),
        "country": ctx["merchant"]["country"],
        "city": None,
        "ip_address": ctx["context"]["ip"],
        "device_id": ctx["context"]["device_id"],
        "card_last4": None,
        "is_fraud": tx.get("decision") == "DENY",
        "fraud_scenarios": tx.get("rule_hits") or [],
        "explanation": ", ".join(tx.get("reasons") or []),
        "batch_id": batch_stub,
        "metadata": data,
    }
