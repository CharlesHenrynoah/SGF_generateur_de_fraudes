"""Tests unitaires — modèles bank-security et sql_row_from_rag."""
import pytest

from app.models.bank_security import (
    LabelCreateDTO,
    OpenAPIScoreRequest,
    ScoreRequestFlat,
    ScoreResponseDTO,
)
from app.services.rag_bank_security import sql_row_from_rag


@pytest.fixture
def sample_openapi_body():
    return {
        "tenant_id": "bank-fr-001",
        "idempotency_key": "tx-test-001",
        "event": {
            "type": "card_payment",
            "id": "evt_1",
            "ts": "2025-01-01T12:00:00.000000Z",
            "amount": 10.0,
            "currency": "EUR",
            "merchant": {
                "id": "m1",
                "name": "Shop",
                "mcc": "5411",
                "country": "FR",
            },
            "card": {"card_id": "c1", "user_id": "u1", "type": "physical"},
            "context": {
                "ip": "1.2.3.4",
                "geo": "FR",
                "device_id": "d1",
                "channel": "pos",
            },
        },
    }


def test_openapi_score_request_validates(sample_openapi_body):
    m = OpenAPIScoreRequest.model_validate(sample_openapi_body)
    assert m.tenant_id == "bank-fr-001"
    assert m.event.type == "card_payment"


def test_flat_score_request_validates():
    flat = {
        "event_id": "evt_1",
        "tenant_id": "default",
        "amount": 10.0,
        "currency": "EUR",
        "merchant": {
            "id": "m1",
            "name": "Shop",
            "mcc": "5411",
            "country": "FR",
        },
        "card": {"card_id": "c1", "user_id": "u1", "type": "physical"},
        "context": {
            "ip": "1.2.3.4",
            "geo": "FR",
            "device_id": "d1",
            "channel": "pos",
        },
    }
    ScoreRequestFlat.model_validate(flat)


def test_score_response_and_label():
    ScoreResponseDTO.model_validate(
        {
            "event_id": "evt_1",
            "decision_id": "dec_1",
            "decision": "ALLOW",
            "latency_ms": 1,
            "model_version": "v1",
        }
    )
    LabelCreateDTO.model_validate(
        {"event_id": "evt_1", "label": "fraud", "source": "analyst"}
    )


def test_sql_row_openapi(sample_openapi_body):
    row = sql_row_from_rag(sample_openapi_body, "openapi", True)
    assert row["transaction_id"] == "evt_1"
    assert row["is_fraud"] is True
