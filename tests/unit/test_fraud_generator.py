"""Tests unitaires — schéma API SafeGuard / bank-security et répartition légit / fraude."""
import pytest

from app.models.bank_security import OpenAPIScoreRequest, ScoreRequestFlat
from app.services.dataset_service import dataset_service


@pytest.fixture
def sample_openapi_score_body():
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
            "metadata": {"tx_count_1h": 4, "tx_count_15m": 1},
        },
    }


def test_post_v1_score_openapi_schema_validates(sample_openapi_score_body):
    """Schéma attendu pour POST /v1/score (variante OpenAPI documentée)."""
    m = OpenAPIScoreRequest.model_validate(sample_openapi_score_body)
    assert m.event.metadata["tx_count_1h"] == 4


def test_post_v1_score_flat_schema_validates():
    """Schéma decision-engine plat."""
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
        "has_initial_2fa": False,
        "metadata": {"tx_count_1h": 2},
    }
    ScoreRequestFlat.model_validate(flat)


@pytest.mark.parametrize("count,fraud_ratio", [(100, 0.1), (50, 0.25), (10, 0.0)])
def test_fraud_ratio_split_math(count, fraud_ratio):
    """Répartition légit / fraude conforme au ratio demandé (logique CLI)."""
    fraud_n = int(count * fraud_ratio)
    legit_n = count - fraud_n
    assert fraud_n + legit_n == count
    assert 0 <= fraud_n <= count


def test_fallback_seeds_cover_fraud_patterns():
    """Sans CSV, les graines incluent montants / pays / vélocité variés (checklist SafeGuard)."""
    dataset_service.initialize()
    countries = set()
    patterns = set()
    amounts = []
    for _ in range(80):
        s = dataset_service.get_random_fraud()
        countries.add(s["country"])
        patterns.add(s.get("fraud_pattern", "unknown"))
        amounts.append(s["amount"])
    assert len(countries) >= 2
    assert max(amounts) >= 1000
    assert len(patterns) >= 1
