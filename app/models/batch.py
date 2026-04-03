"""Métadonnées de lot de génération."""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.models.transaction import FraudScenario


class BatchCreate(BaseModel):
    batch_id: str
    seed: Optional[int] = None
    scenarios: List[FraudScenario]
    count: int
    fraud_ratio: float
    generated_count: int
    fraudulent_count: int
    legit_count: int
    s3_uri: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        return self.model_dump(**kwargs)
