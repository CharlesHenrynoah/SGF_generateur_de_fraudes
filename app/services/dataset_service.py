"""Service to load and sample from local CSV datasets for RAG/Few-Shot generation."""
import logging
import os
import random
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Racine du projet (…/SGF_generateur_de_fraudes)
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_DATADIR = os.path.join(_PROJECT_ROOT, "Datas")

# Pays souvent cités comme à risque élevé (tests AML / géo) — données synthétiques
_HIGH_RISK_GEO = ["NG", "PK", "BD", "MM", "UA", "RU", "BY", "IR", "VE", "AF"]

# Pays « normaux » pour légitimes
_LOW_RISK_GEO = ["FR", "DE", "NL", "BE", "CH", "US", "CA", "GB"]


def _default_seed_legit() -> Dict[str, Any]:
    return {
        "amount": round(random.uniform(5, 180), 2),
        "transaction_type": random.choice(["Online", "POS"]),
        "merchant_category": random.choice(["grocery", "fuel", "pharmacy", "restaurant"]),
        "country": random.choice(_LOW_RISK_GEO),
        "hour": random.randint(9, 21),
        "user_id": random.randint(10000, 99999),
        "is_fraud": 0,
        "velocity_hint": "normal",
    }


def _default_seed_fraud() -> Dict[str, Any]:
    pattern = random.choice(["high_amount", "high_risk_country", "odd_hour", "velocity", "mixed"])
    country = random.choice(_HIGH_RISK_GEO) if pattern in ("high_risk_country", "mixed") else "US"
    if pattern == "high_amount":
        amount = round(random.uniform(8000, 95000), 2)
    elif pattern == "velocity":
        amount = round(random.uniform(20, 150), 2)
    else:
        amount = round(random.uniform(200, 12000), 2)
    hour = random.randint(1, 5) if pattern in ("odd_hour", "mixed") else random.randint(22, 23)
    vel = random.choice(
        [
            {"tx_count_15m": 12, "tx_count_1h": 38},
            {"tx_count_1h": 22, "distinct_merchants_1h": 9},
            {"burst": True, "tx_count_10m": 8},
        ]
    )
    return {
        "amount": amount,
        "transaction_type": random.choice(["Online", "Online"]),
        "merchant_category": random.choice(["electronics", "gaming", "crypto", "luxury"]),
        "country": country,
        "hour": hour,
        "user_id": random.randint(10000, 99999),
        "is_fraud": 1,
        "fraud_pattern": pattern,
        "velocity_hint": vel,
    }


class DatasetService:
    def __init__(self):
        self.synthetic_df = None
        self.creditcard_df = None
        self._initialized = False
        self._use_fallback: bool = False

    def initialize(self):
        """Load datasets into memory."""
        if self._initialized:
            return

        synthetic_path = os.path.join(_DATADIR, "synthetic_fraud_dataset.csv")
        creditcard_path = os.path.join(_DATADIR, "creditcard.csv")

        try:
            if os.path.isfile(synthetic_path):
                self.synthetic_df = pd.read_csv(synthetic_path)
                logger.info("Dataset synthétique chargé: %s lignes", len(self.synthetic_df))
            else:
                logger.warning("Fichier absent: %s — mode RAG avec graines par défaut", synthetic_path)
                self.synthetic_df = None

            if os.path.isfile(creditcard_path):
                self.creditcard_df = pd.read_csv(creditcard_path)
                logger.info("creditcard.csv chargé: %s lignes", len(self.creditcard_df))
            else:
                self.creditcard_df = None

            self._use_fallback = self.synthetic_df is None
            self._initialized = True
        except Exception as e:
            logger.error("Erreur chargement datasets: %s — fallback", e)
            self.synthetic_df = None
            self._use_fallback = True
            self._initialized = True

    def get_few_shot_examples(self, count: int = 3, fraud_only: bool = False) -> List[Dict]:
        """Get random examples from the synthetic dataset."""
        if not self._initialized:
            self.initialize()

        if self._use_fallback or self.synthetic_df is None:
            return [_default_seed_fraud() if fraud_only else _default_seed_legit() for _ in range(count)]

        df = self.synthetic_df
        if fraud_only:
            df = df[df["is_fraud"] == 1]
        if len(df) < count:
            sample = df
        else:
            sample = df.sample(n=count)
        return sample.to_dict("records")

    def get_random_fraud(self) -> Dict:
        """Get a single random fraud example."""
        if not self._initialized:
            self.initialize()
        if self._use_fallback or self.synthetic_df is None:
            return _default_seed_fraud()
        df = self.synthetic_df[self.synthetic_df["is_fraud"] == 1]
        if len(df) == 0:
            return _default_seed_fraud()
        return df.sample(n=1).to_dict("records")[0]

    def get_random_legit(self) -> Dict:
        """Get a single random legit example."""
        if not self._initialized:
            self.initialize()
        if self._use_fallback or self.synthetic_df is None:
            return _default_seed_legit()
        df = self.synthetic_df[self.synthetic_df["is_fraud"] == 0]
        if len(df) == 0:
            return _default_seed_legit()
        return df.sample(n=1).to_dict("records")[0]


dataset_service = DatasetService()
