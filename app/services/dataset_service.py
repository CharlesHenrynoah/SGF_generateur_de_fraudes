"""Service to load and sample from local CSV datasets for RAG/Few-Shot generation."""
import pandas as pd
import random
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class DatasetService:
    def __init__(self):
        self.synthetic_df = None
        self.creditcard_df = None
        self._initialized = False

    def initialize(self):
        """Load datasets into memory."""
        if self._initialized:
            return

        try:
            # Load synthetic dataset (Rich features)
            synthetic_path = "/Users/charles-henrynoah/SGF_generateur_de_fraudes/Datas/synthetic_fraud_dataset.csv"
            self.synthetic_df = pd.read_csv(synthetic_path)
            logger.info(f"Loaded synthetic dataset: {len(self.synthetic_df)} rows")

            # Load creditcard dataset (PCA anonymized - mostly for amount distribution if needed)
            creditcard_path = "/Users/charles-henrynoah/SGF_generateur_de_fraudes/Datas/creditcard.csv"
            self.creditcard_df = pd.read_csv(creditcard_path)
            logger.info(f"Loaded creditcard dataset: {len(self.creditcard_df)} rows")
            
            self._initialized = True
        except Exception as e:
            logger.error(f"Error loading datasets: {e}")
            raise

    def get_few_shot_examples(self, count: int = 3, fraud_only: bool = False) -> List[Dict]:
        """Get random examples from the synthetic dataset."""
        if not self._initialized:
            self.initialize()

        df = self.synthetic_df
        
        if fraud_only:
            df = df[df['is_fraud'] == 1]
        
        # Sample random rows
        if len(df) < count:
            sample = df
        else:
            sample = df.sample(n=count)
        
        return sample.to_dict('records')

    def get_random_fraud(self) -> Dict:
        """Get a single random fraud example."""
        return self.get_few_shot_examples(1, fraud_only=True)[0]

    def get_random_legit(self) -> Dict:
        """Get a single random legit example."""
        if not self._initialized:
            self.initialize()
        
        df = self.synthetic_df[self.synthetic_df['is_fraud'] == 0]
        return df.sample(n=1).to_dict('records')[0]

dataset_service = DatasetService()
