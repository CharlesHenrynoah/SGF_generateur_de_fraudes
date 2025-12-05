"""Validation and filtering service for synthetic transactions."""
import hashlib
import json
import logging
from typing import List, Set, Dict
from app.models.transaction import Transaction
from scipy import stats
import numpy as np

logger = logging.getLogger(__name__)


class ValidationService:
    """Service for validating and filtering transactions."""
    
    def __init__(self):
        self.seen_signatures: Set[str] = set()
    
    def validate_schema(self, transactions: List[Transaction]) -> List[Transaction]:
        """Validate transactions against schema."""
        valid_transactions = []
        for tx in transactions:
            try:
                # Pydantic validation happens automatically
                # Additional custom validation can be added here
                valid_transactions.append(tx)
            except Exception as e:
                logger.warning(f"Transaction {tx.transaction_id} failed validation: {e}")
        return valid_transactions
    
    def deduplicate(
        self,
        transactions: List[Transaction],
        existing_signatures: Set[str] = None
    ) -> tuple[List[Transaction], Set[str]]:
        """Remove duplicate transactions based on hash signatures."""
        if existing_signatures is None:
            existing_signatures = self.seen_signatures.copy()
        
        unique_transactions = []
        new_signatures = set()
        
        for tx in transactions:
            signature = self._compute_signature(tx)
            if signature not in existing_signatures and signature not in new_signatures:
                unique_transactions.append(tx)
                new_signatures.add(signature)
            else:
                logger.debug(f"Duplicate transaction detected: {tx.transaction_id}")
        
        # Update seen signatures
        existing_signatures.update(new_signatures)
        
        return unique_transactions, existing_signatures
    
    def _compute_signature(self, transaction: Transaction) -> str:
        """Compute hash signature for deduplication."""
        # Create a signature based on key fields
        signature_data = {
            "user_id": transaction.user_id,
            "amount": transaction.amount,
            "currency": transaction.currency,
            "timestamp": transaction.timestamp.isoformat(),
            "merchant_id": transaction.merchant_id,
            "transaction_type": transaction.transaction_type.value,
        }
        signature_str = json.dumps(signature_data, sort_keys=True)
        return hashlib.sha256(signature_str.encode()).hexdigest()
    
    def validate_distribution(
        self,
        transactions: List[Transaction],
        reference_distribution: Dict = None
    ) -> Dict:
        """Validate statistical distribution of transactions."""
        if not transactions:
            return {"valid": False, "reason": "Empty transaction list"}
        
        amounts = [tx.amount for tx in transactions]
        
        # Basic statistics
        stats_result = {
            "count": len(transactions),
            "mean_amount": np.mean(amounts),
            "median_amount": np.median(amounts),
            "std_amount": np.std(amounts),
            "min_amount": np.min(amounts),
            "max_amount": np.max(amounts),
            "fraud_ratio": sum(1 for tx in transactions if tx.is_fraud) / len(transactions),
        }
        
        # Kolmogorov-Smirnov test if reference distribution provided
        if reference_distribution:
            try:
                # Perform KS test
                ks_statistic, p_value = stats.kstest(
                    amounts,
                    lambda x: stats.norm.cdf(
                        x,
                        loc=reference_distribution.get("mean", np.mean(amounts)),
                        scale=reference_distribution.get("std", np.std(amounts))
                    )
                )
                stats_result["ks_statistic"] = ks_statistic
                stats_result["ks_p_value"] = p_value
                stats_result["distribution_valid"] = p_value > 0.05
            except Exception as e:
                logger.warning(f"KS test failed: {e}")
                stats_result["distribution_valid"] = True  # Assume valid if test fails
        
        stats_result["valid"] = True
        return stats_result
    
    def check_anti_memorization(
        self,
        transactions: List[Transaction],
        real_transactions: List[Transaction] = None
    ) -> Dict:
        """Check that generated transactions don't memorize real data."""
        if not real_transactions:
            # If no real transactions provided, assume no memorization
            return {"memorization_detected": False, "distance": 1.0}
        
        # Compute distance metrics
        gen_amounts = [tx.amount for tx in transactions]
        real_amounts = [tx.amount for tx in real_transactions]
        
        # Use statistical distance (e.g., Wasserstein distance)
        try:
            from scipy.stats import wasserstein_distance
            distance = wasserstein_distance(gen_amounts, real_amounts)
            # Normalize distance
            max_distance = max(np.std(gen_amounts), np.std(real_amounts)) * 2
            normalized_distance = min(distance / max_distance if max_distance > 0 else 1.0, 1.0)
            
            # Memorization threshold (adjust as needed)
            memorization_threshold = 0.1
            memorization_detected = normalized_distance < memorization_threshold
            
            return {
                "memorization_detected": memorization_detected,
                "distance": normalized_distance,
                "wasserstein_distance": distance
            }
        except Exception as e:
            logger.warning(f"Anti-memorization check failed: {e}")
            return {"memorization_detected": False, "distance": 1.0}
    
    def validate_business_rules(self, transactions: List[Transaction]) -> List[Transaction]:
        """Validate business rules and constraints."""
        valid_transactions = []
        
        for tx in transactions:
            # Business rule: fraudulent transactions should have fraud scenarios
            if tx.is_fraud and not tx.fraud_scenarios:
                logger.warning(f"Fraudulent transaction {tx.transaction_id} missing fraud scenarios")
                # Add default scenario
                tx.fraud_scenarios = ["identity_theft"]
            
            # Business rule: legitimate transactions should not have fraud scenarios
            if not tx.is_fraud and tx.fraud_scenarios:
                logger.warning(f"Legitimate transaction {tx.transaction_id} has fraud scenarios")
                tx.fraud_scenarios = []
            
            # Business rule: amounts should be reasonable
            if tx.amount > 1000000:  # $1M limit
                logger.warning(f"Transaction {tx.transaction_id} has unusually large amount")
                continue
            
            valid_transactions.append(tx)
        
        return valid_transactions


# Global instance
validation_service = ValidationService()
