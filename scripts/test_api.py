#!/usr/bin/env python3
"""Script de test pour l'API Fraud Generator."""
import requests
import json
import time
from datetime import datetime, timedelta

API_URL = "http://localhost:8010"


def test_health():
    """Test du health check."""
    print("Testing health endpoint...")
    response = requests.get(f"{API_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    return response.status_code == 200


def test_preview():
    """Test du endpoint preview."""
    print("\nTesting preview endpoint...")
    payload = {
        "count": 10,
        "fraud_ratio": 0.2,
        "scenarios": ["card_testing", "account_takeover"],
        "currency": "USD",
        "countries": ["US"],
        "seed": 42
    }
    
    start_time = time.time()
    response = requests.post(
        f"{API_URL}/v1/generator/preview",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    elapsed = time.time() - start_time
    
    print(f"Status: {response.status_code}")
    print(f"Time: {elapsed:.2f}s")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Batch ID: {data['batch_id']}")
        print(f"Generated: {data['generated']}")
        print(f"Fraudulent: {data['fraudulent']}")
        print(f"Legit: {data['legit']}")
        print(f"Latency: {data['latency_ms']}ms")
        
        if data.get('transactions'):
            print(f"\nFirst transaction:")
            print(json.dumps(data['transactions'][0], indent=2, default=str))
        
        return True
    else:
        print(f"Error: {response.text}")
        return False


def test_generate():
    """Test du endpoint generate (petit batch)."""
    print("\nTesting generate endpoint (small batch)...")
    payload = {
        "count": 100,
        "fraud_ratio": 0.1,
        "scenarios": ["identity_theft"],
        "currency": "EUR",
        "countries": ["FR", "DE"],
        "start_date": (datetime.now() - timedelta(days=7)).isoformat(),
        "end_date": datetime.now().isoformat(),
        "seed": 12345
    }
    
    start_time = time.time()
    response = requests.post(
        f"{API_URL}/v1/generator/generate",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=300  # 5 minutes timeout
    )
    elapsed = time.time() - start_time
    
    print(f"Status: {response.status_code}")
    print(f"Time: {elapsed:.2f}s")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Batch ID: {data['batch_id']}")
        print(f"Generated: {data['generated']}")
        print(f"Fraudulent: {data['fraudulent']}")
        print(f"Legit: {data['legit']}")
        print(f"S3 URI: {data.get('s3_uri', 'N/A')}")
        print(f"Latency: {data['latency_ms']}ms")
        return True
    else:
        print(f"Error: {response.text}")
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("Fraud Generator API Tests")
    print("=" * 50)
    
    results = []
    
    results.append(("Health Check", test_health()))
    results.append(("Preview", test_preview()))
    results.append(("Generate", test_generate()))
    
    print("\n" + "=" * 50)
    print("Test Results:")
    print("=" * 50)
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{name}: {status}")
    
    all_passed = all(result for _, result in results)
    print(f"\nOverall: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}")
    exit(0 if all_passed else 1)
