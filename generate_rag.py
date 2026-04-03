#!/usr/bin/env python3
"""Exemples RAG au format bank-security (OpenAPI, flat, response, label, hybrid)."""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.dataset_service import dataset_service
from app.services.llm_service import llm_service
from app.services.rag_bank_security import RagFormat, generate_rag_payload

try:
    from rich.console import Console
    from rich.syntax import Syntax

    console = Console()
except ImportError:
    console = None


async def main():
    parser = argparse.ArgumentParser(description="Génération RAG bank-security")
    parser.add_argument(
        "--format",
        "-f",
        dest="fmt",
        choices=["openapi", "flat", "response", "label", "hybrid"],
        default="openapi",
        help="Format de sortie (défaut: openapi)",
    )
    args = parser.parse_args()
    fmt: RagFormat = args.fmt

    dataset_service.initialize()
    await llm_service.initialize()

    print(f"🚀 Exemples RAG — format: {fmt}\n")

    print("--- Légitime ---")
    legit = await generate_rag_payload(is_fraud=False, fmt=fmt)
    if console:
        console.print(Syntax(json.dumps(legit, indent=2, default=str), "json", theme="monokai"))
    else:
        print(json.dumps(legit, indent=2, default=str))

    print("\n--- Frauduleux ---")
    fraud = await generate_rag_payload(is_fraud=True, fmt=fmt)
    if console:
        console.print(Syntax(json.dumps(fraud, indent=2, default=str), "json", theme="monokai"))
    else:
        print(json.dumps(fraud, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
