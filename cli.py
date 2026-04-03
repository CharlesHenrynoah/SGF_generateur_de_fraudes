#!/usr/bin/env python3
"""CLI interactive pour générer des transactions frauduleuses."""
import asyncio
import sys
import os
import json
import random
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
import uuid

# Ajouter le répertoire app au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config import settings
from app.models.transaction import GenerateRequest, FraudScenario
from app.services.llm_service import llm_service
from app.services.validation_service import validation_service
from app.services.storage_service import storage_service
from app.services.kafka_service import kafka_service
from app.services.dataset_service import dataset_service
from app.services.rag_bank_security import (
    RagFormat,
    generate_rag_payload,
    sql_row_from_rag,
)

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.panel import Panel
    from rich.prompt import Prompt, Confirm, IntPrompt
    from rich.syntax import Syntax
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Rich non installé, interface basique utilisée. Installez avec: pip install rich")


def generate_batch_id() -> str:
    """Generate a unique batch ID."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"gen_{timestamp}"


def display_welcome(console=None):
    """Affiche le message de bienvenue."""
    msg = "🧠 Générateur de Fraudes - CLI\nGénération de transactions synthétiques frauduleuses"
    if console and RICH_AVAILABLE:
        console.print(Panel.fit(f"[bold cyan]{msg}[/bold cyan]", border_style="cyan"))
    else:
        print("=" * 60)
        print(msg)
        print("=" * 60)


def select_fraud_scenarios(console=None) -> List[FraudScenario]:
    """Permet à l'utilisateur de sélectionner les scénarios de fraude."""
    scenarios_map = {
        "1": FraudScenario.CARD_TESTING,
        "2": FraudScenario.ACCOUNT_TAKEOVER,
        "3": FraudScenario.IDENTITY_THEFT,
        "4": FraudScenario.MERCHANT_FRAUD,
        "5": FraudScenario.MONEY_LAUNDERING,
        "6": FraudScenario.PHISHING,
        "7": FraudScenario.CHARGEBACK_FRAUD,
    }
    
    scenarios_desc = {
        "1": "Card Testing - Test de cartes avec multiples petites transactions",
        "2": "Account Takeover - Prise de contrôle de compte",
        "3": "Identity Theft - Vol d'identité",
        "4": "Merchant Fraud - Fraude commerçant",
        "5": "Money Laundering - Blanchiment d'argent",
        "6": "Phishing - Transaction depuis compte compromis",
        "7": "Chargeback Fraud - Fraude par rétrofacturation",
    }
    
    print("\n📋 Scénarios de fraude disponibles:")
    for key, scenario in scenarios_map.items():
        print(f"  {key}. {scenario.value} - {scenarios_desc[key]}")
    
    if console and RICH_AVAILABLE:
        selected = Prompt.ask(
            "\n[bold]Sélectionnez les scénarios (séparés par des virgules, 'all' pour tous, 'none' pour aucun)[/bold]",
            default="all"
        )
    else:
        selected = input("\nSélectionnez les scénarios (séparés par des virgules, 'all' pour tous, 'none' pour aucun) [all]: ").strip() or "all"
    
    if selected.lower() == "all":
        return list(FraudScenario)
    elif selected.lower() == "none":
        return []
    else:
        selected_scenarios = []
        for key in selected.split(","):
            key = key.strip()
            if key in scenarios_map:
                selected_scenarios.append(scenarios_map[key])
        return selected_scenarios


async def generate_complex_transaction(
    is_fraud: bool, fmt: RagFormat = "hybrid"
) -> Optional[Dict[str, Any]]:
    """Génère un payload RAG ; `fmt` = openapi | flat | response | label | hybrid."""
    return await generate_rag_payload(is_fraud, fmt)


def _rag_fraud_display_label(tx: Dict[str, Any], fmt: RagFormat, known_fraud: bool) -> str:
    if fmt == "hybrid" or fmt == "response":
        return "yes" if tx.get("decision") == "DENY" else "none"
    if fmt == "label":
        return "yes" if tx.get("label") == "fraud" else "none"
    return "yes" if known_fraud else "none"


async def main_rag_mode(
    count: int,
    fraud_ratio: float,
    no_save: bool,
    console=None,
    rag_format: RagFormat = "openapi",
):
    """Mode RAG : payloads alignés bank-security (voir --rag-format)."""
    if console and RICH_AVAILABLE:
        console.print(f"\n🚀 Mode RAG — format [bold]{rag_format}[/bold]")
    else:
        print(f"\n🚀 Mode RAG — format {rag_format}")
    print(
        "   (openapi = OpenAPI §3, flat = decision-engine §1, response = §2, label = case-service §4, hybrid = legacy)\n"
    )

    dataset_service.initialize()
    await llm_service.initialize()

    if not no_save:
        await storage_service.initialize_db()

    fraud_count = int(count * fraud_ratio)
    legit_count = count - fraud_count

    records: List[Tuple[Dict[str, Any], bool]] = []

    for _ in range(fraud_count):
        print(".", end="", flush=True)
        tx = await generate_rag_payload(True, rag_format)
        if tx:
            records.append((tx, True))

    for _ in range(legit_count):
        print(".", end="", flush=True)
        tx = await generate_rag_payload(False, rag_format)
        if tx:
            records.append((tx, False))

    print("\n\n✅ Génération terminée !")

    display_limit = 10 if len(records) <= 10 else 3

    if console and RICH_AVAILABLE:
        for tx, known_fraud in records[:display_limit]:
            console.print(Syntax(json.dumps(tx, indent=2, default=str), "json", theme="monokai", word_wrap=True))
            label = _rag_fraud_display_label(tx, rag_format, known_fraud)
            status_color = "red" if label == "yes" else "green"
            console.print(f"[{status_color}]fraud indicator : {label}[/{status_color}]")
            console.print("-" * 40)

        if len(records) > display_limit:
            console.print(
                f"[italic]... et {len(records) - display_limit} autres payloads (non affichés)[/italic]"
            )

    else:
        for tx, known_fraud in records[:display_limit]:
            print(json.dumps(tx, indent=2, default=str))
            print(f"fraud indicator : {_rag_fraud_display_label(tx, rag_format, known_fraud)}")
            print("-" * 40)

        if len(records) > display_limit:
            print(f"... et {len(records) - display_limit} autres payloads (non affichés)")

    if not no_save:
        print("\n💾 Sauvegarde en base de données...")
        saved_count = 0
        sql_success = False

        try:
            if hasattr(storage_service, "db_engine") and storage_service.db_engine:
                from sqlalchemy import text

                try:
                    with storage_service.db_engine.connect() as conn:
                        for tx, known_fraud in records:
                            try:
                                row = sql_row_from_rag(tx, rag_format, known_fraud)
                                meta = row.pop("metadata")
                                params = {**row, "metadata": json.dumps(meta, default=str)}
                                conn.execute(
                                    text(
                                        """
                                    INSERT INTO synthetic_transactions 
                                    (transaction_id, user_id, merchant_id, amount, currency, 
                                     transaction_type, timestamp, country, city, ip_address, 
                                     device_id, card_last4, is_fraud, fraud_scenarios, 
                                     explanation, batch_id, metadata)
                                    VALUES (:transaction_id, :user_id, :merchant_id, :amount, 
                                            :currency, :transaction_type, :timestamp, :country, 
                                            :city, :ip_address, :device_id, :card_last4, 
                                            :is_fraud, :fraud_scenarios, :explanation, 
                                            :batch_id, :metadata::jsonb)
                                    ON CONFLICT (transaction_id) DO NOTHING
                                """
                                    ),
                                    params,
                                )
                                saved_count += 1
                            except Exception as e:
                                print(f"Erreur insert SQL transaction: {e}")
                        conn.commit()
                        sql_success = True
                except Exception as sql_error:
                    print(f"⚠️ Connexion SQL directe échouée ({sql_error}). Tentative via API Supabase...")
                    sql_success = False
            else:
                print("ℹ️ Pas de moteur SQL configuré ou initialisé.")
                sql_success = False

        except Exception as e:
            print(f"⚠️ Erreur inattendue SQL: {e}")
            sql_success = False

        if not sql_success:
            if hasattr(storage_service, "supabase") and storage_service.supabase:
                print("🔄 Utilisation de l'API Supabase (fallback)...")
                try:
                    saved_count = 0
                    for tx, known_fraud in records:
                        row = sql_row_from_rag(tx, rag_format, known_fraud)
                        meta = row.pop("metadata")
                        data = {**row, "metadata": meta}
                        storage_service.supabase.table("synthetic_transactions").insert(data).execute()
                        saved_count += 1
                except Exception as api_error:
                    print(f"❌ Erreur insertion API Supabase: {api_error}")
            else:
                print("❌ Impossible de sauvegarder: ni SQL ni API Supabase disponibles.")

        print(f"✓ {saved_count} enregistrements sauvegardés.")


async def main_async(
    count: Optional[int] = None,
    fraud_ratio: Optional[float] = None,
    currency: str = "USD",
    countries: str = "US",
    seed: Optional[int] = None,
    no_save: bool = False,
    no_s3: bool = False,
    no_kafka: bool = False,
    rag_mode: bool = False,
    rag_format: RagFormat = "openapi",
):
    """Fonction principale async."""
    console = Console() if RICH_AVAILABLE else None
    
    display_welcome(console)
    
    # Vérifier la configuration
    provider = (settings.llm_provider or "gemini").strip().lower()
    if provider == "gemini" and not settings.gemini_api_key:
        error_msg = "❌ Erreur: GEMINI_API_KEY non configurée (LLM_PROVIDER=gemini)"
        if console:
            console.print(f"[bold red]{error_msg}[/bold red]")
        else:
            print(error_msg)
        print("Configurez-la dans le fichier .env")
        sys.exit(1)
    if provider == "openai" and not settings.openai_api_key:
        error_msg = "❌ Erreur: OPENAI_API_KEY non configurée (LLM_PROVIDER=openai)"
        if console:
            console.print(f"[bold red]{error_msg}[/bold red]")
        else:
            print(error_msg)
        sys.exit(1)
    
    # Vérifier la configuration de la base de données
    if not settings.database_url and (not settings.supabase_url or not settings.supabase_service_key):
        warning = "⚠️  Avertissement: Base de données non configurée, la sauvegarde en DB sera ignorée"
        if console:
            console.print(f"[yellow]{warning}[/yellow]")
        else:
            print(warning)
        print("   Configurez DATABASE_URL dans le fichier .env")
    
    # Mode interactif si les options ne sont pas fournies
    if count is None:
        if console and RICH_AVAILABLE:
            count = IntPrompt.ask("\n[bold]Nombre de transactions à générer[/bold]", default=1000)
        else:
            count = int(input("\nNombre de transactions à générer [1000]: ").strip() or "1000")
    
    if fraud_ratio is None:
        if console and RICH_AVAILABLE:
            fraud_ratio = float(Prompt.ask("[bold]Ratio de transactions frauduleuses (0.0-1.0)[/bold]", default="0.1"))
        else:
            fraud_ratio = float(input("Ratio de transactions frauduleuses (0.0-1.0) [0.1]: ").strip() or "0.1")
    
    if not 0 <= fraud_ratio <= 1:
        error_msg = "❌ Le ratio de fraude doit être entre 0.0 et 1.0"
        if console:
            console.print(f"[bold red]{error_msg}[/bold red]")
        else:
            print(error_msg)
        sys.exit(1)
        
    # BRANCHEMENT RAG MODE
    if rag_mode:
        await main_rag_mode(count, fraud_ratio, no_save, console, rag_format=rag_format)
        return

    # ... (Suite du code existant pour le mode normal) ...
    
    # Sélection des scénarios
    scenarios = select_fraud_scenarios(console)
    
    # Plage de dates
    if console and RICH_AVAILABLE:
        use_date_range = Confirm.ask("\n[bold]Utiliser une plage de dates spécifique?[/bold]", default=False)
    else:
        use_date_range = input("\nUtiliser une plage de dates spécifique? (o/N): ").strip().lower() == "o"
    
    start_date = None
    end_date = None
    
    if use_date_range:
        start_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        end_str = datetime.now().strftime("%Y-%m-%d")
        
        if console and RICH_AVAILABLE:
            start_str = Prompt.ask("[bold]Date de début (YYYY-MM-DD)[/bold]", default=start_str)
            end_str = Prompt.ask("[bold]Date de fin (YYYY-MM-DD)[/bold]", default=end_str)
        else:
            start_str = input(f"Date de début (YYYY-MM-DD) [{start_str}]: ").strip() or start_str
            end_str = input(f"Date de fin (YYYY-MM-DD) [{end_str}]: ").strip() or end_str
        
        try:
            start_date = datetime.fromisoformat(f"{start_str}T00:00:00")
            end_date = datetime.fromisoformat(f"{end_str}T23:59:59")
        except:
            error_msg = "❌ Format de date invalide"
            if console:
                console.print(f"[bold red]{error_msg}[/bold red]")
            else:
                print(error_msg)
            sys.exit(1)
    
    # Créer la requête
    request = GenerateRequest(
        count=count,
        fraud_ratio=fraud_ratio,
        scenarios=scenarios,
        currency=currency,
        countries=[c.strip() for c in countries.split(",")],
        start_date=start_date,
        end_date=end_date,
        seed=seed
    )
    
    # Afficher le résumé
    summary_msg = "\n📋 Résumé de la génération:"
    if console:
        console.print(f"\n[bold cyan]{summary_msg}[/bold cyan]")
    else:
        print(summary_msg)
    
    print(f"  Nombre total: {count}")
    print(f"  Transactions frauduleuses: {int(count * fraud_ratio)}")
    print(f"  Transactions légitimes: {int(count * (1 - fraud_ratio))}")
    print(f"  Ratio de fraude: {fraud_ratio:.1%}")
    print(f"  Scénarios: {', '.join([s.value for s in scenarios]) if scenarios else 'Aucun'}")
    print(f"  Devise: {currency}")
    print(f"  Pays: {countries}")
    print(f"  Seed: {seed if seed else 'Aléatoire'}")
    
    if console and RICH_AVAILABLE:
        continue_gen = Confirm.ask("\n[bold]Continuer avec la génération?[/bold]", default=True)
    else:
        continue_gen = input("\nContinuer avec la génération? (O/n): ").strip().lower() != "n"
    
    if not continue_gen:
        cancel_msg = "Génération annulée"
        if console:
            console.print(f"[yellow]{cancel_msg}[/yellow]")
        else:
            print(cancel_msg)
        sys.exit(0)
    
    # Génération
    batch_id = generate_batch_id()
    start_time = datetime.now()
    
    print("\n🚀 Génération en cours...")
    
    # Initialiser les services
    await llm_service.initialize()
    await storage_service.initialize_s3()
    await storage_service.initialize_db()
    await kafka_service.initialize()
    
    print("  ✓ Services initialisés")
    
    # Générer les transactions
    print(f"  ⏳ Génération de {count} transactions...")
    transactions = await llm_service.generate_transactions(request)
    print(f"  ✓ {len(transactions)} transactions générées")
    
    # Valider
    print("  ⏳ Validation...")
    transactions = validation_service.validate_schema(transactions)
    transactions = validation_service.validate_business_rules(transactions)
    transactions, _ = validation_service.deduplicate(transactions)
    print(f"  ✓ {len(transactions)} transactions validées")
    
    # Ajouter batch_id
    for tx in transactions:
        tx.batch_id = batch_id
    
    # Sauvegarder
    s3_uri = None
    if not no_s3:
        try:
            print("  ⏳ Export vers S3...")
            s3_uri = await storage_service.export_to_s3(transactions, batch_id, format="parquet")
            print(f"  ✓ Exporté vers S3: {s3_uri}")
        except Exception as e:
            print(f"  ⚠️  Erreur export S3: {e}")
    
    if not no_save:
        try:
            print("  ⏳ Sauvegarde en base de données...")
            saved = await storage_service.save_to_database(transactions, batch_id)
            if saved > 0:
                print(f"  ✓ {saved} transactions sauvegardées dans Supabase")
            else:
                print(f"  ⚠️  Aucune transaction sauvegardée (vérifiez la connexion)")
        except Exception as e:
            print(f"  ⚠️  Erreur sauvegarde DB: {e}")
            print(f"      Vérifiez votre connexion Supabase avec: python scripts/test_supabase_connection.py")
    
    if not no_kafka:
        try:
            print("  ⏳ Publication sur Kafka...")
            published = await kafka_service.publish_transactions(transactions, batch_id)
            print(f"  ✓ {published} transactions publiées")
        except Exception as e:
            print(f"  ⚠️  Erreur publication Kafka: {e}")
    
    # Résultats
    elapsed = (datetime.now() - start_time).total_seconds()
    fraudulent_count = sum(1 for tx in transactions if tx.is_fraud)
    legit_count = len(transactions) - fraudulent_count
    
    success_msg = "\n✅ Génération terminée!"
    if console:
        console.print(f"\n[bold green]{success_msg}[/bold green]\n")
    else:
        print(success_msg)
    
    print("📊 Résultats:")
    print(f"  Batch ID: {batch_id}")
    print(f"  Total généré: {len(transactions)}")
    print(f"  Frauduleuses: {fraudulent_count}")
    print(f"  Légitimes: {legit_count}")
    if transactions:
        print(f"  Ratio réel: {fraudulent_count/len(transactions):.1%}")
    print(f"  Temps écoulé: {elapsed:.2f}s")
    if s3_uri:
        print(f"  S3 URI: {s3_uri}")
    
    # Afficher quelques exemples
    if transactions:
        print("\n📊 Exemples de transactions:")
        for i, tx in enumerate(transactions[:5], 1):
            print(f"\n  {i}. {tx.transaction_id[:30]}...")
            print(f"     Montant: {tx.amount} {tx.currency}")
            tt = tx.transaction_type
            tt_show = tt.value if hasattr(tt, "value") else tt
            print(f"     Type: {tt_show}")
            print(f"     Fraude: {'✓' if tx.is_fraud else '✗'}")
            if tx.fraud_scenarios:
                scen = [
                    (s.value if hasattr(s, "value") else str(s))
                    for s in tx.fraud_scenarios
                ]
                print(f"     Scénarios: {', '.join(scen)}")


def main():
    """Point d'entrée principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description="CLI pour générer des transactions frauduleuses")
    parser.add_argument("-c", "--count", type=int, help="Nombre de transactions à générer")
    parser.add_argument("-r", "--fraud-ratio", type=float, help="Ratio de transactions frauduleuses (0.0-1.0)")
    parser.add_argument("--currency", default="USD", help="Devise (défaut: USD)")
    parser.add_argument("--countries", default="US", help="Pays (séparés par des virgules, défaut: US)")
    parser.add_argument("--seed", type=int, help="Seed pour reproductibilité")
    parser.add_argument("--no-save", action="store_true", help="Ne pas sauvegarder en base de données")
    parser.add_argument("--no-s3", action="store_true", help="Ne pas exporter vers S3")
    parser.add_argument("--no-kafka", action="store_true", help="Ne pas publier sur Kafka")
    parser.add_argument("--rag", action="store_true", help="Mode RAG : données CSV + payloads bank-security")
    parser.add_argument(
        "--rag-format",
        choices=["openapi", "flat", "response", "label", "hybrid"],
        default="openapi",
        help="Format JSON RAG : openapi (§3), flat (§1), response (§2), label (§4), hybrid (legacy)",
    )

    args = parser.parse_args()

    asyncio.run(
        main_async(
            count=args.count,
            fraud_ratio=args.fraud_ratio,
            currency=args.currency,
            countries=args.countries,
            seed=args.seed,
            no_save=args.no_save,
            no_s3=args.no_s3,
            no_kafka=args.no_kafka,
            rag_mode=args.rag,
            rag_format=args.rag_format,
        )
    )


if __name__ == "__main__":
    main()
