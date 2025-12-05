#!/usr/bin/env python3
"""CLI interactive pour générer des transactions frauduleuses."""
import asyncio
import sys
import os
import json
import random
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
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


async def generate_complex_transaction(is_fraud: bool) -> Optional[Dict[str, Any]]:
    """
    Génère une transaction complexe basée sur un exemple réel du CSV (RAG).
    """
    # 1. Récupérer une "graine" du dataset réel
    if is_fraud:
        seed_data = dataset_service.get_random_fraud()
    else:
        seed_data = dataset_service.get_random_legit()

    # 2. Préparer le prompt avec le format STRICT demandé
    prompt = f"""
    You are a financial fraud simulation engine. 
    Generate a JSON object representing a transaction analysis response.
    
    BASE DATA (Use this as context but enrich it significantly):
    - Amount: {seed_data['amount']}
    - Type: {seed_data['transaction_type']}
    - Merchant Category: {seed_data['merchant_category']}
    - Country: {seed_data['country']}
    - Hour: {seed_data['hour']}
    - Is Fraud: {is_fraud}
    
    OUTPUT FORMAT (Strict JSON):
    {{
      "decision_id": "dec_{str(uuid.uuid4())[:12]}",
      "decision": "{'DENY' if is_fraud else 'ALLOW'}", 
      "score": {round(random.uniform(0.85, 0.99) if is_fraud else random.uniform(0.01, 0.15), 2)},
      "rule_hits": ["list", "of", "rules"],
      "reasons": ["Human readable reasons"],
      "latency_ms": {random.randint(20, 100)},
      "model_version": "gbdt_v1.2.3",
      "sla": {{ "p95_budget_ms": 100 }},
      "transaction_context": {{
        "tenant_id": "bank-fr-001",
        "idempotency_key": "tx-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}",
        "event": {{
          "type": "card_payment",
          "id": "evt_{str(uuid.uuid4())[:12]}",
          "ts": "{datetime.now().isoformat()}Z",
          "amount": {seed_data['amount']},
          "currency": "EUR",
          "merchant": {{
            "id": "merch_{str(uuid.uuid4())[:8]}",
            "name": "Generate a realistic name based on category '{seed_data['merchant_category']}'",
            "mcc": "Generate valid 4-digit MCC",
            "country": "{seed_data['country']}"
          }},
          "card": {{
            "card_id": "card_tok_{str(uuid.uuid4())[:8]}",
            "type": "{'virtual' if seed_data['transaction_type'] == 'Online' else 'physical'}",
            "user_id": "user_{seed_data['user_id']}"
          }},
          "context": {{
            "ip": "Generate realistic IP",
            "geo": "{seed_data['country']}",
            "device_id": "dev_{str(uuid.uuid4())[:8]}",
            "channel": "{'web' if seed_data['transaction_type'] == 'Online' else 'pos'}"
          }},
          "security": {{
            "auth_method": "3ds/pin/none",
            "aml_flag": {str(is_fraud).lower()}
          }},
          "kyc": {{
            "status": "verified",
            "level": "standard",
            "confidence": 0.95
          }}
        }}
      }}
    }}
    
    INSTRUCTIONS:
    - If decision is DENY, provide strong reasons in 'reasons' and 'rule_hits' related to fraud.
    - If decision is ALLOW, reasons should explain why it's safe.
    - Make merchant name realistic for the category '{seed_data['merchant_category']}' in country '{seed_data['country']}'.
    - Ensure JSON is valid.
    """

    # 3. Appel OpenAI
    try:
        response = await llm_service.openai_client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": "You are a data generator. Output ONLY valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"Error generating: {e}")
        return None


async def main_rag_mode(count: int, fraud_ratio: float, no_save: bool, console=None):
    """Mode RAG : Génération complexe basée sur les CSV."""
    print("\n🚀 Mode RAG activé : Génération de transactions complexes basées sur vos CSV...")
    
    # Init services
    dataset_service.initialize()
    await llm_service.initialize()
    
    # Initialiser la DB seulement si on veut sauvegarder
    if not no_save:
        await storage_service.initialize_db()
    
    fraud_count = int(count * fraud_ratio)
    legit_count = count - fraud_count
    
    transactions = []
    
    # Génération frauduleuse
    for _ in range(fraud_count):
        print(".", end="", flush=True)
        tx = await generate_complex_transaction(is_fraud=True)
        if tx:
            transactions.append(tx)
            
    # Génération légitime
    for _ in range(legit_count):
        print(".", end="", flush=True)
        tx = await generate_complex_transaction(is_fraud=False)
        if tx:
            transactions.append(tx)
            
    print("\n\n✅ Génération terminée !")
    
    # Affichage
    display_limit = 10 if len(transactions) <= 10 else 3
    
    if console and RICH_AVAILABLE:
        for tx in transactions[:display_limit]:
            # Extraire la partie contexte (requête pure)
            tx_context = tx.get('transaction_context', {})
            
            # Afficher le JSON de la transaction
            console.print(Syntax(json.dumps(tx_context, indent=2), "json", theme="monokai", word_wrap=True))
            
            # Afficher le statut de fraude
            is_fraud_status = "yes" if tx.get('decision') == 'DENY' else "none"
            status_color = "red" if is_fraud_status == "yes" else "green"
            console.print(f"[{status_color}]fraud : {is_fraud_status}[/{status_color}]")
            console.print("-" * 40)
            
        if len(transactions) > display_limit:
            console.print(f"[italic]... et {len(transactions) - display_limit} autres transactions (sauvegardées mais non affichées)[/italic]")
            
    else:
        for tx in transactions[:display_limit]:
            tx_context = tx.get('transaction_context', {})
            print(json.dumps(tx_context, indent=2))
            is_fraud_status = "yes" if tx.get('decision') == 'DENY' else "none"
            print(f"fraud : {is_fraud_status}")
            print("-" * 40)
            
        if len(transactions) > display_limit:
            print(f"... et {len(transactions) - display_limit} autres transactions (sauvegardées mais non affichées)")
        
    # Sauvegarde spéciale pour le format complexe
    if not no_save:
        print("\n💾 Sauvegarde en base de données...")
        saved_count = 0
        
        # On doit adapter car la table SQL est plate, et le JSON est complexe.
        # On va extraire les champs principaux pour les colonnes SQL, et stocker TOUT le JSON dans 'metadata'.
        
        # Flag pour savoir si on a réussi avec SQL
        sql_success = False
        
        try:
            # Tenter d'abord la connexion SQL directe
            if hasattr(storage_service, 'db_engine') and storage_service.db_engine:
                from sqlalchemy import text
                try:
                    with storage_service.db_engine.connect() as conn:
                        for tx in transactions:
                            try:
                                ctx = tx['transaction_context']['event']
                                
                                conn.execute(text("""
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
                                """), {
                                    'transaction_id': ctx['id'],
                                    'user_id': ctx['card']['user_id'],
                                    'merchant_id': ctx['merchant']['id'],
                                    'amount': ctx['amount'],
                                    'currency': ctx['currency'],
                                    'transaction_type': ctx['type'],
                                    'timestamp': ctx['ts'].replace('Z', ''),
                                    'country': ctx['merchant']['country'],
                                    'city': None,
                                    'ip_address': ctx['context']['ip'],
                                    'device_id': ctx['context']['device_id'],
                                    'card_last4': None,
                                    'is_fraud': (tx['decision'] == 'DENY'),
                                    'fraud_scenarios': tx.get('rule_hits', []),
                                    'explanation': ", ".join(tx.get('reasons', [])),
                                    'batch_id': generate_batch_id(),
                                    'metadata': json.dumps(tx)
                                })
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

        # Fallback: Utiliser le client Supabase (HTTP API) si SQL a échoué
        if not sql_success:
            if hasattr(storage_service, 'supabase') and storage_service.supabase:
                print("🔄 Utilisation de l'API Supabase (fallback)...")
                try:
                    # Reset count pour éviter doublons si partiel
                    saved_count = 0 
                    for tx in transactions:
                        ctx = tx['transaction_context']['event']
                        data = {
                            'transaction_id': ctx['id'],
                            'user_id': ctx['card']['user_id'],
                            'merchant_id': ctx['merchant']['id'],
                            'amount': ctx['amount'],
                            'currency': ctx['currency'],
                            'transaction_type': ctx['type'],
                            'timestamp': ctx['ts'].replace('Z', ''),
                            'country': ctx['merchant']['country'],
                            'is_fraud': (tx['decision'] == 'DENY'),
                            'fraud_scenarios': tx.get('rule_hits', []),
                            'explanation': ", ".join(tx.get('reasons', [])),
                            'batch_id': generate_batch_id(),
                            'metadata': tx,
                            # Champs optionnels
                            'city': None,
                            'ip_address': ctx['context']['ip'],
                            'device_id': ctx['context']['device_id'],
                            'card_last4': None
                        }
                        storage_service.supabase.table('synthetic_transactions').insert(data).execute()
                        saved_count += 1
                except Exception as api_error:
                    print(f"❌ Erreur insertion API Supabase: {api_error}")
            else:
                print("❌ Impossible de sauvegarder: ni SQL ni API Supabase disponibles.")

        print(f"✓ {saved_count} transactions complexes sauvegardées.")


async def main_async(
    count: Optional[int] = None,
    fraud_ratio: Optional[float] = None,
    currency: str = "USD",
    countries: str = "US",
    seed: Optional[int] = None,
    no_save: bool = False,
    no_s3: bool = False,
    no_kafka: bool = False,
    rag_mode: bool = False
):
    """Fonction principale async."""
    console = Console() if RICH_AVAILABLE else None
    
    display_welcome(console)
    
    # Vérifier la configuration
    if not settings.openai_api_key:
        error_msg = "❌ Erreur: OPENAI_API_KEY non configurée"
        if console:
            console.print(f"[bold red]{error_msg}[/bold red]")
        else:
            print(error_msg)
        print("Configurez-la dans le fichier .env ou via la variable d'environnement")
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
        await main_rag_mode(count, fraud_ratio, no_save, console)
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
            print(f"     Type: {tx.transaction_type.value}")
            print(f"     Fraude: {'✓' if tx.is_fraud else '✗'}")
            if tx.fraud_scenarios:
                print(f"     Scénarios: {', '.join([s.value for s in tx.fraud_scenarios])}")


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
    parser.add_argument("--rag", action="store_true", help="Mode RAG : Utiliser les données CSV locales et le format JSON complexe")
    
    args = parser.parse_args()
    
    asyncio.run(main_async(
        count=args.count,
        fraud_ratio=args.fraud_ratio,
        currency=args.currency,
        countries=args.countries,
        seed=args.seed,
        no_save=args.no_save,
        no_s3=args.no_s3,
        no_kafka=args.no_kafka,
        rag_mode=args.rag
    ))


if __name__ == "__main__":
    main()
