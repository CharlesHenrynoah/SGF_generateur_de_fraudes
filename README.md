# Générateur de Fraudes - Synthetic Fraud Generator

Système complet de génération de transactions frauduleuses synthétiques pour l'entraînement et le test de modèles de détection de fraude.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ DECISION ENGINE (Core)                                      │
└────────┬──────────────────────────┬────────────────────────┘
         │                          │
         ▼                          ▼
┌─────────────────────┐  ┌──────────────────────┐
│ RULES SERVICE       │  │ MODEL SERVING        │
└─────────┬───────────┘  └──────────┬───────────┘
          │                         │
          └───────────┬─────────────┘
                      ▼
         ┌─────────────────────────────┐
         │ FEATURE STORE ONLINE        │
         └─────────────────────────────┘
                      ▲
                      │ (datasets synthétiques validés)
┌─────────────────────┴─────────────────────────────────────┐
│ 🧠 FRAUD GENERATION SERVICE (LLM)                          │
│ ┌─────────────────────────┐ ┌─────────────────────────┐   │
│ │ Synthetic API (REST)    │→│ Validation & Filtrage   │→│ Export │
│ │ /generate, /preview     │ │ schéma, dédup, mix ratio│ │ DB/S3 │
│ └─────────────────────────┘ └─────────────────────────┘   │
│                                                             │
│ ┌────────────────────┐                                     │
│ │ LLM Fine-tuné      │ ◄─── prompts + seed                │
│ │ (LoRA/PEFT)        │                                     │
│ └────────────────────┘                                     │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Démarrage Rapide

### Prérequis

- Python 3.11+
- Clé API OpenAI (modèle économique recommandé: `gpt-4o-mini`)
- Compte Supabase (optionnel, pour la persistance)

### Installation avec Docker Compose

1. **Cloner le projet** :
```bash
git clone <repository-url>
cd SGF_generateur_de_fraudes
```

2. **Configurer l'environnement** :
```bash
cp .env.example .env
# Éditer .env avec vos configurations
```

3. **Démarrer les services** :
```bash
docker-compose up -d
```

4. **Vérifier le statut** :
```bash
docker-compose ps
curl http://localhost:8010/health
```

### Installation locale (développement)

1. **Créer un environnement virtuel** :
```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
```

2. **Installer les dépendances** :
```bash
pip install -r requirements.txt
```

3. **Configurer l'environnement** :
```bash
cp .env.example .env
# Éditer .env avec vos credentials OpenAI et Supabase
```

Voir `CONFIGURATION.md` pour les détails.

4. **Utiliser la CLI interactive** :
```bash
python cli.py
```

Ou avec des paramètres :
```bash
python cli.py --count 1000 --fraud-ratio 0.1 --currency EUR
```

5. **Démarrer l'API** (optionnel) :
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8010 --reload
```

## 📡 API Endpoints

### Health Check
```bash
GET /health
GET /health/ready
```

### Génération de Transactions

#### Preview (100 transactions max)
```bash
POST /v1/generator/preview
Content-Type: application/json

{
  "count": 100,
  "fraud_ratio": 0.1,
  "scenarios": ["card_testing", "account_takeover"],
  "currency": "USD",
  "countries": ["US", "FR"],
  "seed": 42
}
```

#### Génération complète
```bash
POST /v1/generator/generate
Content-Type: application/json

{
  "count": 50000,
  "fraud_ratio": 0.08,
  "scenarios": ["identity_theft", "merchant_fraud"],
  "currency": "EUR",
  "countries": ["FR", "DE", "IT"],
  "start_date": "2025-01-01T00:00:00Z",
  "end_date": "2025-01-31T23:59:59Z",
  "seed": 12345
}
```

**Réponse** :
```json
{
  "batch_id": "gen_2025_01_30_143022",
  "generated": 50000,
  "fraudulent": 4000,
  "legit": 46000,
  "s3_uri": "s3://synthetic-fraud/synthetic/gen_2025_01_30_143022_20250130_143045.parquet",
  "latency_ms": 180000
}
```

## 🔧 Configuration

### Variables d'environnement principales

- `OPENAI_API_KEY` : **Requis** - Votre clé API OpenAI
- `OPENAI_MODEL` : Modèle OpenAI à utiliser (défaut: `gpt-4o-mini` - économique)
- `SUPABASE_URL` : URL de votre projet Supabase
- `SUPABASE_SERVICE_KEY` : Clé de service Supabase
- `DATABASE_URL` : Connection string PostgreSQL de Supabase
- `S3_ENDPOINT_URL` : URL du service S3/MinIO (optionnel)
- `KAFKA_BOOTSTRAP_SERVERS` : Serveurs Kafka (optionnel)

Voir `.env.example` et `CONFIGURATION.md` pour les détails complets.

### Utilisation de la CLI

La CLI interactive permet de générer des transactions facilement :

```bash
# Mode interactif (questions posées)
python cli.py

# Avec paramètres
python cli.py --count 5000 --fraud-ratio 0.15 --currency EUR --countries "FR,DE"

# Sans sauvegarde (test uniquement)
python cli.py --count 100 --no-save --no-s3 --no-kafka
```

Options disponibles :
- `-c, --count` : Nombre de transactions
- `-r, --fraud-ratio` : Ratio de fraude (0.0-1.0)
- `--currency` : Devise (USD, EUR, etc.)
- `--countries` : Pays (séparés par virgules)
- `--seed` : Seed pour reproductibilité
- `--no-save` : Ne pas sauvegarder en DB
- `--no-s3` : Ne pas exporter vers S3
- `--no-kafka` : Ne pas publier sur Kafka

## 🏭 Déploiement Production (Kubernetes)

### Prérequis

- Cluster Kubernetes 1.24+
- Ingress Controller
- GPU nodes (pour le service LLM)

### Déploiement

```bash
# Créer le namespace
kubectl apply -f k8s/namespace.yaml

# Créer les secrets
kubectl create secret generic fraud-generator-secrets \
  --from-literal=database-url='postgresql://...' \
  --namespace=fraud-generator

# Déployer les services
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/api-deployment.yaml
kubectl apply -f k8s/llm-deployment.yaml
kubectl apply -f k8s/ingress.yaml
```

Voir `k8s/README.md` pour plus de détails.

## 📊 Latences Cibles

| Composant | P50 | P95 | P99 | Timeout |
|-----------|-----|-----|-----|---------|
| `/preview` (100 tx) | 800ms | 1.2s | 1.5s | 2s |
| `/generate` (50k tx) | 3min | 5min | 8min | 10min |
| LLM Inference (1k tx) | 20s | 35s | 50s | 60s |
| Post-processing | 3s | 5s | 8s | 10s |
| Validation | 1s | 2s | 3s | 5s |
| Export S3 | 2s | 4s | 6s | 10s |

## 🔐 Sécurité

- **Transit** : Communication sécurisée par mTLS entre services
- **At-rest** : Chiffrement AES256 pour modèles et exports
- **Auth** : OAuth2/OIDC pour l'accès API
- **PII** : Jamais de données réelles, hash salé pour identifiants
- **Audit** : Table `synthetic_batches` immuable (WORM)

## 📈 Scalabilité

- **Horizontal** : Autoscaling basé sur QPS (API) et GPU utilisation (LLM)
- **Vertical** : GPU mémoire pour contexte LLM, CPU pour post-processing
- **Sharding** : Partitionnement par batch_id, tenant, scenario
- **Cache** : Redis pour prompts précompilés et templates
- **Débit cible** : ≥ 5,000 transactions/s sur Kafka

## 🧪 Tests

```bash
# Tests unitaires
pytest tests/

# Tests d'intégration
pytest tests/integration/

# Tests de charge
locust -f tests/load/locustfile.py
```

## 📁 Structure du Projet

```
SGF_generateur_de_fraudes/
├── app/
│   ├── main.py                 # Application FastAPI principale
│   ├── config.py               # Configuration
│   ├── models/                 # Modèles Pydantic
│   │   ├── transaction.py
│   │   └── batch.py
│   ├── routers/                # Routes API
│   │   ├── generator.py
│   │   └── health.py
│   └── services/               # Services métier
│       ├── llm_service.py      # Génération LLM
│       ├── validation_service.py
│       ├── storage_service.py  # S3, PostgreSQL
│       └── kafka_service.py
├── db/
│   └── init.sql                # Schéma de base de données
├── k8s/                        # Configurations Kubernetes
│   ├── namespace.yaml
│   ├── api-deployment.yaml
│   ├── llm-deployment.yaml
│   └── ...
├── docker-compose.yml          # Développement local
├── Dockerfile.api
├── Dockerfile.llm
├── requirements.txt
└── README.md
```

## 🔄 Flux de Génération

1. **Client** envoie requête POST `/v1/generator/generate`
2. **Validation** des paramètres d'entrée
3. **Génération LLM** : création de transactions synthétiques
4. **Post-processing** : nettoyage et formatage
5. **Validation** : schéma, déduplication, tests statistiques
6. **Labellisation** : ajout de `is_fraud`, `fraud_scenarios`, `explanation`
7. **Persistance** : Supabase, S3, Kafka
8. **Réponse** avec batch_id et métriques

## 🎯 Scénarios de Fraude Supportés

- `card_testing` : Test de cartes avec multiples petites transactions
- `account_takeover` : Prise de contrôle de compte
- `identity_theft` : Vol d'identité
- `merchant_fraud` : Fraude commerçant
- `money_laundering` : Blanchiment d'argent
- `phishing` : Transaction depuis compte compromis
- `chargeback_fraud` : Fraude par rétrofacturation

## 📝 License

[À définir]

## 🤝 Contribution

[À définir]

## 📧 Contact

[À définir]
