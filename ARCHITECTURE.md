# Architecture Technique - Générateur de Fraude

## Vue d'ensemble

Le générateur de fraudes est un système microservices conçu pour générer des transactions synthétiques frauduleuses et légitimes à grande échelle, utilisant des LLMs fine-tunés et des règles métier pour créer des datasets réalistes.

## Composants Principaux

### 1. Fraud Generator API (`fraud-generator-api`)

**Responsabilités** :
- Exposition des endpoints REST (`/v1/generator/generate`, `/v1/generator/preview`)
- Orchestration du flux de génération
- Validation des requêtes
- Gestion des tâches en arrière-plan (persistance, Kafka)

**Technologies** :
- FastAPI
- Python 3.11+
- Uvicorn (ASGI server)

**Endpoints** :
- `GET /health` : Health check
- `GET /health/ready` : Readiness check
- `POST /v1/generator/preview` : Prévisualisation (max 100 transactions)
- `POST /v1/generator/generate` : Génération complète (jusqu'à 100k transactions)

### 2. Fraud Generator LLM Service (`fraud-generator-llm`)

**Responsabilités** :
- Génération de transactions synthétiques via LLM
- Support de la génération basée sur règles (fallback)
- Gestion du modèle fine-tuné (LoRA/PEFT)

**Technologies** :
- Transformers (HuggingFace)
- PyTorch
- PEFT / LoRA pour fine-tuning
- BitsAndBytes pour quantisation 4-bit

**Modèles supportés** :
- DialoGPT (par défaut)
- Modèles personnalisés via configuration

### 3. Validation Service

**Responsabilités** :
- Validation de schéma (Pydantic)
- Déduplication (hash signatures)
- Tests statistiques (Kolmogorov-Smirnov, Anderson-Darling)
- Vérification anti-mémorisation
- Validation des règles métier

**Algorithme de déduplication** :
- Hash SHA256 basé sur : user_id, amount, currency, timestamp, merchant_id, transaction_type

### 4. Storage Service

**Responsabilités** :
- Export vers S3/MinIO (Parquet, CSV)
- Persistance dans PostgreSQL/Supabase
- Calcul de hash SHA256 pour audit

**Formats supportés** :
- Parquet (recommandé pour performance)
- CSV (compatibilité)

### 5. Kafka Service

**Responsabilités** :
- Publication d'événements de transactions synthétiques
- Publication des requêtes de génération (audit)
- Support de la consommation asynchrone

**Topics** :
- `synthetic_tx_events` : Transactions générées
- `synthetic_generate_requests` : Requêtes de génération (audit)

## Flux de Données

### Flux de Génération

```
1. Client → POST /v1/generator/generate
   ↓
2. API → Validation des paramètres
   ↓
3. API → LLM Service → Génération batch par batch
   ↓
4. API → Validation Service
   ├─→ Validation schéma
   ├─→ Déduplication
   ├─→ Tests statistiques
   └─→ Validation règles métier
   ↓
5. API → Storage Service
   ├─→ Export S3 (Parquet)
   ├─→ Persistance DB (async)
   └─→ Publication Kafka (async)
   ↓
6. API → Réponse avec batch_id et métriques
```

### Flux de Consommation

```
Kafka Topic (synthetic_tx_events)
   ↓
├─→ Training Pipeline (ML/LLM)
├─→ A/B Tests
├─→ Load Tests (Decision Engine)
└─→ Analytics (Offline)
```

## Base de Données

### Tables

#### `synthetic_transactions`
Stocke toutes les transactions générées.

**Index** :
- `transaction_id` (UNIQUE)
- `user_id`
- `batch_id`
- `timestamp`
- `is_fraud`

#### `synthetic_batches`
Table WORM (Write Once Read Many) pour l'audit.

**Champs clés** :
- `batch_id` (UNIQUE)
- `sha256_hash` : Hash des transactions pour intégrité
- `metadata` : JSONB avec statistiques

### Vues

#### `fraud_statistics`
Vue agrégée pour statistiques quotidiennes.

## Infrastructure

### Développement (Docker Compose)

**Services** :
- `supabase-db` : PostgreSQL 15
- `redis` : Cache et stockage temporaire
- `minio` : S3 compatible
- `kafka` + `zookeeper` : Messagerie
- `fraud-generator-api` : API principale
- `fraud-generator-llm` : Service LLM (optionnel, nécessite GPU)

**Volumes** :
- `postgres_data` : Données PostgreSQL
- `redis_data` : Données Redis
- `minio_data` : Données MinIO
- `./models` : Modèles LLM
- `./data` : Exports locaux

### Production (Kubernetes)

**Deployments** :
- `fraud-generator-api` : 2-10 replicas (HPA)
- `fraud-generator-llm` : 1-3 replicas GPU (HPA)
- `redis` : StatefulSet 3 replicas (cluster)

**Services** :
- `fraud-generator-api-service` : ClusterIP
- `fraud-generator-llm-service` : ClusterIP
- `redis-service` : Headless service

**Autoscaling** :
- API : CPU (70%), Memory (80%)
- LLM : GPU utilization (80%)

**Ingress** :
- Nginx Ingress Controller
- Rate limiting : 100 req/min
- TLS via Cert-Manager

## Sécurité

### Transit
- mTLS entre services (Istio/Linkerd)
- HTTPS pour API publique

### At-rest
- Chiffrement AES256 pour exports S3
- Volumes Kubernetes chiffrés (PVC)

### Authentification
- OAuth2/OIDC pour API
- HMAC pour Kafka

### PII
- Jamais de données réelles
- Hash salé pour identifiants sensibles
- Masquage automatique dans logs

### Audit
- Table `synthetic_batches` immuable
- SHA256 hash de chaque export
- Logs structurés (structlog)

## Performance

### Latences Cibles

| Opération | P50 | P95 | P99 |
|-----------|-----|-----|-----|
| Preview (100 tx) | 800ms | 1.2s | 1.5s |
| Generate (50k tx) | 3min | 5min | 8min |
| LLM (1k tx) | 20s | 35s | 50s |

### Débit

- **Génération** : ≥ 1,000 transactions/s
- **Kafka** : ≥ 5,000 transactions/s
- **Export S3** : ≥ 50 MB/s

### Scalabilité

- **Horizontal** : Autoscaling basé sur métriques
- **Vertical** : GPU mémoire pour LLM
- **Sharding** : Par batch_id, tenant, scenario

## Monitoring

### Métriques Prometheus

- `fraud_generator_requests_total` : Nombre total de requêtes
- `fraud_generator_latency_seconds` : Latence des requêtes
- `fraud_generator_transactions_generated` : Transactions générées
- `fraud_generator_errors_total` : Erreurs

### Logs

- Format structuré (JSON)
- Niveaux : DEBUG, INFO, WARNING, ERROR
- Context : batch_id, transaction_id, user_id

## Scénarios de Fraude

1. **Card Testing** : Multiples petites transactions pour tester des cartes
2. **Account Takeover** : Prise de contrôle de compte utilisateur
3. **Identity Theft** : Vol d'identité et utilisation frauduleuse
4. **Merchant Fraud** : Fraude commerçant
5. **Money Laundering** : Blanchiment via transactions structurées
6. **Phishing** : Transactions depuis comptes compromis
7. **Chargeback Fraud** : Fraude par rétrofacturation

## Évolutions Futures

- [ ] Support de modèles LLM plus grands (GPT-4, Claude)
- [ ] Génération multi-tenant
- [ ] Interface web pour configuration
- [ ] Dashboard de monitoring
- [ ] Support de streaming (WebSocket)
- [ ] Fine-tuning automatique basé sur feedback
