# Déploiement Kubernetes

Ce répertoire contient les configurations Kubernetes pour déployer le générateur de fraudes en production.

## Prérequis

- Cluster Kubernetes (1.24+)
- kubectl configuré
- Ingress Controller (Nginx ou Traefik)
- Cert-Manager (optionnel, pour TLS)
- GPU nodes avec NVIDIA GPU Operator (pour le service LLM)

## Déploiement

1. Créer le namespace :
```bash
kubectl apply -f namespace.yaml
```

2. Créer les secrets (à adapter selon votre environnement) :
```bash
kubectl create secret generic fraud-generator-secrets \
  --from-literal=database-url='postgresql://user:pass@host:5432/db' \
  --from-literal=supabase-key='your-key' \
  --from-literal=s3-access-key='your-key' \
  --from-literal=s3-secret-key='your-secret' \
  --namespace=fraud-generator
```

3. Appliquer les configurations :
```bash
kubectl apply -f configmap.yaml
kubectl apply -f redis-deployment.yaml
kubectl apply -f api-deployment.yaml
kubectl apply -f llm-deployment.yaml
kubectl apply -f ingress.yaml
```

## Vérification

```bash
# Vérifier les pods
kubectl get pods -n fraud-generator

# Vérifier les services
kubectl get svc -n fraud-generator

# Vérifier les logs
kubectl logs -f deployment/fraud-generator-api -n fraud-generator
kubectl logs -f deployment/fraud-generator-llm -n fraud-generator
```

## Autoscaling

Les HPA (Horizontal Pod Autoscalers) sont configurés pour :
- API : 2-10 replicas basés sur CPU/memory
- LLM : 1-3 replicas basés sur utilisation GPU

## Sécurité

- Utiliser des secrets Kubernetes pour les credentials
- Activer mTLS avec Istio/Linkerd
- Configurer les NetworkPolicies pour limiter le trafic
- Utiliser RBAC pour les permissions
