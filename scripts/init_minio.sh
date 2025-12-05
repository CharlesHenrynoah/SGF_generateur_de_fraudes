#!/bin/bash
# Script d'initialisation MinIO pour créer le bucket

set -e

MC_ALIAS="local"
MC_ENDPOINT="${S3_ENDPOINT_URL:-http://localhost:9000}"
MC_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
MC_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
MC_BUCKET="${S3_BUCKET_NAME:-synthetic-fraud}"

echo "Initialisation MinIO..."
echo "Endpoint: $MC_ENDPOINT"
echo "Bucket: $MC_BUCKET"

# Attendre que MinIO soit prêt
until curl -f "$MC_ENDPOINT/minio/health/live" > /dev/null 2>&1; do
  echo "En attente de MinIO..."
  sleep 2
done

# Utiliser mc (MinIO Client) si disponible, sinon utiliser l'API REST
if command -v mc &> /dev/null; then
  echo "Utilisation de mc..."
  mc alias set $MC_ALIAS $MC_ENDPOINT $MC_ACCESS_KEY $MC_SECRET_KEY
  mc mb $MC_ALIAS/$MC_BUCKET --ignore-existing || true
  echo "Bucket créé: $MC_BUCKET"
else
  echo "mc non disponible, utilisation de l'API REST..."
  # Utiliser l'API REST de MinIO pour créer le bucket
  python3 << EOF
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

s3_client = boto3.client(
    's3',
    endpoint_url='$MC_ENDPOINT',
    aws_access_key_id='$MC_ACCESS_KEY',
    aws_secret_access_key='$MC_SECRET_KEY',
    config=Config(signature_version='s3v4')
)

try:
    s3_client.head_bucket(Bucket='$MC_BUCKET')
    print(f"Bucket $MC_BUCKET existe déjà")
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        s3_client.create_bucket(Bucket='$MC_BUCKET')
        print(f"Bucket $MC_BUCKET créé avec succès")
    else:
        print(f"Erreur: {e}")
EOF
fi

echo "Initialisation MinIO terminée"
