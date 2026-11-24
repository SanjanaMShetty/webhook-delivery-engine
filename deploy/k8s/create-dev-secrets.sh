#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=webhook-engine
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl -n "$NAMESPACE" get secret webhook-engine-secrets >/dev/null 2>&1; then
  DB_PASSWORD="$(python -c 'import secrets; print(secrets.token_hex(24))')"
  FERNET_KEY="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
  kubectl create secret generic webhook-engine-secrets \
    --namespace "$NAMESPACE" \
    --from-literal="POSTGRES_PASSWORD=$DB_PASSWORD" \
    --from-literal="WEBHOOK_ENCRYPTION_KEY=$FERNET_KEY"
fi
if ! kubectl -n "$NAMESPACE" get configmap webhook-engine-config >/dev/null 2>&1; then
  DB_PASSWORD="$(kubectl -n "$NAMESPACE" get secret webhook-engine-secrets -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 --decode)"
  kubectl create configmap webhook-engine-config \
    --namespace "$NAMESPACE" \
    --from-literal="DATABASE_URL=postgresql+psycopg://webhook:$DB_PASSWORD@postgres:5432/webhooks" \
    --from-literal="REDIS_URL=redis://redis:6379/0" \
    --from-literal="ALLOW_PRIVATE_WEBHOOK_TARGETS=false" \
    --from-literal="ENDPOINT_RATE_LIMIT_PER_SECOND=10" \
    --from-literal="ENDPOINT_CONCURRENCY_LIMIT=5" \
    --from-literal="CIRCUIT_BREAKER_FAILURE_THRESHOLD=5" \
    --from-literal="CIRCUIT_BREAKER_COOLDOWN_SECONDS=30" \
    --from-literal="LOG_LEVEL=INFO" \
    --from-literal="WORKER_METRICS_HOST=0.0.0.0" \
    --from-literal="WORKER_METRICS_PORT=9101" \
    --from-literal="OUTBOX_METRICS_HOST=0.0.0.0" \
    --from-literal="OUTBOX_METRICS_PORT=9102"
fi
echo "Development credentials are stored in the webhook-engine namespace secret."
